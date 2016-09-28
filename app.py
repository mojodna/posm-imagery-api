# coding=utf-8

from datetime import datetime
import json
import math
import random
import shutil
from StringIO import StringIO
import subprocess32 as subprocess
import time
import os
import uuid

from cachetools.func import lru_cache, rr_cache
from celery import Celery, chain, group, states
from flask import Flask, redirect, request, send_from_directory, jsonify, url_for
from flask_uploads import UploadSet, configure_uploads
from flask_tus import tus_manager
import mercantile
from mercantile import Tile
import numpy as np
from PIL import Image
import rasterio
from rasterio.warp import transform_bounds


# TODO strip slashes if necessary
IMAGERY_PATH = os.environ.get('IMAGERY_PATH', 'imagery')
MIN_ZOOM = int(os.environ.get('MIN_ZOOM', 0))
MAX_ZOOM = int(os.environ.get('MAX_ZOOM', 22))

app = Flask(__name__)
app.config['CELERY_BROKER_URL'] = os.environ.get('CELERY_BROKER_URL', 'redis://')
app.config['CELERY_RESULT_BACKEND'] = os.environ.get('CELERY_RESULT_BACKEND', 'redis://')
app.config['USE_X_SENDFILE'] = os.environ.get('USE_X_SENDFILE', False)
# TODO add slashes if necessary
app.config['UPLOADED_IMAGERY_DEST'] = os.environ.get('UPLOADED_IMAGERY_DEST', 'uploads/')

# Initialize Celery
celery = Celery(app.name, broker=app.config['CELERY_BROKER_URL'])
celery.conf.update(app.config)

# Initialize Tus
tm = tus_manager(app, upload_url='/imagery/upload',
    upload_folder=app.config['UPLOADED_IMAGERY_DEST'])

# overwrite tus_max_file_size to support big(ger) files
tm.tus_max_file_size = 17179869184 # 16GB

# Initialize Flask-Uploads
imagery = UploadSet('imagery', ('tif', 'tiff'))
configure_uploads(app, (imagery,))


@tm.upload_file_handler
def upload_file_handler(upload_file_path, filename=None):
    id = str(uuid.uuid4())
    task_info = '{}/{}/ingest.task'.format(IMAGERY_PATH, id)
    os.mkdir(os.path.dirname(task_info))

    task = initialize_imagery(id, upload_file_path).apply_async()

    while task.parent is not None:
        task = task.parent

    # stash task.id in the imagery directory so we know which task to look up
    f = open(task_info, 'w')
    f.write(task.id)
    f.close()

    return id


def initialize_imagery(id, source_path):
    return chain(
        place_file.si(id, source_path),
        create_metadata.si(id),
        group(create_overviews.si(id), create_warped_vrt.si(id))
    )


@celery.task(bind=True)
def place_file(self, id, source_path):
    target_dir = '{}/{}'.format(IMAGERY_PATH, id)
    if not os.path.exists(target_dir):
        os.mkdir(target_dir)
    output_file = '{}/index.tif'.format(target_dir)

    # rewrite with gdal_translate
    gdal_translate = [
        'gdal_translate',
        source_path,
        output_file,
        '-co', 'TILED=yes',
        '-co', 'COMPRESS=DEFLATE',
        '-co', 'PREDICTOR=2',
        '-co', 'SPARSE_OK=yes',
        '-co', 'BLOCKXSIZE=256',
        '-co', 'BLOCKYSIZE=256',
        '-co', 'INTERLEAVE=band',
        '-co', 'NUM_THREADS=ALL_CPUS',
    ]

    started_at = datetime.utcnow()

    self.update_state(state='RUNNING',
                      meta={
                        'name': 'preprocess',
                        'started_at': started_at.isoformat(),
                        'status': 'Rewriting imagery'
                      })

    try:
        returncode = subprocess.call(gdal_translate, timeout=60*5)
    except subprocess.TimeoutExpired as e:
        raise Exception(json.dumps({
            'name': 'overviews',
            'started_at': started_at.isoformat(),
            'command': ' '.join(gdal_translate),
            'return_code': returncode,
            'status': 'Timed out'
        }))

    if returncode != 0:
        raise Exception(json.dumps({
            'name': 'preprocess',
            'started_at': started_at.isoformat(),
            'command': ' '.join(gdal_translate),
            'return_code': returncode,
            'status': 'Failed'
        }))

    # delete original
    os.unlink(source_path)

    return {
        'name': 'preprocess',
        'completed_at': datetime.utcnow().isoformat(),
        'started_at': started_at,
        'status': 'Image pre-processing completed'
    }


@celery.task(bind=True)
def create_metadata(self, id):
    raster_path = '{}/{}/index.tif'.format(IMAGERY_PATH, id)

    started_at = datetime.utcnow()
    self.update_state(state='RUNNING',
                      meta={
                        'name': 'metadata',
                        'started_at': started_at.isoformat(),
                        'status': 'Reading metadata from imagery'
                      })

    with rasterio.drivers():
        with rasterio.open(raster_path) as src:
            # grab the lowest resolution dimension
            # NOTE: this assumes that units are in meters
            # see https://github.com/openterrain/spark-chunker/blob/master/gglp/get_zoom.py for an
            # alternative that may handle 4326-projected files better
            resolution = max(abs(src.affine[0]), abs(src.affine[4]))

            zoom = int(math.ceil(math.log((2 * math.pi * 6378137) /
                                          (resolution * 256)) / math.log(2)))
            width = src.meta['width']
            height = src.meta['height']

            bounds = transform_bounds(src.crs, {'init': 'epsg:4326'}, *src.bounds)

    self.update_state(meta={
                        'name': 'metadata',
                        'started_at': started_at.isoformat(),
                        'status': 'Writing metadata'
                      })

    with open('{}/{}/index.json'.format(IMAGERY_PATH, id), 'w') as metadata:
        metadata.write(json.dumps({
            'tilejson': '2.1.0',
            'name': id,
            'bounds': bounds,
            'meta': {
                'approximateZoom': zoom,
                'width': width,
                'height': height
            }
        }))

    return {
        'name': 'metadata',
        'completed_at': datetime.utcnow().isoformat(),
        'started_at': started_at,
        'status': 'Metadata creation completed'
    }


@celery.task(bind=True)
def create_overviews(self, id):
    raster_path = '{}/{}/index.tif'.format(IMAGERY_PATH, id)
    # initialize Flask
    # TODO Celery's @worker_init.connect decorator _should_ work for this
    app.config['SERVER_NAME'] = 'localhost:8000'
    meta = get_metadata(id)
    approximate_zoom = meta['meta']['approximateZoom']

    # create external overviews
    gdaladdo = [
        'gdaladdo',
        '-r', 'cubic',
        '--config', 'GDAL_TIFF_OVR_BLOCKSIZE', '256',
        '--config', 'TILED_OVERVIEW', 'yes',
        '--config', 'COMPRESS_OVERVIEW', 'DEFLATE',
        '--config', 'PREDICTOR_OVERVIEW', '2',
        '--config', 'SPARSE_OK_OVERVIEW', 'yes',
        '--config', 'BLOCKXSIZE_OVERVIEW', '256',
        '--config', 'BLOCKYSIZE_OVERVIEW', '256',
        '--config', 'INTERLEAVE_OVERVIEW', 'band',
        '--config', 'NUM_THREADS_OVERVIEW', 'ALL_CPUS',
        '-ro',
        raster_path,
    ]

    # generate a list of overview values
    gdaladdo.extend([str(2 ** (x + 1)) for x in range(approximate_zoom)])

    started_at = datetime.utcnow()

    self.update_state(state='RUNNING',
                      meta={
                        'name': 'overviews',
                        'started_at': started_at.isoformat(),
                        'status': 'Creating external overviews'
                      })

    try:
        returncode = subprocess.call(gdaladdo, timeout=60*5)
    except subprocess.TimeoutExpired as e:
        raise Exception(json.dumps({
            'name': 'overviews',
            'started_at': started_at.isoformat(),
            'command': ' '.join(gdaladdo),
            'return_code': returncode,
            'status': 'Timed out'
        }))

    if returncode != 0:
        raise Exception(json.dumps({
            'name': 'overviews',
            'started_at': started_at.isoformat(),
            'command': ' '.join(gdaladdo),
            'return_code': returncode,
            'status': 'Failed'
        }))

    return {
        'name': 'overviews',
        'completed_at': datetime.utcnow().isoformat(),
        'started_at': started_at,
        'status': 'Overview addition completed'
    }


@celery.task(bind=True)
def create_warped_vrt(self, id):
    raster_path = '{}/{}/index.tif'.format(IMAGERY_PATH, id)
    vrt_path = '{}/{}/index.vrt'.format(IMAGERY_PATH, id)
    # initialize Flask
    # TODO Celery's @worker_init.connect decorator _should_ work for this
    app.config['SERVER_NAME'] = 'localhost:5000'
    meta = get_metadata(id)
    approximate_zoom = meta['meta']['approximateZoom']

    # create a warped VRT to reproject on the fly
    gdalwarp = [
        'gdalwarp',
        raster_path,
        vrt_path,
        '-r',
        'cubic',
        '-t_srs', 'epsg:3857',
        '-overwrite',
        '-of', 'VRT',
        '-te', '-20037508.34', '-20037508.34', '20037508.34', '20037508.34',
        '-ts', str(2 ** approximate_zoom * 256), str(2 ** approximate_zoom * 256),
        '-dstalpha',
    ]

    started_at = datetime.utcnow()

    self.update_state(state='RUNNING',
                      meta={
                        'name': 'warped-vrt',
                        'started_at': started_at.isoformat(),
                        'status': 'Creating warped VRT'
                      })

    try:
        returncode = subprocess.call(gdalwarp, timeout=60*5)
    except subprocess.TimeoutExpired as e:
        raise Exception(json.dumps({
            'name': 'overviews',
            'started_at': started_at.isoformat(),
            'command': ' '.join(gdalwarp),
            'return_code': returncode,
            'status': 'Timed out'
        }))

    if returncode != 0:
        raise Exception(json.dumps({
            'name': 'warped-vrt',
            'started_at': started_at.isoformat(),
            'command': ' '.join(gdalwarp),
            'return_code': returncode,
            'status': 'Failed'
        }))

    return {
        'completed_at': datetime.utcnow().isoformat(),
        'started_at': started_at,
        'status': 'Warped VRT creation completed'
    }


@celery.task(bind=True)
def generate_mbtiles(self, id):
    """Generate an MBTiles archive for a given style."""

    # initialize Flask
    # TODO Celery's @worker_init.connect decorator _should_ work for this
    app.config['SERVER_NAME'] = 'localhost:5000'

    meta = get_metadata(id)

    output_path = './{}/{}/index.mbtiles'.format(IMAGERY_PATH, id)

    generate_cmd = [
        'tl',
        'copy',
        '-q',
        '-b', ' '.join(map(str, meta['bounds'])),
        '-z', str(MIN_ZOOM),
        '-Z', str(meta['meta']['approximateZoom']),
        meta['tiles'][0],
        'mbtiles://{}'.format(output_path)
    ]

    started_at = datetime.utcnow()

    self.update_state(state='RUNNING',
                      meta={
                        'name': 'mbtiles',
                        'started_at': started_at.isoformat(),
                        'status': 'Generating tiles'
                      })

    print('Running {}'.format(' '.join(generate_cmd)))

    try:
        returncode = subprocess.call(generate_cmd, timeout=60*60)
    except subprocess.TimeoutExpired as e:
        raise Exception(json.dumps({
            'name': 'overviews',
            'started_at': started_at.isoformat(),
            'command': ' '.join(generate_cmd),
            'return_code': returncode,
            'status': 'Timed out'
        }))

    if returncode != 0:
        raise Exception(json.dumps({
            'name': 'mbtiles',
            'started_at': started_at.isoformat(),
            'command': ' '.join(generate_cmd),
            'return_code': returncode,
            'status': 'Failed'
        }))

    return {
        'completed_at': datetime.utcnow().isoformat(),
        'started_at': started_at,
        'status': 'MBTiles generation completed'
    }


@lru_cache()
def get_metadata(id):
    with open('{}/{}/index.json'.format(IMAGERY_PATH, id)) as metadata:
        meta = json.load(metadata)

    with app.app_context():
        meta['tiles'] = [
            '{}/{{z}}/{{x}}/{{y}}.png'.format(url_for('get_imagery_metadata', id=id, _external=True))
        ]

    return meta


@lru_cache()
def get_source(path):
    with rasterio.drivers():
        return rasterio.open(path)


def render_tile(meta, tile, scale=1):
    src_tile_zoom = meta['meta']['approximateZoom']
    # do calculations in src_tile_zoom space
    dz = src_tile_zoom - tile.z
    x = 2**dz * tile.x
    y = 2**dz * tile.y
    mx = 2**dz * (tile.x + 1)
    my = 2**dz * (tile.y + 1)
    dx = mx - x
    dy = my - y
    top = (2**src_tile_zoom * 256) - 1

    # y, x (rows, columns)
    # window is measured in pixels at src_tile_zoom
    window = [[top - (top - (256 * y)), top - (top - ((256 * y) + int(256 * dy)))],
              [256 * x, (256 * x) + int(256 * dx)]]

    src = get_source('{}/{}/index.vrt'.format(IMAGERY_PATH, meta['name']))
    # use decimated reads to read from overviews, per https://github.com/mapbox/rasterio/issues/710
    data = np.empty(shape=(4, 256 * scale, 256 * scale)).astype(src.profile['dtype'])
    data = src.read(out=data, window=window)

    return data


class InvalidTileRequest(Exception):
    status_code = 404

    def __init__(self, message, status_code=None, payload=None):
        Exception.__init__(self)
        self.message = message
        if status_code is not None:
            self.status_code = status_code
        self.payload = payload

    def to_dict(self):
        rv = dict(self.payload or ())
        rv['message'] = self.message
        return rv


@rr_cache()
def read_tile(id, tile, scale=1):
    if not MIN_ZOOM <= tile.z <= MAX_ZOOM:
        raise InvalidTileRequest('Invalid zoom: {} outside [{}, {}]'.format(tile.z, MIN_ZOOM, MAX_ZOOM))

    meta = get_metadata(id)

    sw = mercantile.tile(*meta['bounds'][0:2], zoom=tile.z)
    ne = mercantile.tile(*meta['bounds'][2:4], zoom=tile.z)

    if not sw.x <= tile.x <= ne.x:
        raise InvalidTileRequest('Invalid x coordinate: {} outside [{}, {}]'.format(tile.x, sw.x, ne.x))

    if not ne.y <= tile.y <= sw.y:
        raise InvalidTileRequest('Invalid y coordinate: {} outside [{}, {}]'.format(tile.y, sw.y, ne.y))

    data = render_tile(meta, tile, scale=scale)
    imgarr = np.ma.transpose(data, [1, 2, 0]).astype(np.byte)

    out = StringIO()
    im = Image.fromarray(imgarr, 'RGBA')
    im.save(out, 'png')

    return out.getvalue()


@app.errorhandler(InvalidTileRequest)
def handle_invalid_tile_request(error):
    response = jsonify(error.to_dict())
    response.status_code = error.status_code
    return response


@app.errorhandler(IOError)
def handle_ioerror(error):
    return '', 404


@app.route('/imagery', methods=['GET'])
def list_imagery():
    """List available imagery"""
    sources = os.listdir(IMAGERY_PATH)
    return jsonify(sources), 200


@app.route('/imagery/upload', methods=['PUT'])
def upload_imagery():
    filename = app.config['UPLOADED_IMAGERY_DEST'] + imagery.save(request.files['file'])

    id = upload_file_handler(filename)

    return redirect(url_for('get_imagery_metadata', id=id))


@app.route('/imagery/<id>', methods=['GET'])
def get_imagery_metadata(id):
    """Get imagery metadata"""
    return jsonify(get_metadata(id)), 200


@app.route('/imagery/<id>/<int:z>/<int:x>/<int:y>.png', methods=['GET'])
def get_tile(id, z, x, y):
    tile = read_tile(id, Tile(x, y, z))

    return tile, 200, {
        'Content-Type': 'image/png'
    }


@app.route('/imagery/<id>/<int:z>/<int:x>/<int:y>@<int:scale>x.png', methods=['GET'])
def get_scaled_tile(id, z, x, y, scale):
    tile = read_tile(id, Tile(x, y, z), scale=scale)

    return tile, 200, {
        'Content-Type': 'image/png'
    }


@app.route('/imagery/<id>/mbtiles', methods=['GET'])
def get_mbtiles(id):
    return send_from_directory(
        IMAGERY_PATH,
        '{}/index.mbtiles'.format(id),
        as_attachment=True,
        attachment_filename='{}.mbtiles'.format(id),
        conditional=True
    )


# TODO allow bounding boxes + zoom ranges to be provided
@app.route('/imagery/<id>/mbtiles', methods=['POST'])
def request_mbtiles(id):
    meta = get_metadata(id)

    task_info = '{}/{}/mbtiles.task'.format(IMAGERY_PATH, id)
    mbtiles_archive = '{}/{}/index.mbtiles'.format(IMAGERY_PATH, id)

    if os.path.exists(mbtiles_archive):
        return jsonify({
            'message': 'MBTiles archive already exists'
        }), 400

    if os.path.exists(task_info):
        return jsonify({
            'message': 'MBTiles generation already in progress'
        }), 400

    task = generate_mbtiles.s(id=id).apply_async()

    # stash task.id in the imagery directory so we know which task to look up
    f = open(task_info, 'w')
    f.write(task.id)
    f.close()

    return '', 202, {
        'Location': url_for('get_mbtiles_status', id=id)
    }


def serialize_status(task_id):
    result = celery.AsyncResult(task_id)

    status = {
        'state': result.state,
        'steps': []
    }

    for _, node in result.iterdeps(intermediate=True):
        if hasattr(node, 'info'):
            if isinstance(node.info, Exception):
                status['steps'].append(json.loads(node.info.message))
            else:
                status['steps'].append(node.info)

    return jsonify(status)


@app.route('/imagery/<id>/mbtiles/status', methods=['GET'])
def get_mbtiles_status(id):
    task_info = '{}/{}/mbtiles.task'.format(IMAGERY_PATH, id)

    with open(task_info) as t:
        task_id = t.read()

    return serialize_status(task_id), 200


@app.route('/imagery/<id>/ingest/status', methods=['GET'])
def get_ingestion_status(id):
    task_info = '{}/{}/ingest.task'.format(IMAGERY_PATH, id)

    with open(task_info) as t:
        task_id = t.read()

    return serialize_status(task_id), 200


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000, debug=True)
