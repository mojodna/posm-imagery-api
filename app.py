# coding=utf-8

from datetime import datetime
import json
import math
from StringIO import StringIO
import subprocess32 as subprocess
import os
import uuid

from cachetools.func import lru_cache, rr_cache
from celery import Celery, chain, group, states
from flask import Flask, redirect, request, send_from_directory, jsonify, url_for
from flask_cors import CORS
from flask_uploads import UploadSet, configure_uploads
from flask_tus import tus_manager
import mercantile
from mercantile import Tile
import numpy as np
from PIL import Image
import rasterio
from rasterio.warp import transform_bounds
from werkzeug.wsgi import DispatcherMiddleware


APPLICATION_ROOT = os.environ.get('APPLICATION_ROOT', '')
REDIS_URL = os.environ.get('REDIS_URL', 'redis://')
CELERY_BROKER_URL = os.environ.get('CELERY_BROKER_URL', REDIS_URL)
CELERY_RESULT_BACKEND = os.environ.get('CELERY_RESULT_BACKEND', REDIS_URL)
IMAGERY_PATH = os.environ.get('IMAGERY_PATH', 'imagery')
MIN_ZOOM = int(os.environ.get('MIN_ZOOM', 0))
MAX_ZOOM = int(os.environ.get('MAX_ZOOM', 22))
SERVER_NAME = os.environ.get('SERVER_NAME', 'localhost:8000')
USE_X_SENDFILE = os.environ.get('USE_X_SENDFILE', False)
UPLOADED_IMAGERY_DEST = os.environ.get('UPLOADED_IMAGERY_DEST', 'uploads/')

# strip trailing slash if necessary
if IMAGERY_PATH[-1] == '/':
    IMAGERY_PATH = IMAGERY_PATH[:-1]

# add trailing slash if necessary
if UPLOADED_IMAGERY_DEST[-1] != '/':
    UPLOADED_IMAGERY_DEST = UPLOADED_IMAGERY_DEST[:-1]

app = Flask(__name__)
CORS(app)
app.config['APPLICATION_ROOT'] = APPLICATION_ROOT
app.config['CELERY_BROKER_URL'] = CELERY_BROKER_URL
app.config['CELERY_RESULT_BACKEND'] = CELERY_RESULT_BACKEND
app.config['CELERY_TRACK_STARTED'] = True
app.config['USE_X_SENDFILE'] = USE_X_SENDFILE
app.config['UPLOADED_IMAGERY_DEST'] = UPLOADED_IMAGERY_DEST

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
def upload_file_handler(upload_file_path, filename=None, remote=False):
    id = str(uuid.uuid4())
    task_info = os.path.join(IMAGERY_PATH, id, 'ingest.task')
    os.mkdir(os.path.dirname(task_info))

    if remote:
        upload_file_path = '/vsicurl/{}'.format(upload_file_path)

    task = initialize_imagery(id, upload_file_path).apply_async()

    tasks = []

    while task.parent:
        if isinstance(task, celery.GroupResult):
            for child in task.children:
                tasks.append(child.id)
        else:
            tasks.append(task.id)
        task = task.parent

    tasks.append(task.id)
    tasks.reverse()

    # stash task ids in the imagery directory so we know which task(s) to look up
    with open(task_info, 'w') as f:
        f.write(json.dumps(tasks))

    with open(os.path.join(IMAGERY_PATH, id, 'index.json'), 'w') as metadata:
        metadata.write(json.dumps({
            'tilejson': '2.1.0',
            'name': id,
        }))

    return id


def initialize_imagery(id, source_path):
    return chain(
        place_file.si(id, source_path),
        create_metadata.si(id),
        group(create_overviews.si(id), create_warped_vrt.si(id))
    )


@celery.task(bind=True)
def place_file(self, id, source_path):
    target_dir = os.path.join(IMAGERY_PATH, id)
    if not os.path.exists(target_dir):
        os.mkdir(target_dir)
    output_file = os.path.join(target_dir, 'index.tif')

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

    if not source_path.startswith(('/vsicurl', 'http://', 'https://')):
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
    raster_path = os.path.join(IMAGERY_PATH, id, 'index.tif')

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
            width = src.width
            height = src.height

            bounds = transform_bounds(src.crs, {'init': 'epsg:4326'}, *src.bounds)
            bandCount = src.count

    self.update_state(meta={
                        'name': 'metadata',
                        'started_at': started_at.isoformat(),
                        'status': 'Writing metadata'
                      })

    with open(os.path.join(IMAGERY_PATH, id, 'index.json'), 'w') as metadata:
        metadata.write(json.dumps({
            'tilejson': '2.1.0',
            'name': id,
            'bounds': bounds,
            'meta': {
                'approximateZoom': zoom,
                'bandCount': bandCount,
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
    raster_path = os.path.join(IMAGERY_PATH, id, 'index.tif')
    # initialize Flask
    # TODO Celery's @worker_init.connect decorator _should_ work for this
    app.config['SERVER_NAME'] = SERVER_NAME
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
    raster_path = os.path.join(IMAGERY_PATH, id, 'index.tif')
    vrt_path = os.path.join(IMAGERY_PATH, id, 'index.vrt')
    # initialize Flask
    # TODO Celery's @worker_init.connect decorator _should_ work for this
    app.config['SERVER_NAME'] = SERVER_NAME
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
    ]

    # add an alpha band (for NODATA) if one wasn't already included
    if meta['meta']['bandCount'] < 4:
        gdalwarp.append('-dstalpha')

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
    app.config['SERVER_NAME'] = SERVER_NAME

    meta = get_metadata(id)

    output_path = os.path.join(IMAGERY_PATH, id, 'index.mbtiles')

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
    with open(os.path.join(IMAGERY_PATH, id, 'index.json')) as metadata:
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

    src = get_source(os.path.join(IMAGERY_PATH, meta['name'], 'index.vrt'))
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


@app.route('/imagery')
def list_imagery():
    """List available imagery"""
    sources = os.listdir(IMAGERY_PATH)
    return jsonify(sources), 200


@app.route('/imagery/upload', methods=['PUT'])
def upload_imagery():
    filename = app.config['UPLOADED_IMAGERY_DEST'] + imagery.save(request.files['file'])

    id = upload_file_handler(filename)

    return redirect(url_for('get_imagery_metadata', id=id))


@app.route('/imagery/ingest', methods=['POST', 'PUT'])
def ingest_source():
    if request.args.get('url') is None:
        return jsonify({
            'message': '"url" parameter is required.'
        }), 400

    id = upload_file_handler(request.args.get('url'), remote=True)

    return redirect(url_for('get_imagery_metadata', id=id))


@app.route('/imagery/<id>')
def get_imagery_metadata(id):
    """Get imagery metadata"""
    return jsonify(get_metadata(id)), 200


@app.route('/imagery/<id>/<int:z>/<int:x>/<int:y>.png')
def get_tile(id, z, x, y):
    tile = read_tile(id, Tile(x, y, z))

    return tile, 200, {
        'Content-Type': 'image/png'
    }


@app.route('/imagery/<id>/<int:z>/<int:x>/<int:y>@<int:scale>x.png')
def get_scaled_tile(id, z, x, y, scale):
    tile = read_tile(id, Tile(x, y, z), scale=scale)

    return tile, 200, {
        'Content-Type': 'image/png'
    }


@app.route('/imagery/<id>/mbtiles')
def get_mbtiles(id):
    return send_from_directory(
        IMAGERY_PATH,
        os.path.join(id, 'index.mbtiles'),
        as_attachment=True,
        attachment_filename='{}.mbtiles'.format(id),
        conditional=True
    )


# TODO allow bounding boxes + zoom ranges to be provided
@app.route('/imagery/<id>/mbtiles', methods=['POST'])
def request_mbtiles(id):
    meta = get_metadata(id)

    task_info = os.path.join(IMAGERY_PATH, id, 'mbtiles.task')
    mbtiles_archive = os.path.join(IMAGERY_PATH, id, 'index.mbtiles')

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
    with open(task_info, 'w') as f:
        f.write(json.dumps([task.id]))

    return '', 202, {
        'Location': url_for('get_mbtiles_status', id=id)
    }


def serialize_status(task_ids):
    status = {
        'steps': []
    }

    states = []

    for id in task_ids:
        result = celery.AsyncResult(id)

        states.append(result.state)

        if isinstance(result.info, Exception):
            status['steps'].append(json.loads(result.info.message))
        else:
            status['steps'].append(result.info)

    status['state'] = min(states)

    return jsonify(status)


@app.route('/imagery/<id>/mbtiles/status')
def get_mbtiles_status(id):
    task_info = os.path.join(IMAGERY_PATH, id, 'mbtiles.task')

    with open(task_info) as t:
        tasks = json.load(t)

    return serialize_status(tasks), 200


@app.route('/imagery/<id>/ingest/status')
def get_ingestion_status(id):
    task_info = os.path.join(IMAGERY_PATH, id, 'ingest.task')

    with open(task_info) as t:
        tasks = json.load(t)

    return serialize_status(tasks), 200


app.wsgi_app = DispatcherMiddleware(None, {
    app.config['APPLICATION_ROOT']: app.wsgi_app
})


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000, debug=True)
