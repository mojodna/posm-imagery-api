# coding=utf-8

from flask import url_for
from flask_script import Manager

from app import app


manager = Manager(app)


@manager.command
def list_routes():
    import urllib
    output = []
    for rule in app.url_map.iter_rules():
        line = urllib.unquote("{:20s}\t{}\t{:50s}".format(
            ','.join(rule.methods), rule.rule, rule.endpoint))
        output.append(line)

    for line in sorted(output):
        print line


if __name__ == '__main__':
    manager.run()
