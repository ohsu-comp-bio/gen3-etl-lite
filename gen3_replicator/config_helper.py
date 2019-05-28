"""Usefule helpers."""
import json
import os
from datetime import date, datetime


def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError ("Type %s not serializable" % type(obj))


def load_json(file_name):
    """Return parsed json file."""
    for prefix in config_paths():
        path = os.path.join(prefix,file_name)
        if (os.path.exists(path)):
            with open(path, 'r') as reader:
                return json.load(reader)
    return {}


def save_json(object, file_name):
    """Saves object in file_name."""
    prefix = config_paths()[0]
    path = os.path.join(prefix,file_name)
    with open(path, 'w') as output:
        output.write(json.dumps(object, separators=(',', ': '), default=json_serial))


def config_paths():
    """Return path to config."""
    return [os.path.dirname(os.path.abspath(__file__)), '/config']
