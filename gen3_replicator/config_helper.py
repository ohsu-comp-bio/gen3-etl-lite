"""Usefule helpers."""
import json
import os


def load_json(file_name):
    """Return parsed json file."""
    for prefix in config_paths():
        path = os.path.join(prefix,file_name)
        if (os.path.exists(path)):
            with open(path, 'r') as reader:
                return json.load(reader)

def config_paths():
    """Return path to config."""
    return [os.path.dirname(os.path.abspath(__file__)), '/config']
