import configparser
import os

def load_config(config_item):
    config = configparser.ConfigParser()
    config_folder = os.path.dirname(os.path.abspath(__file__))
    config.read(config_folder+'/config.cfg')
    conf = dict(config.items(config_item))

    return conf