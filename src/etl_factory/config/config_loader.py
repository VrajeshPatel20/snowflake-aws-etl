import configparser
from pathlib import Path
import json
import os

env = os.getenv("run_env", "local")

def remove_quotes(d):
    if isinstance(d, dict):
        return {k: remove_quotes(v) for k, v in d.items()}
    elif isinstance(d, list):
        return [remove_quotes(i) for i in d]
    elif isinstance(d, str):
        return d.strip('"')
    else:
        return d
class EnvConfigLoader:
    def __init__(self, config_dir=None):

        config_dir = Path(__file__).parent
        self.file_path = Path(config_dir) / f"{env}.ini"
        self.config = configparser.ConfigParser(interpolation=None)
        print (self.file_path)
        print(config_dir)
        self.config.read(self.file_path)

    def get_value(self, section, key, fallback=None):
        try:
            return self.config.get(section, key, fallback=fallback)
        except configparser.NoSectionError:
            # If section doesn't exist and fallback is provided, return fallback
            # Otherwise, raise the error
            if fallback is not None:
                return fallback
            raise

config = EnvConfigLoader()