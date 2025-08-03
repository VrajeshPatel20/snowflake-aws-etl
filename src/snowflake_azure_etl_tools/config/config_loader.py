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

        config_dir = config_dir or Path(__file__).parent / "config"
        self.file_path = Path(config_dir) / f"{env}.ini"
        self.config = configparser.ConfigParser()
        self.config.read(self.file_path)

    def get_value(self, section, key, fallback=None):
        return self.config.get(section, key, fallback=fallback)

config = EnvConfigLoader()