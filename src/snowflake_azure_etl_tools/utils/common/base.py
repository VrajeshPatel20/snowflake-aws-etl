from src.snowflake_azure_etl_tools.config.config_loader import config


class Base:
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        for key, value in kwargs.items():
            setattr(self, key, value)

    @staticmethod
    def get_config(key, section):
        return config.get_value(key=key, section=section)