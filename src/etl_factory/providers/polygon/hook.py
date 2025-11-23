from polygon import RESTClient
from datetime import datetime
import os
from etl_factory.utils.base import BaseHook

class PolygonHook(BaseHook):
    """
    Hook for interacting with Polygon API.
    This class extends BaseHook to provide functionality specific to Polygon.
    """

    def __init__(self, **kwargs):
        """
        Initialize the PolygonHook with any necessary parameters.
        """
        super().__init__(**kwargs)
        # Get API key from config - section is "polygon", key is "api_key_path"
        # Note: The config value is the API key itself, not a file path
        api_key_path = self.get_config(key="api_key_path", section="polygon", fallback=None)
        
        # If api_key_path is actually a file path, read it; otherwise use it directly as the API key
        if api_key_path and os.path.exists(api_key_path):
            self.api_key = self.read_file(file_path=api_key_path)
        else:
            # The config value is the API key itself
            self.api_key = api_key_path
        
        # Remove quotes if present (configparser might include them)
        self.api_key="xcsDMIO3anKwadWGMGO7Q1CVUQbD6zOv"
        if self.api_key:
            self.api_key = self.api_key.strip('"').strip("'")
        
        print(f"Polygon API key loaded: {'*' * (len(self.api_key) - 4) + self.api_key[-4:] if self.api_key and len(self.api_key) > 4 else 'None'}")
        self.conn = self.get_connection()

    @staticmethod
    def read_file(key=None, file_path=None):
        """
        Read the content of a file. 

        Args:
            key (str): Key to be read.
            file_path (str): Path to the file to be read.

        Returns:
            str: Content of the file.
        """
        print(key)
        if key is not None:
            return key
        if file_path is not None:
            with open(file_path, 'r') as file:
                return file.read()
        return None

    def get_connection(self):
        if self.api_key is None:
            raise ConnectionError("API key is not passed!!")
        conn = RESTClient(api_key=self.api_key, trace=True)
        return conn

    def get_aggregates(self, ticker, multiplier=1, timespan="minute",from_="2025-08-05", to=datetime.now().strftime("%Y-%m-%d"), limit=50000):
        return [a for a in self.conn.list_aggs(ticker=ticker, multiplier=multiplier,timespan=timespan, from_=from_, to=to,limit=limit)]

    def get_last_trade(self, ticker):
        trade = self.conn.get_last_trade(ticker=ticker)
        return trade

    def list_trades(self, ticker, timestamp=datetime.now().strftime("%Y-%m-%d")):
        trades = self.conn.list_trades(ticker=ticker, timestamp=timestamp)
        return trades

    def get_last_quote(self, ticker):
        quote = self.conn.get_last_quote(ticker=ticker)
        return quote

    def list_quotes(self, ticker, timestamp=datetime.now().strftime("%Y-%m-%d")):
        quotes = self.conn.list_quotes(ticker=ticker, timestamp=timestamp)
        return quotes

    def execute(self):
        pass