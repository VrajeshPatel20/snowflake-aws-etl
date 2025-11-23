from typing import List, Dict, Optional
from datetime import datetime, timedelta
from etl_factory.utils.base import BaseOperator
from etl_factory.providers.polygon.hook import PolygonHook


class PolygonOperator(BaseOperator):
    """
    Operator for downloading stock data from Polygon API.
    This class provides functionality to execute operations related to stock data extraction.
    """

    def __init__(self, ticker: str, operation: str = "aggregates", 
                 multiplier: int = 1, timespan: str = "day",
                 from_date: Optional[str] = None, to_date: Optional[str] = None,
                 limit: int = 5000, **kwargs):
        """
        Initialize the PolygonOperator with stock data parameters.
        
        Args:
            ticker: Stock ticker symbol (e.g., "AAPL", "MSFT")
            operation: Operation to perform ("aggregates", "last_trade", "trades", "last_quote", "quotes")
            multiplier: Size of the timespan multiplier
            timespan: Size of the time window (minute, hour, day, week, month, quarter, year)
            from_date: Start date (YYYY-MM-DD format, defaults to 30 days ago)
            to_date: End date (YYYY-MM-DD format, defaults to today)
            limit: Maximum number of results to return
            **kwargs: Additional keyword arguments passed to BaseOperator
        """
        super().__init__(config_section="POLYGON", **kwargs)
        self.ticker = ticker
        self.operation = operation
        self.multiplier = multiplier
        self.timespan = timespan
        self.from_date = from_date or (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
        self.to_date = to_date or datetime.now().strftime("%Y-%m-%d")
        self.limit = limit
        self.hook = PolygonHook()

    def execute(self) -> List[Dict]:
        """
        Execute the stock data extraction operation.
        
        Returns:
            List of dictionaries containing stock data
            
        Raises:
            ValueError: If operation is not supported or required parameters are missing
        """
        if self.operation == "aggregates":
            aggregates = self.hook.get_aggregates(
                ticker=self.ticker,
                multiplier=self.multiplier,
                timespan=self.timespan,
                from_=self.from_date,
                to=self.to_date,
                limit=self.limit
            )
            # Convert to list of dictionaries
            data = []
            for agg in aggregates:
                data.append({
                    "ticker": self.ticker,
                    "timestamp": datetime.fromtimestamp(agg.timestamp / 1000).isoformat() if hasattr(agg, 'timestamp') else None,
                    "open": agg.open if hasattr(agg, 'open') else None,
                    "high": agg.high if hasattr(agg, 'high') else None,
                    "low": agg.low if hasattr(agg, 'low') else None,
                    "close": agg.close if hasattr(agg, 'close') else None,
                    "volume": agg.volume if hasattr(agg, 'volume') else None,
                    "vwap": agg.vwap if hasattr(agg, 'vwap') else None,
                    "transactions": agg.transactions if hasattr(agg, 'transactions') else None,
                    "extracted_at": datetime.now().isoformat()
                })
            return data
            
        elif self.operation == "last_trade":
            trade = self.hook.get_last_trade(self.ticker)
            return [{
                "ticker": self.ticker,
                "price": trade.price if hasattr(trade, 'price') else None,
                "size": trade.size if hasattr(trade, 'size') else None,
                "timestamp": datetime.fromtimestamp(trade.sip_timestamp / 1000000000).isoformat() if hasattr(trade, 'sip_timestamp') else None,
                "extracted_at": datetime.now().isoformat()
            }]
            
        elif self.operation == "last_quote":
            quote = self.hook.get_last_quote(self.ticker)
            return [{
                "ticker": self.ticker,
                "bid": quote.bid if hasattr(quote, 'bid') else None,
                "ask": quote.ask if hasattr(quote, 'ask') else None,
                "bid_size": quote.bid_size if hasattr(quote, 'bid_size') else None,
                "ask_size": quote.ask_size if hasattr(quote, 'ask_size') else None,
                "timestamp": datetime.fromtimestamp(quote.sip_timestamp / 1000000000).isoformat() if hasattr(quote, 'sip_timestamp') else None,
                "extracted_at": datetime.now().isoformat()
            }]
            
        else:
            raise ValueError(f"Unsupported operation: {self.operation}. Supported operations: aggregates, last_trade, last_quote")
