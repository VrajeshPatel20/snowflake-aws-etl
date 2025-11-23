from typing import List, Dict, Optional
from etl_factory.utils.base import BaseOperator
from etl_factory.providers.twitter.hook import TwitterHook


class TwitterOperator(BaseOperator):
    """
    Operator for downloading data from Twitter API based on keywords.
    This class provides functionality to execute operations related to Twitter data extraction.
    """

    def __init__(self, keywords: Optional[List[str]] = None, username: Optional[str] = None,
                 max_results: int = 100, operation: str = "search",
                 tweet_fields: Optional[List[str]] = None,
                 user_fields: Optional[List[str]] = None,
                 expansions: Optional[List[str]] = None, **kwargs):
        """
        Initialize the TwitterOperator with search parameters.
        
        Args:
            keywords: List of keywords to search for (required for search operation)
            username: Twitter username (required for user_tweets operation)
            max_results: Maximum number of results to return
            operation: Operation to perform ("search", "user_tweets")
            tweet_fields: Additional tweet fields to include
            user_fields: Additional user fields to include
            expansions: Fields to expand in the response
            **kwargs: Additional keyword arguments passed to BaseOperator
        """
        super().__init__(config_section="TWITTER", **kwargs)
        self.keywords = keywords
        self.username = username
        self.max_results = max_results
        self.operation = operation
        self.tweet_fields = tweet_fields
        self.user_fields = user_fields
        self.expansions = expansions
        self.hook = TwitterHook(config_section="TWITTER")

    def execute(self) -> List[Dict]:
        """
        Execute the Twitter data extraction operation.
        
        Returns:
            List of dictionaries containing Twitter data
            
        Raises:
            ValueError: If operation is not supported or required parameters are missing
        """
        if self.operation == "search":
            if not self.keywords:
                raise ValueError("Keywords are required for search operation")
            return self.hook.search_tweets(
                keywords=self.keywords,
                max_results=self.max_results,
                tweet_fields=self.tweet_fields,
                user_fields=self.user_fields,
                expansions=self.expansions
            )
        elif self.operation == "user_tweets":
            if not self.username:
                raise ValueError("Username is required for user_tweets operation")
            return self.hook.get_user_tweets(
                username=self.username,
                max_results=self.max_results,
                tweet_fields=self.tweet_fields
            )
        else:
            raise ValueError(f"Unsupported operation: {self.operation}. Supported operations: search, user_tweets")

