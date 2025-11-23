from typing import List, Dict, Optional
from etl_factory.utils.base import BaseOperator
from etl_factory.providers.reddit.hook import RedditHook


class RedditOperator(BaseOperator):
    """
    Operator for downloading data from Reddit API based on keywords.
    This class provides functionality to execute operations related to Reddit data extraction.
    """

    def __init__(self, keywords: List[str], subreddit: Optional[str] = None, 
                 limit: int = 100, sort: str = "relevance", operation: str = "search", 
                 submission_id: Optional[str] = None, **kwargs):
        """
        Initialize the RedditOperator with search parameters.
        
        Args:
            keywords: List of keywords to search for
            subreddit: Optional subreddit to search in (None = all of Reddit)
            limit: Maximum number of results to return
            sort: Sort method ("relevance", "hot", "top", "new", "comments")
            operation: Operation to perform ("search", "hot_posts", "comments")
            submission_id: Submission ID (required for comments operation)
            **kwargs: Additional keyword arguments passed to BaseOperator
        """
        super().__init__(config_section="REDDIT", **kwargs)
        self.keywords = keywords
        self.subreddit = subreddit
        self.limit = limit
        self.sort = sort
        self.operation = operation
        self.submission_id = submission_id
        self.hook = RedditHook(config_section="REDDIT")

    def execute(self) -> List[Dict]:
        """
        Execute the Reddit data extraction operation.
        
        Returns:
            List of dictionaries containing Reddit data
            
        Raises:
            ValueError: If operation is not supported or required parameters are missing
        """
        if self.operation == "search":
            if not self.keywords:
                raise ValueError("Keywords are required for search operation")
            return self.hook.search_submissions(
                keywords=self.keywords,
                subreddit=self.subreddit,
                limit=self.limit,
                sort=self.sort
            )
        elif self.operation == "hot_posts":
            if not self.subreddit:
                raise ValueError("Subreddit is required for hot_posts operation")
            return self.hook.get_hot_posts(
                subreddit=self.subreddit,
                limit=self.limit,
                keywords=self.keywords
            )
        elif self.operation == "comments":
            if not self.submission_id:
                raise ValueError("Submission ID is required for comments operation")
            return self.hook.get_comments(
                submission_id=self.submission_id,
                limit=self.limit
            )
        else:
            raise ValueError(f"Unsupported operation: {self.operation}. Supported operations: search, hot_posts, comments")

