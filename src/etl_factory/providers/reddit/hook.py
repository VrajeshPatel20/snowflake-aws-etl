import praw
from datetime import datetime
from typing import List, Dict, Optional
from etl_factory.utils.base import BaseHook


class RedditHook(BaseHook):
    """
    Hook for interacting with Reddit API.
    This class extends BaseHook to provide functionality for downloading Reddit data based on keywords.
    """

    def __init__(self, config_section="REDDIT", **kwargs):
        """
        Initialize the RedditHook with Reddit API credentials.
        
        Args:
            config_section: Configuration section name in config file
            **kwargs: Additional keyword arguments
        """
        super().__init__(**kwargs)
        # Get config values and treat empty strings as None/empty
        client_id = self.get_config("client_id", section=config_section.lower(), fallback="") or None
        client_secret = self.get_config("client_secret", section=config_section.lower(), fallback="") or None
        user_agent = self.get_config("user_agent", section=config_section.lower(), fallback="ETL Pipeline/1.0") or "ETL Pipeline/1.0"
        username = self.get_config("username", section=config_section.lower(), fallback="") or None
        password = self.get_config("password", section=config_section.lower(), fallback="") or None
        
        self.client_id = client_id
        self.client_secret = client_secret
        self.user_agent = user_agent
        self.username = username
        self.password = password
        self.reddit = self.get_connection()

    def get_connection(self):
        """
        Create and return a Reddit API connection.
        
        Returns:
            praw.Reddit: Reddit API client instance
        """
        if not self.client_id or not self.client_secret:
            raise ConnectionError("Reddit client_id and client_secret are required!")
        
        # Build connection parameters
        connection_params = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "user_agent": self.user_agent
        }
        
        # Only add username/password if both are provided
        if self.username and self.password:
            connection_params["username"] = self.username
            connection_params["password"] = self.password
        
        reddit = praw.Reddit(**connection_params)
        
        # Verify connection if authenticated
        if self.username and self.password:
            try:
                reddit.user.me()
            except Exception:
                # If authentication fails, continue with read-only access
                pass
        
        return reddit

    def search_submissions(self, keywords: List[str], subreddit: Optional[str] = None, 
                          limit: int = 100, sort: str = "relevance") -> List[Dict]:
        """
        Search for Reddit submissions based on keywords.
        
        Args:
            keywords: List of keywords to search for
            subreddit: Optional subreddit to search in (None = all of Reddit)
            limit: Maximum number of results to return
            sort: Sort method ("relevance", "hot", "top", "new", "comments")
            
        Returns:
            List of dictionaries containing submission data
        """
        query = " OR ".join(keywords)
        results = []
        
        try:
            if subreddit:
                subreddit_obj = self.reddit.subreddit(subreddit)
                submissions = subreddit_obj.search(query, limit=limit, sort=sort)
            else:
                submissions = self.reddit.subreddit("all").search(query, limit=limit, sort=sort)
            
            for submission in submissions:
                result = {
                    "id": submission.id,
                    "title": submission.title,
                    "author": str(submission.author) if submission.author else "[deleted]",
                    "created_utc": datetime.fromtimestamp(submission.created_utc).isoformat(),
                    "score": submission.score,
                    "upvote_ratio": submission.upvote_ratio,
                    "num_comments": submission.num_comments,
                    "url": submission.url,
                    "selftext": submission.selftext,
                    "subreddit": str(submission.subreddit),
                    "permalink": f"https://reddit.com{submission.permalink}",
                    "is_self": submission.is_self,
                    "over_18": submission.over_18,
                    "keywords": keywords,
                    "extracted_at": datetime.now().isoformat()
                }
                results.append(result)
        except Exception as e:
            raise Exception(f"Error searching Reddit submissions: {str(e)}")
        
        return results

    def get_hot_posts(self, subreddit: str, limit: int = 100, keywords: Optional[List[str]] = None) -> List[Dict]:
        """
        Get hot posts from a subreddit, optionally filtered by keywords.
        
        Args:
            subreddit: Name of the subreddit
            limit: Maximum number of posts to return
            keywords: Optional list of keywords to filter posts by
            
        Returns:
            List of dictionaries containing post data
        """
        results = []
        
        try:
            subreddit_obj = self.reddit.subreddit(subreddit)
            posts = subreddit_obj.hot(limit=limit)
            
            for post in posts:
                # Filter by keywords if provided
                if keywords:
                    text_content = f"{post.title} {post.selftext}".lower()
                    if not any(keyword.lower() in text_content for keyword in keywords):
                        continue
                
                result = {
                    "id": post.id,
                    "title": post.title,
                    "author": str(post.author) if post.author else "[deleted]",
                    "created_utc": datetime.fromtimestamp(post.created_utc).isoformat(),
                    "score": post.score,
                    "upvote_ratio": post.upvote_ratio,
                    "num_comments": post.num_comments,
                    "url": post.url,
                    "selftext": post.selftext,
                    "subreddit": str(post.subreddit),
                    "permalink": f"https://reddit.com{post.permalink}",
                    "is_self": post.is_self,
                    "over_18": post.over_18,
                    "keywords": keywords or [],
                    "extracted_at": datetime.now().isoformat()
                }
                results.append(result)
        except Exception as e:
            raise Exception(f"Error getting hot posts from r/{subreddit}: {str(e)}")
        
        return results

    def get_comments(self, submission_id: str, limit: int = 100) -> List[Dict]:
        """
        Get comments from a Reddit submission.
        
        Args:
            submission_id: ID of the Reddit submission
            limit: Maximum number of comments to return
            
        Returns:
            List of dictionaries containing comment data
        """
        results = []
        
        try:
            submission = self.reddit.submission(id=submission_id)
            submission.comments.replace_more(limit=0)
            
            for comment in submission.comments.list()[:limit]:
                if hasattr(comment, "body"):
                    result = {
                        "id": comment.id,
                        "author": str(comment.author) if comment.author else "[deleted]",
                        "body": comment.body,
                        "created_utc": datetime.fromtimestamp(comment.created_utc).isoformat(),
                        "score": comment.score,
                        "submission_id": submission_id,
                        "submission_title": submission.title,
                        "extracted_at": datetime.now().isoformat()
                    }
                    results.append(result)
        except Exception as e:
            raise Exception(f"Error getting comments for submission {submission_id}: {str(e)}")
        
        return results

    def execute(self):
        """Execute method required by BaseHook."""
        pass

