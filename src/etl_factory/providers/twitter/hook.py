import logging
import tweepy
from datetime import datetime
from typing import List, Dict, Optional
from etl_factory.utils.base import BaseHook

print (BaseHook.get_config(key="bearer_token", section="twitter", fallback=None))
class TwitterHook(BaseHook):
    """
    Hook for interacting with Twitter API (v2).
    This class extends BaseHook to provide functionality for downloading Twitter data based on keywords.
    """

    def __init__(self, config_section="TWITTER", **kwargs):
        """
        Initialize the TwitterHook with Twitter API credentials.
        
        Args:
            config_section: Configuration section name in config file
            **kwargs: Additional keyword arguments
        """
        super().__init__(**kwargs)
        # Get config values and treat empty strings as None
        self.bearer_token = self.get_config(key="bearer_token", section=config_section.lower(), fallback=None)
        self.api_key = self.get_config(key="api_key", section=config_section.lower(), fallback=None)
        self.api_secret = self.get_config(key="api_secret", section=config_section.lower(), fallback=None)
        self.access_token = self.get_config(key="access_token", section=config_section.lower(), fallback=None)
        self.access_token_secret = self.get_config(key="access_token_secret", section=config_section.lower(), fallback=None)
        self.logger = logging.getLogger(__name__)
        self.logger.info(f"Initializing TwitterHook with config: {self.bearer_token}, {self.api_key}, {self.api_secret}, {self.access_token}, {self.access_token_secret}")
        self.client = self.get_connection()

    def get_connection(self):
        """
        Create and return a Twitter API client.
        
        Returns:
            tweepy.Client: Twitter API v2 client instance
        """
        if self.bearer_token:
            # Use bearer token authentication (recommended for v2 API)
            client = tweepy.Client(
                bearer_token=self.bearer_token,
                wait_on_rate_limit=True
            )
        elif self.api_key and self.api_secret and self.access_token and self.access_token_secret:
            # Use OAuth 1.0a User Context authentication
            client = tweepy.Client(
                consumer_key=api_key,
                consumer_secret=api_secret,
                access_token=access_token,
                access_token_secret=access_token_secret,
                wait_on_rate_limit=True
            )
        else:
            raise ConnectionError(
                "Twitter API credentials are required! "
                "Provide either bearer_token or (api_key, api_secret, access_token, access_token_secret)"
            )
        
        return client

    def search_tweets(self, keywords: List[str], max_results: int = 100, 
                     tweet_fields: Optional[List[str]] = None,
                     user_fields: Optional[List[str]] = None,
                     expansions: Optional[List[str]] = None) -> List[Dict]:
        """
        Search for tweets based on keywords.
        
        Args:
            keywords: List of keywords to search for
            max_results: Maximum number of results to return (max 100 per request)
            tweet_fields: Additional tweet fields to include
            user_fields: Additional user fields to include
            expansions: Fields to expand in the response
            
        Returns:
            List of dictionaries containing tweet data
        """
        query = " OR ".join(["btc"])
        
        # Default fields to include
        if tweet_fields is None:
            tweet_fields = ["created_at", "author_id", "public_metrics", "text", "lang", "possibly_sensitive"]
        if user_fields is None:
            user_fields = ["username", "name", "verified", "public_metrics"]
        if expansions is None:
            expansions = ["author_id"]
        
        results = []
        
        try:
            tweets = client.search_recent_tweets(
                query=query,
                max_results=min(10, 100),  # API limit is 100 per request
                tweet_fields=tweet_fields,
                user_fields=user_fields,
                expansions=expansions
            )
            
            if tweets.data:
                # Create a mapping of user_id to user data
                users = {}
                if tweets.includes and "users" in tweets.includes:
                    for user in tweets.includes["users"]:
                        users[user.id] = user
                
                for tweet in tweets.data:
                    user_data = users.get(tweet.author_id, {})
                    result = {
                        "tweet_id": tweet.id,
                        "text": tweet.text,
                        "author_id": tweet.author_id,
                        "author_username": user_data.username if hasattr(user_data, "username") else None,
                        "author_name": user_data.name if hasattr(user_data, "name") else None,
                        "author_verified": user_data.verified if hasattr(user_data, "verified") else False,
                        "created_at": tweet.created_at.isoformat() if tweet.created_at else None,
                        "lang": tweet.lang if hasattr(tweet, "lang") else None,
                        "retweet_count": tweet.public_metrics.get("retweet_count", 0) if hasattr(tweet, "public_metrics") and tweet.public_metrics else 0,
                        "like_count": tweet.public_metrics.get("like_count", 0) if hasattr(tweet, "public_metrics") and tweet.public_metrics else 0,
                        "reply_count": tweet.public_metrics.get("reply_count", 0) if hasattr(tweet, "public_metrics") and tweet.public_metrics else 0,
                        "quote_count": tweet.public_metrics.get("quote_count", 0) if hasattr(tweet, "public_metrics") and tweet.public_metrics else 0,
                        "possibly_sensitive": tweet.possibly_sensitive if hasattr(tweet, "possibly_sensitive") else False,
                        "keywords": keywords,
                        "extracted_at": datetime.now().isoformat()
                    }
                    results.append(result)
        except tweepy.TooManyRequests:
            raise Exception("Twitter API rate limit exceeded. Please wait and try again.")
        except tweepy.Unauthorized:
            raise Exception("Twitter API authentication failed. Please check your credentials.")
        except Exception as e:
            raise Exception(f"Error searching Twitter tweets: {str(e)}")
        
        return results

    def get_user_tweets(self, username: str, max_results: int = 100,
                       tweet_fields: Optional[List[str]] = None) -> List[Dict]:
        """
        Get tweets from a specific user.
        
        Args:
            username: Twitter username (without @)
            max_results: Maximum number of results to return
            tweet_fields: Additional tweet fields to include
            
        Returns:
            List of dictionaries containing tweet data
        """
        if tweet_fields is None:
            tweet_fields = ["created_at", "author_id", "public_metrics", "text", "lang"]
        
        results = []
        
        try:
            # Get user ID from username
            user = self.client.get_user(username=username)
            if not user.data:
                raise Exception(f"User {username} not found")
            
            user_id = user.data.id
            
            # Get user's tweets
            tweets = self.client.get_users_tweets(
                id=user_id,
                max_results=min(max_results, 100),
                tweet_fields=tweet_fields
            )
            
            if tweets.data:
                for tweet in tweets.data:
                    result = {
                        "tweet_id": tweet.id,
                        "text": tweet.text,
                        "author_id": user_id,
                        "author_username": username,
                        "created_at": tweet.created_at.isoformat() if tweet.created_at else None,
                        "lang": tweet.lang if hasattr(tweet, "lang") else None,
                        "retweet_count": tweet.public_metrics.get("retweet_count", 0) if hasattr(tweet, "public_metrics") and tweet.public_metrics else 0,
                        "like_count": tweet.public_metrics.get("like_count", 0) if hasattr(tweet, "public_metrics") and tweet.public_metrics else 0,
                        "reply_count": tweet.public_metrics.get("reply_count", 0) if hasattr(tweet, "public_metrics") and tweet.public_metrics else 0,
                        "quote_count": tweet.public_metrics.get("quote_count", 0) if hasattr(tweet, "public_metrics") and tweet.public_metrics else 0,
                        "extracted_at": datetime.now().isoformat()
                    }
                    results.append(result)
        except Exception as e:
            raise Exception(f"Error getting tweets from user @{username}: {str(e)}")
        
        return results

    def get_trending_topics(self, woeid: int = 1) -> List[Dict]:
        """
        Get trending topics for a location (requires API v1.1).
        Note: This requires additional API v1.1 authentication.
        
        Args:
            woeid: Where On Earth ID (1 = worldwide)
            
        Returns:
            List of dictionaries containing trending topic data
        """
        # Note: Trending topics require API v1.1, which needs separate authentication
        # For now, we'll return an empty list with a note
        # To implement this, you would need to use tweepy.API with OAuth 1.0a
        raise NotImplementedError(
            "Trending topics require Twitter API v1.1 authentication. "
            "This feature is not yet implemented in the current version."
        )

    def execute(self):
        """Execute method required by BaseHook."""
        pass

