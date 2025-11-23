import logging
from datetime import datetime
from typing import Dict, List, Optional

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from etl_factory.utils.base import BaseHook


class YouTubeHook(BaseHook):
    """Hook for interacting with the YouTube Data API v3."""

    def __init__(self, config_section: str = "YOUTUBE", **kwargs):
        """Initialize the YouTube hook with API credentials."""
        super().__init__(**kwargs)
        self.api_key = self.get_config(key="api_key", section=config_section.lower(), fallback=None)
        if not self.api_key:
            raise ValueError("YouTube API key is required")

        self.logger = logging.getLogger(__name__)
        self.client = self.get_connection()

    def get_connection(self):
        """Create a YouTube API client."""
        try:
            return build("youtube", "v3", developerKey=self.api_key)
        except Exception as exc:
            raise ConnectionError("Failed to initialize YouTube API client") from exc

    def search_videos(
        self,
        keywords: List[str],
        max_results: int = 50,
        published_after: Optional[str] = None,
        published_before: Optional[str] = None,
        region_code: Optional[str] = None,
    ) -> List[Dict]:
        """Search for YouTube videos matching the specified keywords."""
        query = " ".join(keywords)
        params: Dict[str, object] = {
            "q": query,
            "part": "snippet",
            "type": "video",
            "order": "date",
            "maxResults": min(max_results, 50),  # API limit is 50
        }

        if published_after:
            params["publishedAfter"] = published_after
        if published_before:
            params["publishedBefore"] = published_before
        if region_code:
            params["regionCode"] = region_code

        try:
            response = self.client.search().list(**params).execute()
        except HttpError as exc:
            raise Exception(f"YouTube API search error: {exc}") from exc

        results: List[Dict] = []
        for item in response.get("items", []):
            snippet = item.get("snippet", {})
            results.append(
                {
                    "video_id": item.get("id", {}).get("videoId"),
                    "title": snippet.get("title"),
                    "description": snippet.get("description"),
                    "channel_id": snippet.get("channelId"),
                    "channel_title": snippet.get("channelTitle"),
                    "published_at": snippet.get("publishedAt"),
                    "thumbnails": snippet.get("thumbnails", {}),
                    "keywords": keywords,
                    "extracted_at": datetime.utcnow().isoformat(),
                }
            )

        return results

    def get_channel_statistics(self, channel_id: str) -> Dict:
        """Fetch summary information for a single YouTube channel."""
        try:
            response = (
                self.client.channels()
                .list(id=channel_id, part="snippet,statistics,contentDetails")
                .execute()
            )
        except HttpError as exc:
            raise Exception(f"YouTube API channel lookup error: {exc}") from exc

        if not response.get("items"):
            raise ValueError(f"Channel {channel_id} not found")

        channel = response["items"][0]
        snippet = channel.get("snippet", {})
        statistics = channel.get("statistics", {})
        content_details = channel.get("contentDetails", {})

        return {
            "channel_id": channel.get("id"),
            "title": snippet.get("title"),
            "description": snippet.get("description"),
            "country": snippet.get("country"),
            "published_at": snippet.get("publishedAt"),
            "thumbnails": snippet.get("thumbnails", {}),
            "statistics": statistics,
            "related_playlists": content_details.get("relatedPlaylists", {}),
            "extracted_at": datetime.utcnow().isoformat(),
        }

    def get_video_details(self, video_ids: List[str], parts: Optional[List[str]] = None) -> List[Dict]:
        """Retrieve detailed metadata for one or more videos."""
        if not video_ids:
            return []

        part = ",".join(parts) if parts else "snippet,statistics,contentDetails"

        try:
            response = (
                self.client.videos().list(id=",".join(video_ids), part=part).execute()
            )
        except HttpError as exc:
            raise Exception(f"YouTube API video detail error: {exc}") from exc

        video_details: List[Dict] = []
        for item in response.get("items", []):
            video_details.append(
                {
                    "video_id": item.get("id"),
                    "snippet": item.get("snippet", {}),
                    "statistics": item.get("statistics", {}),
                    "content_details": item.get("contentDetails", {}),
                    "extracted_at": datetime.utcnow().isoformat(),
                }
            )

        return video_details

    def execute(self):
        """Execute method required by BaseHook."""
        pass
