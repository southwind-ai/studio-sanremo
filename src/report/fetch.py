"""
Raccoglie dati Spotify e YouTube per ogni artista in gara a Sanremo.
Output: CSV con colonne artista, brano, spotify_popularity, youtube_views, youtube_likes, youtube_comments
"""
import csv
import os
import sys
import time
import base64
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from dotenv import load_dotenv
from contestants import CONTESTANTS

load_dotenv()

SPOTIFY_CLIENT_ID = os.getenv("SPOTIFY_CLIENT_ID", "")
SPOTIFY_CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET", "")
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY", "")

MAX_RETRIES = 5
BACKOFF_FACTOR = 2


def _build_session():
    """Build a requests Session with automatic retry on transient errors."""
    session = requests.Session()
    retry = Retry(
        total=MAX_RETRIES,
        backoff_factor=BACKOFF_FACTOR,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def get_spotify_token():
    """Get Spotify access token using Client Credentials flow."""
    if not SPOTIFY_CLIENT_ID or not SPOTIFY_CLIENT_SECRET:
        print("Warning: Spotify credentials not configured, skipping Spotify data")
        return None
    
    auth_string = f"{SPOTIFY_CLIENT_ID}:{SPOTIFY_CLIENT_SECRET}"
    auth_bytes = auth_string.encode("utf-8")
    auth_base64 = base64.b64encode(auth_bytes).decode("utf-8")
    
    headers = {
        "Authorization": f"Basic {auth_base64}",
        "Content-Type": "application/x-www-form-urlencoded"
    }
    
    data = {"grant_type": "client_credentials"}
    
    try:
        response = requests.post(
            "https://accounts.spotify.com/api/token",
            headers=headers,
            data=data,
            timeout=10
        )
        response.raise_for_status()
        return response.json()["access_token"]
    except Exception as e:
        print(f"Error getting Spotify token: {e}")
        return None


def fetch_spotify_data(artist, song, token):
    """Fetch Spotify popularity for a song."""
    if not token:
        return None
    
    session = _build_session()
    headers = {"Authorization": f"Bearer {token}"}
    
    # Search for the song
    query = f"track:{song} artist:{artist}"
    params = {
        "q": query,
        "type": "track",
        "limit": 5,  # Fetch several results to pick the most popular version
        "market": "IT"
    }
    
    try:
        response = session.get(
            "https://api.spotify.com/v1/search",
            headers=headers,
            params=params,
            timeout=10
        )
        response.raise_for_status()
        data = response.json()
        
        if data.get("tracks", {}).get("items"):
            tracks = data["tracks"]["items"]
            # Pick the track with the highest popularity score.
            # This avoids picking a recently-uploaded / low-popularity version
            # that Spotify may rank first in the search results.
            best_track = max(tracks, key=lambda t: t.get("popularity", 0))
            return best_track.get("popularity", 0)
        return None
    except Exception as e:
        print(f"  Error fetching Spotify data for {artist} - {song}: {e}")
        return None


def fetch_youtube_data(artist, song):
    """Fetch YouTube data for a song (views, likes, comments)."""
    if not YOUTUBE_API_KEY:
        print("Warning: YouTube API key not configured, skipping YouTube data")
        return None, None, None
    
    session = _build_session()
    
    # Search for the video (assume it's the official RAI video)
    search_query = f"{artist} {song} Sanremo 2026"
    params = {
        "part": "snippet",
        "q": search_query,
        "type": "video",
        "maxResults": 1,
        "key": YOUTUBE_API_KEY
    }
    
    try:
        # First, search for the video
        search_response = session.get(
            "https://www.googleapis.com/youtube/v3/search",
            params=params,
            timeout=10
        )
        search_response.raise_for_status()
        search_data = search_response.json()
        
        if not search_data.get("items"):
            return None, None, None
        
        video_id = search_data["items"][0]["id"]["videoId"]
        
        # Then get video statistics
        stats_params = {
            "part": "statistics",
            "id": video_id,
            "key": YOUTUBE_API_KEY
        }
        
        stats_response = session.get(
            "https://www.googleapis.com/youtube/v3/videos",
            params=stats_params,
            timeout=10
        )
        stats_response.raise_for_status()
        stats_data = stats_response.json()
        
        if not stats_data.get("items"):
            return None, None, None
        
        stats = stats_data["items"][0]["statistics"]
        views = int(stats.get("viewCount", 0))
        likes = int(stats.get("likeCount", 0))
        comments = int(stats.get("commentCount", 0))
        
        return views, likes, comments
    except Exception as e:
        print(f"  Error fetching YouTube data for {artist} - {song}: {e}")
        return None, None, None


def fetch_data(serata):
    """Fetch data for all contestants and save to CSV."""
    print(f"Fetching data for Sanremo 2026 - Serata {serata}")
    print(f"Total contestants: {len(CONTESTANTS)}")
    
    # Get Spotify token once
    spotify_token = get_spotify_token()
    
    # Prepare output file
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    datasets_dir = os.path.join(project_root, "datasets")
    os.makedirs(datasets_dir, exist_ok=True)
    
    output_filename = f"sanremo_serata_{serata}.csv"
    output_file = os.path.join(datasets_dir, output_filename)
    output_file_relative = os.path.join("datasets", output_filename)
    
    # Fetch data for each contestant
    rows = []
    for i, (artist, song) in enumerate(CONTESTANTS, 1):
        print(f"[{i}/{len(CONTESTANTS)}] Fetching data for {artist} - {song}...")
        
        spotify_popularity = fetch_spotify_data(artist, song, spotify_token)
        # youtube_views, youtube_likes, youtube_comments = fetch_youtube_data(artist, song)
        
        rows.append({
            "artista": artist,
            "brano": song,
            "spotify_popularity": spotify_popularity if spotify_popularity is not None else "",
            # "youtube_views": youtube_views if youtube_views is not None else "",
            # "youtube_likes": youtube_likes if youtube_likes is not None else "",
            # "youtube_comments": youtube_comments if youtube_comments is not None else "",
        })
        
        # Rate limiting: small delay between requests
        time.sleep(0.5)
    
    # Write CSV
    fieldnames = ["artista", "brano", "spotify_popularity"] #, "youtube_views", "youtube_likes", "youtube_comments"]
    with open(output_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    
    print(f"✓ Saved {len(rows)} records to {output_file}")
    return output_file_relative


if __name__ == "__main__":
    serata = os.getenv("SERATA", "")
    if not serata:
        print("Usage: set SERATA=1 (or 2, 3, 4, 5) in .env")
        sys.exit(1)
    
    try:
        serata_num = int(serata)
        if serata_num < 1 or serata_num > 5:
            print("Error: SERATA must be between 1 and 5")
            sys.exit(1)
    except ValueError:
        print("Error: SERATA must be a number")
        sys.exit(1)
    
    fetch_data(serata_num)

