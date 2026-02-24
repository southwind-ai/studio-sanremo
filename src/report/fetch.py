"""
Raccoglie dati da Reddit (r/italy, r/italyMusic) per ogni artista in gara a Sanremo.
Utilizza la Reddit JSON API pubblica (nessuna autenticazione richiesta).

Output CSV:
  artista, brano, reddit_mentions, reddit_score, reddit_comments,
  sentiment_score, sentiment_label
"""
import csv
import os
import re
import sys
import time

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from dotenv import load_dotenv
from contestants import CONTESTANTS

load_dotenv()

# ── Reddit public API ─────────────────────────────────────────────────────────
REDDIT_BASE  = "https://www.reddit.com"
SUBREDDITS   = ["SanremoFestival", "italy", "Festival_di_Sanremo"]
USER_AGENT   = "StudioSanremo/1.0 (data-analysis project)"

# How far back to look: "day" | "week" | "month" | "year" | "all"
TIME_FILTER  = "month"
MAX_POSTS    = 100          # posts per subreddit per query
REQUEST_DELAY = 1.2         # seconds between Reddit API calls


# ── Italian sentiment lexicon ─────────────────────────────────────────────────
POSITIVE_WORDS = {
    "bravo", "brava", "bravi", "brave", "fantastico", "fantastica",
    "bellissimo", "bellissima", "ottimo", "ottima", "perfetto", "perfetta",
    "straordinario", "straordinaria", "meraviglioso", "meravigliosa",
    "top", "migliore", "bello", "bella", "stupendo", "stupenda",
    "emozionante", "commovente", "potente", "forte", "epico", "epica",
    "capolavoro", "amore", "adoro", "tifo", "favorito", "favorita",
    "fenomeno", "talento", "applausi", "applauso", "vincitore", "vincitrice",
    "vince", "vincerà", "eccellente", "grandioso", "grandiosa", "magnifico",
    "magnifica", "sorprendente", "incantevole", "toccante", "originale",
    "creativo", "creativa", "innovativo", "innovativa", "coinvolgente",
    "energia", "energico", "esplosivo", "esplosiva", "unico", "unica",
    "pazzesco", "pazzesca", "incredibile", "piace", "benissimo", "super",
    "grande", "grandi", "wow", "bene", "simpatico", "simpatica",
}

NEGATIVE_WORDS = {
    "pessimo", "pessima", "orribile", "deludente", "delude", "scadente",
    "noioso", "noiosa", "sbagliato", "sbagliata", "odio", "detesto",
    "brutto", "brutta", "terribile", "disastroso", "disastrosa",
    "delusione", "banale", "banalità", "inutile", "scarso", "scarsa",
    "mediocre", "ridicolo", "ridicola", "imbarazzante", "spazzatura",
    "trash", "schifo", "schifoso", "schifosa", "orrendo", "orrenda",
    "insopportabile", "flop", "fallimento", "incapace", "scialbo", "scialba",
    "spento", "spenta", "piatto", "piatta", "vuoto", "vuota",
    "falso", "falsa", "copiato", "copiata", "antipatico", "antipatica",
    "stonato", "stonata", "pessimamente",
}


# ── HTTP session ───────────────────────────────────────────────────────────────
def _build_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=5,
        backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update({"User-Agent": USER_AGENT})
    return session


SESSION = _build_session()


def _reddit_get(url: str, params: dict | None = None) -> dict | list | None:
    """GET a Reddit JSON endpoint with rate-limiting."""
    time.sleep(REQUEST_DELAY)
    try:
        resp = SESSION.get(url, params=params, timeout=20)
        if resp.status_code == 429:
            print("  ⚠ Rate limited by Reddit — sleeping 30 s...")
            time.sleep(30)
            resp = SESSION.get(url, params=params, timeout=20)
        resp.raise_for_status()
        return resp.json()
    except Exception as exc:
        print(f"  Reddit API error [{url}]: {exc}")
        return None


# ── Reddit helpers ────────────────────────────────────────────────────────────
def _search_subreddit(subreddit: str, query: str) -> list[dict]:
    """Return a list of post dicts from a subreddit search."""
    url = f"{REDDIT_BASE}/r/{subreddit}/search.json"
    params = {
        "q": query,
        "sort": "top",
        "t": TIME_FILTER,
        "limit": MAX_POSTS,
        "restrict_sr": "true",
    }
    data = _reddit_get(url, params)
    if not data:
        return []
    children = data.get("data", {}).get("children", [])
    return [c["data"] for c in children if c.get("kind") == "t3"]


def _fetch_comments(subreddit: str, post_id: str, limit: int = 60) -> list[str]:
    """Return the body text of top-level comments for a post."""
    url = f"{REDDIT_BASE}/r/{subreddit}/comments/{post_id}.json"
    params = {"limit": limit, "depth": 1, "sort": "top"}
    data = _reddit_get(url, params)
    if not data or not isinstance(data, list) or len(data) < 2:
        return []
    children = data[1].get("data", {}).get("children", [])
    return [
        c["data"]["body"]
        for c in children
        if c.get("kind") == "t1"
        and c["data"].get("body") not in (None, "[deleted]", "[removed]")
    ]


# ── Sentiment ─────────────────────────────────────────────────────────────────
def _compute_sentiment(texts: list[str]) -> float:
    """Return a score in [-1.0, +1.0] using the Italian lexicon above."""
    pos = neg = 0
    for text in texts:
        for word in re.findall(r"\b\w+\b", text.lower()):
            if word in POSITIVE_WORDS:
                pos += 1
            elif word in NEGATIVE_WORDS:
                neg += 1
    total = pos + neg
    return round((pos - neg) / total, 3) if total else 0.0


def _sentiment_label(score: float) -> str:
    if score > 0.1:
        return "positivo"
    if score < -0.1:
        return "negativo"
    return "neutro"


# ── Artist matching ───────────────────────────────────────────────────────────
def _artist_in_text(artist: str, text: str) -> bool:
    """True if the artist name (or a significant part of it) appears in text."""
    if not text:
        return False
    t = text.lower()
    a = artist.lower()
    if a in t:
        return True
    # Try each word part (≥ 4 chars) — catches "Annalisa", "Mahmood", …
    for part in a.split():
        if len(part) >= 4 and part in t:
            return True
    return False


# ── Data collection ───────────────────────────────────────────────────────────
def _collect_all_posts() -> list[tuple[dict, list[str]]]:
    """
    Fetch all Sanremo-related posts from every configured subreddit,
    then fetch their top-level comments.
    Returns: list of (post_dict, [comment_text, …])
    """
    queries = ["sanremo 2026", "festival sanremo", "sanremo"]

    seen_ids: set[str] = set()
    subreddit_of: dict[str, str] = {}   # post_id → subreddit (for comment fetching)
    raw_posts: list[dict] = []

    for subreddit in SUBREDDITS:
        for query in queries:
            print(f"  Searching r/{subreddit}: '{query}'…")
            for post in _search_subreddit(subreddit, query):
                pid = post.get("id")
                if pid and pid not in seen_ids:
                    seen_ids.add(pid)
                    subreddit_of[pid] = subreddit
                    raw_posts.append(post)

    print(f"  Found {len(raw_posts)} unique posts across {SUBREDDITS}")

    print("  Fetching comments…")
    result: list[tuple[dict, list[str]]] = []
    for i, post in enumerate(raw_posts, 1):
        pid = post.get("id", "")
        sub = subreddit_of.get(pid, SUBREDDITS[0])
        comments = _fetch_comments(sub, pid) if post.get("num_comments", 0) > 0 else []
        result.append((post, comments))
        if i % 10 == 0:
            print(f"    …{i}/{len(raw_posts)} posts processed")

    print(f"  ✓ Corpus ready: {len(result)} posts")
    return result


def _metrics_for_artist(
    artist: str,
    corpus: list[tuple[dict, list[str]]],
) -> tuple[int, int, int, float, str]:
    """
    Scan the pre-fetched corpus and return:
      (mentions, total_score, total_comments, sentiment_score, sentiment_label)
    """
    mentions = total_score = total_comments = 0
    relevant_texts: list[str] = []

    for post, comment_texts in corpus:
        post_text = (post.get("title", "") + " " + post.get("selftext", "")).strip()
        in_post   = _artist_in_text(artist, post_text)
        in_comments = any(_artist_in_text(artist, c) for c in comment_texts)

        if in_post or in_comments:
            mentions        += 1
            total_score     += post.get("score", 0)
            total_comments  += post.get("num_comments", 0)

            if in_post:
                relevant_texts.append(post_text)
            relevant_texts.extend(c for c in comment_texts if _artist_in_text(artist, c))

    score = _compute_sentiment(relevant_texts)
    label = _sentiment_label(score)
    return mentions, total_score, total_comments, score, label


# ── Public entry point ────────────────────────────────────────────────────────
def fetch_data(serata: int) -> str:
    """
    Fetch Reddit data for all contestants and write a CSV to datasets/.
    Returns the relative path to the CSV (for use by pipeline.py).
    """
    print(f"\n{'='*60}")
    print(f"Sanremo 2026 — Serata {serata} | Reddit data collection")
    print(f"Subreddits : {', '.join('r/'+s for s in SUBREDDITS)}")
    print(f"Contestants: {len(CONTESTANTS)}")
    print(f"{'='*60}\n")

    corpus = _collect_all_posts()

    project_root  = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    datasets_dir  = os.path.join(project_root, "datasets")
    os.makedirs(datasets_dir, exist_ok=True)

    output_filename = f"sanremo_serata_{serata}.csv"
    output_file     = os.path.join(datasets_dir, output_filename)
    output_relative = os.path.join("datasets", output_filename)

    rows = []
    for i, (artist, song) in enumerate(CONTESTANTS, 1):
        print(f"[{i:02d}/{len(CONTESTANTS)}] {artist} — {song}")
        mentions, score, comments, sent_score, sent_lbl = _metrics_for_artist(artist, corpus)
        print(f"       mentions={mentions}  score={score}  comments={comments}  sentiment={sent_lbl}({sent_score})")
        rows.append({
            "artista":          artist,
            "brano":            song,
            "reddit_mentions":  mentions,
            "reddit_score":     score,
            "reddit_comments":  comments,
            "sentiment_score":  sent_score,
            "sentiment_label":  sent_lbl,
        })

    # Sort by discussion volume (mentions desc, then score desc)
    rows.sort(key=lambda r: (r["reddit_mentions"], r["reddit_score"]), reverse=True)

    fieldnames = [
        "artista", "brano",
        "reddit_mentions", "reddit_score", "reddit_comments",
        "sentiment_score", "sentiment_label",
    ]
    with open(output_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"\n✓ Saved {len(rows)} records → {output_file}")
    return output_relative


if __name__ == "__main__":
    serata_env = os.getenv("SERATA", "")
    if not serata_env:
        print("Usage: set SERATA=1 (or 2..5) in .env or environment")
        sys.exit(1)
    try:
        serata_num = int(serata_env)
        if not 1 <= serata_num <= 5:
            raise ValueError
    except ValueError:
        print("Error: SERATA must be an integer between 1 and 5")
        sys.exit(1)

    fetch_data(serata_num)
