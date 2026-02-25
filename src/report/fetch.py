"""
Raccoglie dati dal megathread serale di r/italy dedicato al Festival di Sanremo 2026.
Utilizza la Reddit JSON API pubblica (nessuna autenticazione richiesta).

La comunità italiana di Reddit discute ogni serata in un unico megathread con migliaia
di commenti. Questo script scarica tutti i commenti del megathread (con paginazione)
e calcola per ogni artista: menzioni, score, sentiment e commenti di esempio.

Output CSV:
  artista, brano, reddit_mentions, reddit_score, reddit_comments,
  sentiment_score, sentiment_label, top_comments
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

# ── Megathread URLs per serata ────────────────────────────────────────────────
# Vengono aggiunti man mano che le serate si svolgono.
# Possono essere sovrascritti con la variabile d'ambiente MEGATHREAD_URL.
MEGATHREAD_URLS: dict[int, str] = {
    1: "https://www.reddit.com/r/italy/comments/1rdpfj8/megathread_76_festival_di_sanremo_prima_serata/",
    # 2: "https://www.reddit.com/r/italy/comments/...",
    # 3: "https://www.reddit.com/r/italy/comments/...",
    # 4: "https://www.reddit.com/r/italy/comments/...",
    # 5: "https://www.reddit.com/r/italy/comments/...",
}

# ── Reddit public API ─────────────────────────────────────────────────────────
REDDIT_BASE   = "https://www.reddit.com"
USER_AGENT    = "StudioSanremo/1.0"
REQUEST_DELAY = 1.2     # secondi tra chiamate API
MORE_BATCH    = 100     # commenti per chiamata morechildren


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


# ── Megathread helpers ─────────────────────────────────────────────────────────
def _parse_megathread_url(url: str) -> tuple[str, str]:
    """Estrae (subreddit, post_id) da un URL di thread Reddit."""
    m = re.search(r"/r/(\w+)/comments/(\w+)", url)
    if not m:
        raise ValueError(f"URL Reddit non riconoscibile: {url}")
    return m.group(1), m.group(2)


def _extract_comment_nodes(
    children: list,
    comments: list[dict],
    more_ids: list[str],
) -> None:
    """Ricorsivamente estrae commenti e raccoglie ID 'more' da una lista di nodi."""
    for child in children:
        kind = child.get("kind")
        if kind == "t1":
            data = child["data"]
            body = data.get("body", "")
            if body and body not in ("[deleted]", "[removed]"):
                comments.append({"body": body, "score": data.get("score", 0)})
            replies = data.get("replies", "")
            if isinstance(replies, dict):
                _extract_comment_nodes(
                    replies.get("data", {}).get("children", []),
                    comments,
                    more_ids,
                )
        elif kind == "more":
            more_ids.extend(child["data"].get("children", []))


def _fetch_all_megathread_comments(subreddit: str, post_id: str) -> tuple[dict, list[dict]]:
    """
    Scarica il post del megathread e TUTTI i suoi commenti, seguendo i link 'more'.
    Restituisce (post_dict, lista di {"body": …, "score": …}).
    """
    url = f"{REDDIT_BASE}/r/{subreddit}/comments/{post_id}.json"
    data = _reddit_get(url, {"limit": 500, "depth": 10, "sort": "confidence"})
    if not data or not isinstance(data, list) or len(data) < 2:
        return {}, []

    post = data[0]["data"]["children"][0]["data"]

    comments: list[dict] = []
    more_ids: list[str] = []
    _extract_comment_nodes(data[1]["data"]["children"], comments, more_ids)

    print(f"  Commenti iniziali: {len(comments)}  — ID 'more' da espandere: {len(more_ids)}")

    link_id = f"t3_{post_id}"
    for i in range(0, len(more_ids), MORE_BATCH):
        batch = more_ids[i : i + MORE_BATCH]
        more_data = _reddit_get(
            f"{REDDIT_BASE}/api/morechildren.json",
            {
                "api_type": "json",
                "link_id": link_id,
                "children": ",".join(batch),
                "sort": "confidence",
            },
        )
        if not more_data:
            continue
        things = more_data.get("json", {}).get("data", {}).get("things", [])
        extra_more: list[str] = []
        _extract_comment_nodes(things, comments, extra_more)
        print(f"    …{len(comments)} commenti scaricati (batch {i // MORE_BATCH + 1})")

    print(f"  ✓ Totale commenti nel megathread: {len(comments)}")
    return post, comments


# ── Sentiment ─────────────────────────────────────────────────────────────────
def _compute_sentiment(texts: list[str]) -> float:
    """Restituisce un punteggio in [-1.0, +1.0] usando il lessico italiano."""
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
    """True se il nome dell'artista (o una parte significativa) appare nel testo."""
    if not text:
        return False
    t = text.lower()
    a = artist.lower()
    if a in t:
        return True
    for part in a.split():
        if len(part) >= 4 and part in t:
            return True
    return False


# ── Metriche per artista ───────────────────────────────────────────────────────
def _metrics_for_artist(
    artist: str,
    all_comments: list[dict],
) -> tuple[int, int, float, str, str]:
    """
    Scansiona i commenti del megathread e restituisce:
      (mentions, total_score, sentiment_score, sentiment_label, top_comments)

    - mentions: numero di commenti che menzionano l'artista
    - total_score: somma degli upvote di quei commenti
    - top_comments: stringa ' || '-separata dei 3 commenti più votati
    """
    matching: list[dict] = [
        c for c in all_comments if _artist_in_text(artist, c["body"])
    ]

    mentions = len(matching)
    total_score = sum(c["score"] for c in matching)
    texts = [c["body"] for c in matching]

    score = _compute_sentiment(texts)
    label = _sentiment_label(score)

    top3_bodies = sorted(matching, key=lambda c: c["score"], reverse=True)[:3]
    top_comments = " || ".join(c["body"] for c in top3_bodies)

    return mentions, total_score, score, label, top_comments


# ── Public entry point ────────────────────────────────────────────────────────
def fetch_data(serata: int) -> str:
    """
    Scarica i commenti del megathread di r/italy per la serata indicata
    e scrive un CSV in datasets/.
    Restituisce il percorso relativo del CSV (per pipeline.py).
    """
    # Determina l'URL del megathread: env var ha priorità sulla mappa built-in
    megathread_url = os.getenv("MEGATHREAD_URL") or MEGATHREAD_URLS.get(serata)
    if not megathread_url:
        print(
            f"Errore: URL del megathread per la serata {serata} non configurato.\n"
            f"Impostare MEGATHREAD_URL nell'ambiente oppure aggiungere l'URL a MEGATHREAD_URLS in fetch.py."
        )
        sys.exit(1)

    subreddit, post_id = _parse_megathread_url(megathread_url)

    print(f"\n{'='*60}")
    print(f"Sanremo 2026 — Serata {serata} | Reddit megathread")
    print(f"URL : {megathread_url}")
    print(f"Contestants: {len(CONTESTANTS)}")
    print(f"{'='*60}\n")

    print("Scaricamento commenti del megathread…")
    post, all_comments = _fetch_all_megathread_comments(subreddit, post_id)
    total_thread_comments = len(all_comments)

    project_root  = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    datasets_dir  = os.path.join(project_root, "datasets")
    os.makedirs(datasets_dir, exist_ok=True)

    output_filename = f"sanremo_serata_{serata}.csv"
    output_file     = os.path.join(datasets_dir, output_filename)
    output_relative = os.path.join("datasets", output_filename)

    rows = []
    for i, (artist, song) in enumerate(CONTESTANTS, 1):
        print(f"[{i:02d}/{len(CONTESTANTS)}] {artist} — {song}")
        mentions, score, sent_score, sent_lbl, top_comments = _metrics_for_artist(
            artist, all_comments
        )
        print(
            f"       mentions={mentions}  score={score}"
            f"  sentiment={sent_lbl}({sent_score})"
        )
        rows.append({
            "artista":                artist,
            "brano":                  song,
            "reddit_mentions":        mentions,
            "reddit_score":           score,
            "reddit_total_comments":  total_thread_comments,
            "sentiment_score":        sent_score,
            "sentiment_label":        sent_lbl,
            "top_comments":           top_comments,
        })

    rows.sort(key=lambda r: (r["reddit_mentions"], r["reddit_score"]), reverse=True)

    fieldnames = [
        "artista", "brano",
        "reddit_mentions", "reddit_score", "reddit_total_comments",
        "sentiment_score", "sentiment_label",
        "top_comments",
    ]
    with open(output_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"\n✓ Salvati {len(rows)} record → {output_file}")
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
