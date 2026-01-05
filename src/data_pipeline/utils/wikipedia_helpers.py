# -----------------------------------------------------------
# Wikipedia API Helpers
# Dagster Data pipeline for Structured and Unstructured Data
#
# (C) 2025-2026 Juan-Francisco Reyes, Cottbus, Germany
# Released under MIT License
# email pacoreyes@protonmail.com
# -----------------------------------------------------------

import asyncio
import hashlib
import re
import urllib.parse
from pathlib import Path
from typing import Optional

import httpx
from dagster import AssetExecutionContext

from data_pipeline.settings import settings
from data_pipeline.utils.network_helpers import make_async_request_with_retries

WIKIPEDIA_CACHE_DIR = settings.wikipedia_cache_dirpath


def get_cache_key(text: str) -> str:
    """Creates a SHA256 hash of a string to use as a cache key."""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


async def _async_save_text_cache(cache_dir: Path, key: str, content: str) -> None:
    """Helper function to cache text content asynchronously."""
    cache_file = cache_dir / f"{key}.txt"
    await asyncio.to_thread(cache_dir.mkdir, parents=True, exist_ok=True)

    def write_file():
        with open(cache_file, "w", encoding="utf-8") as f:
            f.write(content)

    await asyncio.to_thread(write_file)


async def async_fetch_wikipedia_article(
    context: AssetExecutionContext,
    title: str,
    qid: Optional[str] = None,
    client: Optional[httpx.AsyncClient] = None,
) -> Optional[str]:
    """
    Fetches the raw plain text of a Wikipedia article by its title, with caching.
    Uses QID as the primary cache key if provided, falling back to the title hash.

    Args:
        context: Dagster execution context for logging.
        title: The title of the Wikipedia article (e.g., "Nirvana_(band)").
        qid: Optional Wikidata QID to use as cache key.
        client: Optional httpx.AsyncClient to reuse connections.

    Returns:
        The raw text of the article if successful, None otherwise.
    """
    # Ensure title is properly encoded for the query parameter
    clean_title = urllib.parse.unquote(title).replace("_", " ")

    # Determine Cache Key: QID preferred, then Title Hash
    cache_key = qid if qid else get_cache_key(clean_title)
    cache_file = WIKIPEDIA_CACHE_DIR / f"{cache_key}.txt"
    
    # 1. CHECK CACHE FIRST
    if await asyncio.to_thread(cache_file.exists):
        try:
            def read_file():
                with open(cache_file, "r", encoding="utf-8") as f:
                    return f.read()
            
            content = await asyncio.to_thread(read_file)
            if content:
                context.log.debug(f"Using cached Wikipedia article for '{clean_title}' (Key: {cache_key})")
                return content
        except Exception as e:
            context.log.warning(f"Failed to read cache for '{clean_title}' (Key: {cache_key}): {e}")

    # 2. FETCH FROM API IF MISS
    params = {
        "action": "query",
        "format": "json",
        "titles": clean_title,
        "prop": "extracts",
        "explaintext": "True",  # Get plain text, not HTML
        "redirects": 1,         # Follow redirects
    }

    try:
        response = await make_async_request_with_retries(
            context=context,
            url=settings.WIKIPEDIA_API_URL,
            method="GET",
            params=params,
            headers=settings.default_request_headers,
            client=client,
        )
        
        data = response.json()
        pages = data.get("query", {}).get("pages", {})
        
        for page_id, page_data in pages.items():
            if page_id == "-1":
                context.log.warning(f"Wikipedia article not found for title: '{clean_title}'")
                return None
            
            extract = page_data.get("extract")
            if extract:
                # 3. SAVE TO CACHE
                await _async_save_text_cache(WIKIPEDIA_CACHE_DIR, cache_key, extract)
                return extract
            
    except Exception as e:
        context.log.error(f"Error fetching Wikipedia article '{clean_title}': {e}")
        return None

    return None


def clean_wikipedia_text(text: str) -> str:
    """
    Cleans raw Wikipedia text by removing footer sections (References, External links, etc.)
    and standardizing whitespace.

    Args:
        text: The raw text from the Wikipedia API.

    Returns:
        Cleaned text suitable for chunking.
    """
    if not text:
        return ""

    # 1. Remove standard footer sections
    # These headers usually appear on their own lines like "== References ==" or "==External links=="
    # We will look for the first occurrence of these and cut off everything after.
    # Common footer sections in English Wikipedia:
    footer_headers = [
        "== References ==",
        "== External links ==",
        "== See also ==",
        "== Further reading ==",
        "== Notes ==",
        "== Discography ==", # Optional: Discographies can be long lists, might dilute semantic search? 
                            # Keeping Discography might be useful for "albums by..." queries. 
                            # Leaving Discography IN for now as it's relevant content.
    ]

    # Normalize headers to catch variations like "==References==" (no spaces)
    # The API 'explaintext' usually formats them as "== Header =="
    
    # We'll use a regex to find the FIRST occurrence of any of the exclusion headers
    # and truncate the text there.
    
    # Pattern explanation:
    # ^==\s*      : Start of line, '==', optional whitespace
    # (References|External links|See also|Further reading|Notes|Discography) : The headers
    # \s*==       : Optional whitespace, '=='
    pattern = re.compile(
        r"^\s*==\s*(References|External links|See also|Further reading|Notes|Discography)\s*==", 
        re.MULTILINE | re.IGNORECASE
    )
    
    match = pattern.search(text)
    if match:
        # Cut text at the start of the match
        text = text[:match.start()]

    # 2. Remove multiple newlines/whitespace
    # Replace 3+ newlines with 2 (paragraph break)
    text = re.sub(r"\n{3,}", "\n\n", text)
    
    # Strip leading/trailing whitespace
    text = text.strip()

    return text
