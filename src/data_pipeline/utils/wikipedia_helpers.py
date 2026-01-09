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
from typing import Optional, List

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
    """
    clean_title = urllib.parse.unquote(title).replace("_", " ")
    cache_key = qid if qid else get_cache_key(clean_title)
    cache_file = WIKIPEDIA_CACHE_DIR / f"{cache_key}.txt"
    
    if await asyncio.to_thread(cache_file.exists):
        try:
            def read_file():
                with open(cache_file, "r", encoding="utf-8") as f:
                    return f.read()
            content = await asyncio.to_thread(read_file)
            if content:
                return content
        except OSError:
            pass

    params = {
        "action": "query",
        "format": "json",
        "titles": clean_title,
        "prop": "extracts",
        "explaintext": "True",
        "redirects": 1,
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
        pages = (data.get("query") or {}).get("pages") or {}
        
        for page_id, page_data in pages.items():
            if page_id == "-1":
                return None
            
            extract = page_data.get("extract")
            if extract:
                await _async_save_text_cache(WIKIPEDIA_CACHE_DIR, cache_key, extract)
                return extract
            
    except (httpx.HTTPError, ValueError):
        return None

    return None
