# -----------------------------------------------------------
# Wikipedia API Helpers
# Dagster Data pipeline for Structured and Unstructured Data
#
# (C) 2025-2026 Juan-Francisco Reyes, Cottbus, Germany
# Released under MIT License
# email pacoreyes@protonmail.com
# ----------------------------------------------------------- 

import re
import urllib.parse
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator, Optional

from dagster import AssetExecutionContext

from data_pipeline.utils.network_helpers import (
    make_async_request_with_retries,
    AsyncClient,
    HTTPError,
)
from data_pipeline.utils.io_helpers import async_read_text_file, async_write_text_file


async def async_fetch_wikipedia_article(
    context: AssetExecutionContext,
    title: str,
    qid: str,
    api_url: str,
    cache_dir: Path,
    headers: Optional[dict[str, str]] = None,
    client: Optional[AsyncClient] = None,
    rate_limit_delay: float = 0.0,
) -> Optional[str]:
    """
    Fetches the raw plain text of a Wikipedia article by its title, with caching.
    The QID is required to ensure consistent cache file naming (e.g., Q123.txt).
    """
    clean_title = urllib.parse.unquote(title).replace("_", " ")
    
    # Use QID for cache filename
    cache_file = cache_dir / f"{qid}.txt"
    
    # 1. Check Cache
    cached_content = await async_read_text_file(cache_file)
    if cached_content:
        return cached_content

    # 2. Fetch from API
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
            url=api_url,
            method="GET",
            params=params,
            headers=headers,
            client=client,
            rate_limit_delay=rate_limit_delay,
        )
        
        data = response.json()
        pages = (data.get("query") or {}).get("pages") or {}
        
        for page_id, page_data in pages.items():
            if page_id == "-1":
                return None
            
            extract = page_data.get("extract")
            if extract:
                # 3. Save to Cache
                await async_write_text_file(cache_file, extract)
                return extract
            
    except (HTTPError, ValueError):
        return None

    return None


@dataclass
class WikipediaSection:
    """Represents a parsed section from a Wikipedia article."""
    name: str
    content: str


def parse_wikipedia_sections(
    raw_text: str,
    exclusion_headers: list[str],
    min_content_length: int = 50,
) -> Iterator[WikipediaSection]:
    """
    Parses raw Wikipedia article text into sections.

    Splits the article by MediaWiki section headers (== Header ==) and yields
    each section with its cleaned content. Stops parsing when an excluded
    section is encountered.

    Args:
        raw_text: Raw plain text from Wikipedia API (explaintext format).
        exclusion_headers: List of section headers to stop parsing at
            (e.g., ["References", "External links", "See also"]).
        min_content_length: Minimum character length for content to be included.

    Yields:
        WikipediaSection objects containing section name and content.
    """
    # Split by section headers (== Header ==, === Subheader ===, etc.)
    segments = re.split(r'(^={2,}[^=]+={2,}\s*$)', raw_text, flags=re.MULTILINE)

    current_section = "Introduction"

    for segment in segments:
        segment = segment.strip()
        if not segment:
            continue

        # Check if segment is a header
        if segment.startswith("==") and segment.endswith("=="):
            header_clean = segment.strip("=").strip()
            # Stop if we hit an excluded section
            if any(ex.lower() == header_clean.lower() for ex in exclusion_headers):
                return
            current_section = header_clean
        else:
            # Content segment
            if len(segment) >= min_content_length:
                yield WikipediaSection(name=current_section, content=segment)
