# -----------------------------------------------------------
# Data Models
# Dagster Data pipeline for Structured and Unstructured Data
#
# (C) 2025-2026 Juan-Francisco Reyes, Cottbus, Germany
# Released under MIT License
# email pacoreyes@protonmail.com
# -----------------------------------------------------------

from typing import Optional

import msgspec


class Artist(msgspec.Struct, kw_only=True, omit_defaults=True):
    """
    Represents a musical artist or group extracted from Wikidata.
    """
    id: str
    name: str
    aliases: Optional[list[str]] = None
    country: Optional[str] = None
    genres: Optional[list[str]] = None  # List of QIDs
    tags: Optional[list[str]] = None
    similar_artists: Optional[list[str]] = None


class Genre(msgspec.Struct, kw_only=True, omit_defaults=True):
    """
    Represents a musical genre.
    """
    id: str
    name: str
    aliases: Optional[list[str]] = None
    parent_ids: Optional[list[str]] = None


class Album(msgspec.Struct, kw_only=True, omit_defaults=True):
    """
    Represents a music album.
    """
    id: str
    title: str
    year: Optional[int] = None
    artist_id: str
    genres: Optional[list[str]] = None


class Track(msgspec.Struct, kw_only=True, omit_defaults=True):
    """
    Represents a music track.
    """
    id: str
    title: str
    album_id: str
    genres: Optional[list[str]] = None


class ArticleMetadata(msgspec.Struct, kw_only=True, omit_defaults=True):
    """
    Metadata for a Wikipedia article chunk.
    """

    title: str
    artist_name: str
    genres: Optional[list[str]] = None
    inception_year: Optional[int] = None
    wikipedia_url: str
    wikidata_uri: str
    chunk_index: int
    total_chunks: int


class Article(msgspec.Struct, kw_only=True, omit_defaults=True):
    """
    Represents a Wikipedia article or relevant text content for an artist,
    structured as a document with metadata and content.
    """
    metadata: ArticleMetadata
    article: str
