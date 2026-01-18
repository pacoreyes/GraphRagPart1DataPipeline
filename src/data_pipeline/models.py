# -----------------------------------------------------------
# Data Models
# Dagster Data pipeline for Structured and Unstructured Data
#
# (C) 2025-2026 Juan-Francisco Reyes, Cottbus, Germany
# Released under MIT License
# email pacoreyes@protonmail.com
# -----------------------------------------------------------

from typing import Optional, get_args, get_origin, Any

import msgspec
import polars as pl


class Artist(msgspec.Struct, kw_only=True, omit_defaults=True):
    """
    Represents a musical artist or group extracted from Wikidata.
    """
    id: str
    name: str  # Indexed in graph database
    mbid: str
    country: str  # Drop after creating relation FROM_COUNTRY in graph database
    aliases: Optional[list[str]] = None  # Indexed in graph database
    genres: Optional[list[str]] = None  # Drop after creating relation PLAYS_GENRE in graph database
    tags: Optional[list[str]] = None  # Important for vector database
    similar_artists: Optional[list[str]] = None  # Drop after creating relation SIMILAR_TO in graph database


class Genre(msgspec.Struct, kw_only=True, omit_defaults=True):
    """
    Represents a musical genre.
    """
    id: str
    name: str
    aliases: Optional[list[str]] = None  # Indexed in graph database
    parent_ids: Optional[list[str]] = None  # Drop after creating relation SUBGENRE_OF in graph database


class Release(msgspec.Struct, kw_only=True, omit_defaults=True):
    """
    Represents a music release (Album or Single).
    """
    id: str
    title: str
    year: Optional[int] = None
    artist_id: str  # Drop after creating relation PERFORMED_BY in graph database


class Track(msgspec.Struct, kw_only=True, omit_defaults=True):
    """
    Represents a music track extracted from MusicBrainz.
    """
    id: str
    title: str
    album_id: str  # Drop after ingesting in graph (embedded as list in Release node, also indexed in graph database)


class Country(msgspec.Struct, kw_only=True, omit_defaults=True):
    """
    Represents a country entity resolved against Wikidata.
    """
    id: str  # Wikidata QID
    name: str


class ArticleMetadata(msgspec.Struct, kw_only=True, omit_defaults=True):
    """
    Metadata for a Wikipedia article chunk.
    """

    title: str
    artist_name: str
    aliases: Optional[list[str]] = None
    tags: Optional[list[str]] = None
    similar_artists: Optional[list[str]] = None
    genres: Optional[list[str]] = None
    inception_year: Optional[int] = None
    country: str
    wikipedia_url: str
    wikidata_uri: str
    chunk_index: int
    total_chunks: int


class Article(msgspec.Struct, kw_only=True, omit_defaults=True):
    """
    Represents a Wikipedia article or relevant text content for an artist,
    structured as a document with metadata and content.
    """
    id: str
    metadata: ArticleMetadata
    article: str


def _to_polars_dtype(py_type: Any) -> pl.DataType | type[pl.DataType]:
    """
    Maps a Python/msgspec type annotation to a Polars DataType.
    Handles Optional[...] and list[...] recursively.
    """
    origin = get_origin(py_type)
    args = get_args(py_type)

    # Handle Optional[T] -> T (Polars fields are nullable by default in schema dicts)
    if origin is Optional or (origin is type(None) or type(None) in args):
        # Extract the non-None type from Union[T, None]
        inner_args = [a for a in args if a is not type(None)]
        if len(inner_args) == 1:
            return _to_polars_dtype(inner_args[0])
        # If it's a Union of multiple types + None, we might default to Utf8 or Object,
        # but for this specific domain model, it's usually Optional[SimpleType].

    # Handle list[T] -> pl.List(T)
    if origin is list or origin is list:
        inner_type = args[0] if args else str
        return pl.List(_to_polars_dtype(inner_type))

    # Base mappings
    if py_type is str:
        return pl.Utf8
    if py_type is int:
        return pl.Int64
    if py_type is float:
        return pl.Float64
    if py_type is bool:
        return pl.Boolean

    # Fallback
    return pl.Utf8


def _generate_polars_schema(model_cls: type[msgspec.Struct]) -> dict[str, pl.DataType]:
    """
    Generates a Polars schema dictionary from a msgspec.Struct class.
    """
    schema = {}
    # msgspec structs store field info in __annotations__
    for field_name, field_type in model_cls.__annotations__.items():
        schema[field_name] = _to_polars_dtype(field_type)
    return schema


# --- Auto-generated Polars Schemas ---
RELEASE_SCHEMA = _generate_polars_schema(Release)
TRACK_SCHEMA = _generate_polars_schema(Track)
COUNTRY_SCHEMA = _generate_polars_schema(Country)
