# -----------------------------------------------------------
# SPARQL Queries
# Dagster Data pipeline for Structured and Unstructured Data
#
# (C) 2025-2026 Juan-Francisco Reyes, Cottbus, Germany
# Released under MIT License
# email pacoreyes@protonmail.com
# -----------------------------------------------------------

def get_tracks_by_albums_batch_query(album_qids: list[str]) -> str:
    """
    Builds a SPARQL query to fetch tracks for multiple albums in one request.
    Uses bidirectional logic: Album->Track (P658) OR Track->Album (P361).
    Implements explicit label fallback (English -> Any).

    Args:
        album_qids: List of Album Wikidata QIDs.

    Returns:
        A SPARQL query string.
    """
    values = " ".join([f"wd:{qid}" for qid in album_qids])
    return f"""
    SELECT DISTINCT ?album ?track ?trackLabel ?genre WHERE {{
      VALUES ?album {{ {values} }}
      {{ ?album wdt:P658 ?track. }}  # Forward: Album has tracklist containing track
      UNION
      {{ ?track wdt:P361 ?album. }}  # Reverse: Track is part of album
      
      OPTIONAL {{ ?track wdt:P136 ?genre. }}

      # Label Fallback: English -> Any
      OPTIONAL {{ ?track rdfs:label ?enLabel . FILTER(LANG(?enLabel) = "en") }}
      OPTIONAL {{ ?track rdfs:label ?anyLabel . }}
      BIND(COALESCE(?enLabel, ?anyLabel) AS ?trackLabel)
    }}
    """


def get_albums_by_artists_batch_query(artist_qids: list[str]) -> str:
    """
    Builds a SPARQL query to fetch albums for multiple artists in one request.

    Args:
        artist_qids: List of Wikidata QIDs.

    Returns:
        A SPARQL query string.
    """
    values = " ".join([f"wd:{qid}" for qid in artist_qids])
    return f"""
    SELECT ?album ?albumLabel ?releaseDate ?artist ?genre WHERE {{
      VALUES ?artist {{ {values} }}
      ?album wdt:P175 ?artist.
      
      # Exclude non-standard releases
      FILTER NOT EXISTS {{ ?album wdt:P31 wd:Q134556. }}   # exclude singles
      FILTER NOT EXISTS {{ ?album wdt:P7937 wd:Q222910. }}  # exclude compilations
      FILTER NOT EXISTS {{ ?album wdt:P7937 wd:Q209939. }}  # exclude live albums
      FILTER NOT EXISTS {{ ?album wdt:P31 wd:Q10590726. }}  # exclude video albums
      
      OPTIONAL {{ ?album wdt:P577 ?releaseDate. }}
      OPTIONAL {{ ?album wdt:P136 ?genre. }}
      
      # Label Fallback: English -> Any
      OPTIONAL {{ ?album rdfs:label ?enLabel . FILTER(LANG(?enLabel) = "en") }}
      OPTIONAL {{ ?album rdfs:label ?anyLabel . }}
      BIND(COALESCE(?enLabel, ?anyLabel) AS ?albumLabel)
    }}
    """


def get_genre_parents_batch_query(genre_qids: list[str]) -> str:
    """
    Builds a SPARQL query to fetch the parent genres (subclass of P279) 
    for a list of genre QIDs.

    Args:
        genre_qids: List of Genre Wikidata QIDs.

    Returns:
        A SPARQL query string.
    """
    values = " ".join([f"wd:{qid}" for qid in genre_qids])
    return f"""
    SELECT DISTINCT ?genre ?parent WHERE {{
      VALUES ?genre {{ {values} }}
      ?genre wdt:P279 ?parent.
    }}
    """


def get_artists_by_year_range_query(
    start_year: int, end_year: int, limit: int, offset: int
) -> str:
    """
    Generate a SPARQL query to fetch artists active within a specific year range.

    Args:
        start_year: The first year of the period (inclusive).
        end_year: The last year of the period (inclusive).
        limit: The maximum number of results to return.
        offset: The offset from which to start fetching results.

    Returns:
        A formatted SPARQL query string.
    """
    return f"""
SELECT ?artist ?artistLabel ?start_date
WHERE {{
  # --- INNER QUERY: Find the artists first (Pagination happens here) ---
  {{
    SELECT DISTINCT ?artist ?start_date
    
    # 1. Genre: Electronic & Subgenres, 'Genre' (P136) AND 'Field of Work' (P101)
    WHERE {{
      # 1. Genre: Electronic & Subgenres
      ?genre wdt:P279* wd:Q9778 .
      ?artist wdt:P136|wdt:P101 ?genre .

      # 2. Type: Human or Group
      {{ ?artist wdt:P31 wd:Q5 . }} UNION {{ ?artist wdt:P31/wdt:P279* wd:Q215380 . }}

      # 3. Date Filter
      ?artist wdt:P571|wdt:P2031 ?start_date .
      FILTER (YEAR(?start_date) >= {start_year} && YEAR(?start_date) <= {end_year})
    }}
    # Pagination applies only to the ID retrieval (Fast)
    ORDER BY ?start_date ?artist
    LIMIT {limit}
    OFFSET {offset}
  }}

  # --- OUTER QUERY: Fetch Labels for the 100 results ---
  
  # A. Try to find an English label specifically
  OPTIONAL {{ ?artist rdfs:label ?enLabel . FILTER(LANG(?enLabel) = "en") }}

  # B. Find ANY other label (Language is irrelevant)
  OPTIONAL {{ ?artist rdfs:label ?anyLabel . }}

  # C. Logic: "If English exists, use it. Otherwise, use the random fallback."
  BIND(COALESCE(?enLabel, ?anyLabel) AS ?artistLabel)
}}
# We group by the ID to merge the multiple labels into one line
GROUP BY ?artist ?artistLabel ?start_date
ORDER BY ?start_date
"""
