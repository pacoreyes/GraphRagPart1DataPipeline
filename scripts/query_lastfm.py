# -----------------------------------------------------------
# Query Last.fm Script
# Dagster Data pipeline for Structured and Unstructured Data
#
# (C) 2025-2026 Juan-Francisco Reyes, Cottbus, Germany
# Released under MIT License
# email pacoreyes@protonmail.com
# -----------------------------------------------------------

import json
import os
import httpx


def main() -> None:
    api_key = os.getenv("LASTFM_API_KEY")
    if not api_key:
        print("Error: LASTFM_API_KEY environment variable is not set.")
        print("Please set it in your .env file or export it in your shell.")
        return

    artist_name = "Kosuke Saito"
    api_url = "http://ws.audioscrobbler.com/2.0/"

    print(f"Querying LastFM for artist: {artist_name}")

    params = {
        "method": "artist.getInfo",
        "artist": artist_name,
        "api_key": api_key,
        "format": "json",
        "autocorrect": 1,
    }

    try:
        # We use httpx as it is available in the project environment
        response = httpx.get(api_url, params=params, timeout=30)
        response.raise_for_status()
        
        data = response.json()

        if "error" in data:
            print(f"\nLast.fm API returned an error: {data.get('message', 'Unknown error')} "
                  f"(Code: {data.get('error')})")
        else:
            print("\n--- Result ---")
            print(json.dumps(data, indent=2, ensure_ascii=False))

    except httpx.HTTPStatusError as e:
        print(f"\nHTTP error occurred: {e}")
    except httpx.RequestError as e:
        print(f"\nAn error occurred while requesting {e.request.url!r}: {e}")
    except json.JSONDecodeError:
        print("\nError: Failed to decode JSON response.")
    except Exception as e:
        print(f"\nAn unexpected error occurred: {e}")


if __name__ == "__main__":
    main()
