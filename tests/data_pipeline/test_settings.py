# -----------------------------------------------------------
# Unit Tests for settings
# Dagster Data pipeline for Structured and Unstructured Data
#
# (C) 2025-2026 Juan-Francisco Reyes, Cottbus, Germany
# Released under MIT License
# email pacoreyes@protonmail.com
# -----------------------------------------------------------

from pathlib import Path
from data_pipeline.settings import Settings

def test_settings_paths_resolve_correctly():
    """Test that core paths point to the expected locations."""
    settings = Settings()
    
    # APP_DIR should be the directory containing settings.py
    assert settings.APP_DIR.name == "data_pipeline"
    
    # PROJECT_DIR should be the root (two levels up from settings.py)
    # In this structure: root/src/data_pipeline/settings.py
    assert settings.PROJECT_DIR.joinpath("src").exists()
    assert settings.PROJECT_DIR.joinpath("pyproject.toml").exists()

def test_directory_auto_creation(tmp_path):
    """
    Test that directories are created. 
    We override DATA_DIR to use a temporary path for the test.
    """
    class TestSettings(Settings):
        DATA_DIR: Path = tmp_path / "data_volume"
        
        # Override env loading for tests
        model_config = {"env_file": None}

    test_settings = TestSettings()
    
    # Check if directories were created
    assert test_settings.wikipedia_cache_dirpath.exists()
    assert test_settings.wikidata_cache_dirpath.exists()
    assert test_settings.lastfm_cache_dirpath.exists()
    assert test_settings.temp_dirpath.exists()
    assert test_settings.datasets_dirpath.exists()
    assert test_settings.vector_db_dirpath.exists()

def test_default_values():
    settings = Settings()
    assert settings.WIKIDATA_SPARQL_BATCH_SIZE == 500
    assert "User-Agent" in settings.default_request_headers
    assert settings.DEFAULT_COLLECTION_NAME == "music_rag_collection"