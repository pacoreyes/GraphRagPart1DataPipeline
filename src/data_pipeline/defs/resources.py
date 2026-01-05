# -----------------------------------------------------------
# Dagster Resources
# Dagster Data pipeline for Structured and Unstructured Data
#
# (C) 2025-2026 Juan-Francisco Reyes, Cottbus, Germany
# Released under MIT License
# email pacoreyes@protonmail.com
# -----------------------------------------------------------

import os
from typing import Any

from dagster import (
    ConfigurableResource,
    Definitions,
    EnvVar,
    FilesystemIOManager,
)
from dagster_gcp import GCSPickleIOManager, GCSResource

from data_pipeline.settings import settings


class ApiConfiguration(ConfigurableResource):
    """
    Configuration for external API keys.
    """
    lastfm_api_key: str
    nomic_api_key: str


# Environment Logic
is_prod = os.getenv("DAGSTER_ENV") == "PROD"

# Base resources
resource_defs: dict[str, Any] = {
    "api_config": ApiConfiguration(
        lastfm_api_key=EnvVar("LASTFM_API_KEY"),
        nomic_api_key=EnvVar("NOMIC_API_KEY"),
    )
}

if is_prod:
    # PROD: run on Docker, save to GCS
    resource_defs["io_manager"] = GCSPickleIOManager(
        gcs_bucket="graphrag_data_lakehouse",
        gcs_prefix="dagster_data",
        gcs=GCSResource()
    )
else:
    # DEV: Save to Local Disk
    resource_defs["io_manager"] = FilesystemIOManager(
        base_dir=str(settings.temp_dirpath)
    )

# Export as Definitions for automatic loading
defs = Definitions(
    resources=resource_defs,
)
