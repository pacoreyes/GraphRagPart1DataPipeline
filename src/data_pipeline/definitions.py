# -----------------------------------------------------------
# Dagster Definitions
# Dagster Data pipeline for Structured and Unstructured Data
#
# (C) 2025-2026 Juan-Francisco Reyes, Cottbus, Germany
# Released under MIT License
# email pacoreyes@protonmail.com
# -----------------------------------------------------------

from dagster import (
    Definitions,
    load_assets_from_package_module,
    load_asset_checks_from_modules
)

from data_pipeline.defs import assets, resources, checks

# 1. Load Assets
# This scans the 'data_pipeline.defs.assets' package for @asset decorated functions
all_assets = load_assets_from_package_module(assets)

# 2. Load Asset Checks
# This scans the 'checks' module
all_checks = load_asset_checks_from_modules([checks])

# 3. Construct Definitions
# Merging everything explicitly
defs = Definitions(
    assets=all_assets,
    asset_checks=all_checks,
    resources=resources.resource_defs,
)
