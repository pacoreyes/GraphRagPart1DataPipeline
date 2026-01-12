# -----------------------------------------------------------
# Dagster I/O Managers
# Dagster Data pipeline for Structured and Unstructured Data
#
# (C) 2025-2026 Juan-Francisco Reyes, Cottbus, Germany
# Released under MIT License
# email pacoreyes@protonmail.com
# -----------------------------------------------------------

from pathlib import Path
from typing import Union, Optional

import msgspec
import polars as pl
from dagster import ConfigurableIOManager, InputContext, OutputContext


class PolarsJSONLIOManager(ConfigurableIOManager):
    """
    I/O Manager that saves and loads Polars DataFrames as JSONL files.
    Supports sparse JSON output by omitting null properties.
    """
    base_dir: str

    def _get_path(self, context: Union[InputContext, OutputContext], partition_key: Optional[str] = None) -> Path:
        """Determines the file path based on the asset key and partition."""
        asset_name = context.asset_key.path[-1]
        
        pk = partition_key
        if not pk:
            if isinstance(context, OutputContext):
                if context.has_partition_key:
                    pk = context.partition_key
            elif isinstance(context, InputContext):
                if context.has_asset_partitions and context.has_partition_key:
                    pk = context.asset_partition_key
        
        if pk:
            return Path(self.base_dir) / asset_name / f"{pk}.jsonl"
        
        return Path(self.base_dir) / f"{asset_name}.jsonl"

    def handle_output(self, context: OutputContext, obj: Union[pl.DataFrame, pl.LazyFrame]):
        """Saves a Polars DataFrame or LazyFrame to a JSONL file, omitting null values."""
        if isinstance(obj, pl.LazyFrame):
            obj = obj.collect()
        
        if not isinstance(obj, pl.DataFrame):
            raise TypeError(f"Expected pl.DataFrame or pl.LazyFrame, got {type(obj)}")

        path = self._get_path(context)
        path.parent.mkdir(parents=True, exist_ok=True)
        
        # Restore Sparse JSON: Convert to dicts and remove None values
        # Polars write_ndjson includes nulls; we must manualy filter to satisfy the requirement
        dicts = obj.to_dicts()
        
        with open(path, "wb") as f:
            for d in dicts:
                # Remove keys with None values only.
                cleaned_d = {k: v for k, v in d.items() if v is not None}
                
                if cleaned_d:
                    f.write(msgspec.json.encode(cleaned_d))
                    f.write(b"\n")
        
        context.add_output_metadata({
            "row_count": len(obj),
            "path": str(path),
            "file_size_kb": path.stat().st_size / 1024,
            "sparse_json": True
        })

    def load_input(self, context: InputContext) -> Union[pl.LazyFrame, dict[str, pl.LazyFrame]]:
        """Loads Polars DataFrame(s) lazily. Returns a single LazyFrame or a dict for Fan-In."""
        if context.has_asset_partitions:
            if not context.has_partition_key:
                partition_keys = context.asset_partition_keys
                paths = []
                results = {}
                for pk in partition_keys:
                    path = self._get_path(context, partition_key=pk)
                    if path.exists():
                        paths.append(str(path))
                        results[pk] = pl.scan_ndjson(str(path))
                
                # Return a single combined LazyFrame for Fan-In
                if paths:
                    return pl.scan_ndjson(paths)
                return pl.LazyFrame()
            
        path = self._get_path(context)
        if not path.exists():
            return pl.LazyFrame()

        return pl.scan_ndjson(str(path))
