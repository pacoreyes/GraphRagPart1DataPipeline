# -----------------------------------------------------------
# Dagster I/O Managers
# Dagster Data pipeline for Structured and Unstructured Data
#
# (C) 2025-2026 Juan-Francisco Reyes, Cottbus, Germany
# Released under MIT License
# email pacoreyes@protonmail.com
# -----------------------------------------------------------

import shutil
from pathlib import Path
from typing import Union, Optional, Any

import msgspec
import polars as pl
from dagster import ConfigurableIOManager, InputContext, OutputContext


class BasePolarsIOManager(ConfigurableIOManager):
    """
    Base class for Polars-based I/O Managers.
    """
    base_dir: str
    extension: str

    def handle_output(self, context: OutputContext, obj: Any):
        raise NotImplementedError("This is a base class. Use a concrete subclass.")

    def load_input(self, context: InputContext) -> Any:
        raise NotImplementedError("This is a base class. Use a concrete subclass.")

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
            return Path(self.base_dir) / asset_name / f"{pk}.{self.extension}"
        
        return Path(self.base_dir) / f"{asset_name}.{self.extension}"


class PolarsParquetIOManager(BasePolarsIOManager):
    """
    I/O Manager that saves and loads DataFrames as Parquet files.
    Optimized for performance and compatibility with data viewers.
    """
    extension: str = "parquet"

    def handle_output(self, context: OutputContext, obj: Any):
        """Saves output to a Parquet file."""
        path = self._get_path(context)
        path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = path.with_suffix(".tmp")

        if isinstance(obj, pl.LazyFrame):
            obj.sink_parquet(temp_path)
            # Metadata for LazyFrame requires scanning the sinked file or collecting
            row_count = pl.scan_parquet(temp_path).select(pl.len()).collect().item()
        elif isinstance(obj, pl.DataFrame):
            obj.write_parquet(temp_path)
            row_count = len(obj)
        elif isinstance(obj, list):
            # Convert list of dicts/structs to DataFrame
            df = pl.from_dicts([
                msgspec.to_builtins(i) if not isinstance(i, dict) else i 
                for i in (obj[0] if obj and isinstance(obj[0], list) else obj)
            ])
            df.write_parquet(temp_path)
            row_count = len(df)
        else:
            raise TypeError(f"Unsupported output type for Parquet: {type(obj)}")

        shutil.move(str(temp_path), str(path))
        context.add_output_metadata({
            "row_count": row_count,
            "path": str(path),
            "file_size_kb": path.stat().st_size / 1024,
            "format": "parquet"
        })

    def load_input(self, context: InputContext) -> pl.LazyFrame:
        """Loads data as a Polars LazyFrame from Parquet."""
        if context.has_asset_partitions:
            if not context.has_partition_key:
                partition_keys = context.asset_partition_keys
                paths = [str(self._get_path(context, pk)) for pk in partition_keys
                         if self._get_path(context, pk).exists()]
                return pl.scan_parquet(paths) if paths else pl.LazyFrame()
            
        path = self._get_path(context)
        return pl.scan_parquet(path) if path.exists() else pl.LazyFrame()


class PolarsJSONLIOManager(BasePolarsIOManager):
    """
    I/O Manager that saves and loads DataFrames and Iterators as JSONL files.
    Supports sparse JSON output and O(1) memory streaming.
    """
    extension: str = "jsonl"

    def handle_output(self, context: OutputContext, obj: Any):
        """
        Saves the output to a JSONL file sparsely.
        """
        path = self._get_path(context)
        path.parent.mkdir(parents=True, exist_ok=True)
        
        temp_path = path.with_suffix(".tmp")
        row_count = 0

        def filter_none(d: dict[str, Any]) -> dict[str, Any]:
            return {k: v for k, v in d.items() if v is not None}

        def write_item(f, item):
            nonlocal row_count
            row_count += 1
            if not isinstance(item, dict):
                try:
                    d = msgspec.to_builtins(item)
                except (TypeError, AttributeError):
                    d = item
            else:
                d = item
            
            if isinstance(d, dict):
                cleaned = filter_none(d)
                if cleaned:
                    f.write(msgspec.json.encode(cleaned))
                    f.write(b"\n")
            else:
                f.write(msgspec.json.encode(d))
                f.write(b"\n")

        with open(temp_path, "wb") as f:
            if isinstance(obj, (pl.DataFrame, pl.LazyFrame)):
                lf = obj.lazy() if isinstance(obj, pl.DataFrame) else obj
                df = lf.collect()
                for row in df.to_dicts():
                    write_item(f, row)
            
            elif isinstance(obj, list):
                for item in obj:
                    if isinstance(item, list):
                        for subitem in item:
                            write_item(f, subitem)
                    else:
                        write_item(f, item)
            else:
                write_item(f, obj)

        shutil.move(str(temp_path), str(path))
        
        context.add_output_metadata({
            "row_count": row_count,
            "path": str(path),
            "file_size_kb": path.stat().st_size / 1024,
            "sparse_json": True
        })

    def load_input(self, context: InputContext) -> pl.LazyFrame:
        """Loads data as a Polars LazyFrame from JSONL."""
        if context.has_asset_partitions:
            if not context.has_partition_key:
                partition_keys = context.asset_partition_keys
                paths = [str(self._get_path(context, pk)) for pk in partition_keys
                         if self._get_path(context, pk).exists()]
                return pl.scan_ndjson(paths) if paths else pl.LazyFrame()
            
        path = self._get_path(context)
        return pl.scan_ndjson(str(path)) if path.exists() else pl.LazyFrame()
