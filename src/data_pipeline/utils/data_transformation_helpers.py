# -----------------------------------------------------------
# Data Transformation Helpers
# Dagster Data pipeline for Structured and Unstructured Data
#
# (C) 2025-2026 Juan-Francisco Reyes, Cottbus, Germany
# Released under MIT License
# email pacoreyes@protonmail.com
# -----------------------------------------------------------

import re
import unicodedata
from typing import Union, Optional, overload, Sequence, Any

import ftfy
import polars as pl


@overload
def normalize_and_clean_text(text_or_expr: str) -> str: ...


@overload
def normalize_and_clean_text(text_or_expr: pl.Expr) -> pl.Expr: ...


def normalize_and_clean_text(text_or_expr: Union[str, pl.Expr]) -> Union[str, pl.Expr]:
    """
    Unified robust text cleaning function.
    
    Pipeline:
    1. Repair encoding (ftfy)
    2. Normalize Unicode (NFKC)
    3. Sanitize (Regex: fix quotes, newlines, whitespace)
    """
    
    # --- Python String Implementation ---
    if isinstance(text_or_expr, str):
        text = text_or_expr
        # 1. Repair (Mojibake)
        text = ftfy.fix_text(text)
        # 2. Normalize (Unicode Canonical)
        text = unicodedata.normalize("NFKC", text)
        # 3. Sanitize (Regex)
        text = text.replace('\\"', '"')
        text = re.sub(r"[\n\r]+", " ", text)
        text = re.sub(r"\s+", " ", text)
        return text.strip()

    # --- Polars Expression Implementation ---
    elif isinstance(text_or_expr, pl.Expr):
        expr = text_or_expr
        
        # 1 & 2. Repair & Normalize
        # We must use a Python UDF here as Polars lacks native ftfy/NFKC support.
        # return_dtype=pl.String is important for schema inference.
        def _repair_and_normalize(val: Optional[str]) -> Optional[str]:
            if val is None:
                return None
            val = ftfy.fix_text(val)
            return unicodedata.normalize("NFKC", val)

        expr = expr.map_elements(_repair_and_normalize, return_dtype=pl.String)
        
        # 3. Sanitize (Native Polars Regex for speed)
        expr = (
            expr
            .str.replace_all(r'\\"', '"')
            .str.replace_all(r"[\n\r]+", " ")
            .str.replace_all(r"\s+", " ")
            .str.strip_chars()
        )
        return expr
        
    else:
        raise TypeError(f"Expected str or pl.Expr, got {type(text_or_expr)}")


def deduplicate_by_priority(
    df: pl.DataFrame | pl.LazyFrame,
    sort_col: str,
    unique_cols: list[str],
    descending: bool = False,
) -> pl.LazyFrame:
    """
    Deduplicates a DataFrame/LazyFrame based on priority (sort order).
    
    Sorts the frame by `sort_col`, then iteratively applies unique constraints
    on `unique_cols` keeping the first occurrence (highest priority).
    """
    if isinstance(df, pl.DataFrame):
        lf = df.lazy()
    else:
        lf = df

    lf = lf.sort(sort_col, descending=descending)

    for col in unique_cols:
        lf = lf.unique(subset=[col], keep="first", maintain_order=True)

    return lf


def format_list_natural_language(items: Optional[Sequence[Any]]) -> str:
    """
    Formats a list of strings into a natural language string with Oxford comma.
    e.g. ['A', 'B', 'C'] -> "A, B, and C"
         ['A', 'B'] -> "A and B"
         ['A'] -> "A"
    """
    if not items:
        return ""
    
    # Filter empty/None and unique-ify while preserving order
    clean_items = []
    seen = set()
    for x in items:
        if x and x not in seen:
            clean_items.append(str(x))
            seen.add(x)
            
    if not clean_items:
        return ""
        
    if len(clean_items) == 1:
        return clean_items[0]
        
    if len(clean_items) == 2:
        return f"{clean_items[0]} and {clean_items[1]}"
        
    return f"{', '.join(clean_items[:-1])}, and {clean_items[-1]}"
