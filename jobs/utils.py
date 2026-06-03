from enum import Enum

import pyspark.sql.functions as F
from pyspark.sql.functions import Column
# (
#     round, regexp_replace, trim, col, lower, upper, 
#     initcap, split, concat_ws, to_timestamp
# )

# =============== HELPER OBJECTS ===============
CASE_RULES = {
    "pascal": (r"([a-z])([A-Z])", r"$1 $2"),
    "snake": (r"_+", " "),
    "kebab": (r"-+", " "),
}

# =============== FUNCTIONS ===============
def to_title_case(col: Column, case: str) -> Column:
    """Convert string case format to title case.

    Example:
        >>> from pyspark.sql.functions import col, initcap
        >>> df = df.select(
        ...     col("ldts"),
        ...     col("rsrc"),
        ...     to_title_case(col("class"), "snake").alias("class")
        ... )
    """
    case = case.lower().strip()
    if case not in CASE_RULES:
        raise ValueError(
            f"Unsupported case '{case}'. "
            f"Supported: {list(CASE_RULES.keys())}"
        )

    pattern, replacement = CASE_RULES[case]
    return F.initcap(
        F.regexp_replace(col, pattern, replacement)
    )


def trim_whitespace(col: Column) -> Column:
    """Clean whitespace and normalize string formatting.
    Example:
        >>> from pyspark.sql.functions import col
        >>> df = df.select(
        ...     trim_whitespace(col("raw_text")).alias("clean_text")
        ... )
    """
    return F.trim(
        F.regexp_replace(
            F.regexp_replace(col, r"[\r\n]", " "), 
            r"\s+", 
            " "
        )
    )


def roundfloat(col: Column, decimals: int = 2) -> Column:
    """Spark's .round() and .cast("float")"""
    return F.round(col, decimals).cast("float")


def epoch_to_timestamp(col: Column, unit: str = "s") -> Column:
    """Spark's to_timestamp with added supports for different units of seconds."""
    unit = unit.lower().strip()
    factor_map = {
        "s": 1,
        "ms": 1_000,
        "us": 1_000_000,
    }

    if unit not in factor_map:
        raise ValueError(f"Unsupported unit '{unit}'")

    return F.to_timestamp(
        F.from_unixtime(col / factor_map[unit])
    )