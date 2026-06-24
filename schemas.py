from abc import ABC, abstractmethod
from typing import ClassVar, Any
from datetime import datetime

from pydantic import (
    BaseModel, 
    ConfigDict,
    field_validator,
    model_serializer,
    model_validator
)

from pyspark.sql import DataFrame, Column

# =============== Shared settings for all GraphQL Queries ==============
class GraphQLQuery(BaseModel):
    """Base class for GraphQL queries.

    Attributes:
        QUERY (str): The GraphQL query string.

    Methods:
        serialize: Format .model_dump() for the FFLogs GraphQL requests.

    Example:
        >>> class MyQuery(GraphQLQuery):
        ...     QUERY = "query { user(id: $id) { name } }"
        ...     id: int
        >>> q = MyQuery(id=123)
        >>> q.serialize()
        {'query': 'query { user(id: $id) { name } }', 'variables': {'id': 123}}
    """
    
    QUERY: ClassVar[str] 

    model_config = ConfigDict(
        frozen=True,
        extra="forbid",
        strict=True,                    
        validate_default=True,          
        populate_by_name=True,  
    )

    @model_validator(mode="before")
    @classmethod
    def normalize_time_fields(cls, data: Any) -> Any:
        """Normalizes any field ending in `_time` to a UNIX timestamp in milliseconds. 
        
        Accepts either a numeric UNIX ms timestamp (passed through
        unchanged) or a local-time string in 'YYYY-MM-DD HH:MM:SS' format,
        converted using the system's local timezone.
        """
        for key, value in list(data.items()):
            if not key.endswith("_time"):
                continue

            if isinstance(value, str):
                try:
                    dt = datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
                except ValueError as e:
                    raise ValueError(
                        f"Invalid datetime string '{value}'. "
                        "Expected format: 'YYYY-MM-DD HH:MM:SS'."
                    ) from e
                data[key] = dt.timestamp() * 1000
            elif isinstance(value, (int, float)):
                data[key] = float(value)

        return data

    @model_serializer
    def serialize(self) -> dict:
        return {
            "query": self.QUERY,
            "variables": {
                field: getattr(self, field)
                for field in self.model_fields
                if getattr(self, field) is not None
            },
        }

# =============== Shared settings for all Bronze, Silver, and Gold shaped tables ==============
class VolumeContent(ABC):
    volume_dir: str
    source: str
    content: dict | str | None

    def __init__(self, volume_dir: str, source: str) -> None:
        self.volume_dir = volume_dir
        self.source = source
        self.content = None

    @abstractmethod
    def read(self) -> "VolumeContent": ...

    @abstractmethod
    def preview(self) -> dict | str | None: ...


class BronzeTable(ABC):
    """
    Abstract base class for Bronze tables.

    The user must call methods in the following order:
    ingest > 
    attach_metadata (optional)

    Example:
        >>> class FFLogsEncounterBronze(BronzeTable):
        ...     def ingest(self): ...
        >>> bronze = FFLogsEncounterBronze(raw)
        >>> bronze = bronze.ingest().attach_metadata()
    """
    raw: VolumeContent
    df: DataFrame | None

    def __init__(self, raw: VolumeContent) -> None:
        self.raw = raw
        self.df = None

    @abstractmethod
    def ingest(self) -> "BronzeTable": ...

    def attach_metadata(self) -> "BronzeTable":
        """Override to attach pipeline metadata (e.g. ldts, rsrc)."""
        return self


class SilverTable(ABC):
    """
    Abstract base class for Silver tables.

    The user must call methods in the following order:
    unpack (optional) > 
    define > 
    clean > 
    hash (optional) > 
    expect (optional) > 
    deduplicate (optional)

    Example:
        >>> class FFLogsEncounterSilver(SilverTable):
        ...     def define(self): ...
        ...     def clean(self): ...
        >>> silver = FFLogsEncounterSilver(bronze)
        >>> silver = silver.unpack().define().clean().expect().hash().deduplicate()
    """
    base_df: DataFrame
    df: DataFrame | None

    def __init__(self, bronze: BronzeTable) -> None:
        assert bronze.df is not None

        self.base_df = bronze.df
        self.df = None

    @abstractmethod
    def define(self) -> "SilverTable": ...

    @abstractmethod
    def clean(self) -> "SilverTable": ...

    def unpack(self) -> "SilverTable":
        """Override to explode nested structures if needed."""
        return self

    def hash(self) -> "SilverTable":
        """Override to add a surrogate key if required."""
        return self

    def expect(self) -> "SilverTable":
        """Override to define data quality filters."""
        return self

    def deduplicate(self) -> "SilverTable":
        """Override to deduplicate records if needed."""
        return self


class GoldTable(ABC):
    """
    Abstract base class for Gold tables.

    The user must call methods in the following order:
    define

    Example:
        >>> class FFLogsEncounterGold(GoldTable):
        ...     def define(self): ...
        >>> gold = FFLogsEncounterGold(silver)
        >>> gold = gold.define()
    """
    base_df: DataFrame
    df: DataFrame | None

    def __init__(self, silver: SilverTable) -> None:
        assert silver.df is not None 
        
        self.base_df = silver.df
        self.df = None

    @abstractmethod
    def define(self) -> "GoldTable": ...


class AnalysisQueryParameters(BaseModel):
    """Custom base class for an analysis query and its required parameters."""

    model_config = ConfigDict(
        frozen=True
    )