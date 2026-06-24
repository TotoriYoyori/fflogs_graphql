import IPython
from typing import ClassVar

from pydantic import (
    BaseModel,
    ConfigDict,
    model_validator
)

# ================ PRE-SETTINGS ===============
dbutils = IPython.get_ipython().user_ns["dbutils"]

# =============== SINGLE SOURCE OF TRUTH FOR ALL FFLOGS_GRAPHQL RELATED VARIABLES ===============
class FFLogsGraphQLSettings(BaseModel):
    SECRET_SCOPE: ClassVar[str] = "fflogs_graphql"
    
    fflogs_client_id: str = ""
    fflogs_client_secret: str = ""

    encounter_ranking_volume: str = "/Volumes/fflogs_graphql/bronze/encounter_ranking"
    guilds_volume: str = "/Volumes/fflogs_graphql/bronze/guilds"
    volume_dir: str = encounter_ranking_volume  # Backward compatibility
    
    bronze_schema: str = "fflogs_graphql.bronze"
    silver_schema: str = "fflogs_graphql.silver"
    gold_schema: str = "fflogs_graphql.gold"


    model_config = ConfigDict(frozen=True)

    @model_validator(mode="before")
    @classmethod
    def fetch_from_secrets_manager(cls, data) -> dict[str, str]:
        return {
            field: dbutils.secrets.get(cls.SECRET_SCOPE, field)
            for field in cls.model_fields
            if field in ["fflogs_client_id", "fflogs_client_secret"]
        }

    @property
    def fflogs_access_token(self) -> str:
        """Get fresh access token from secrets (always fresh even after refresh)"""
        return dbutils.secrets.get(self.SECRET_SCOPE, "fflogs_access_token")

fflogs_settings = FFLogsGraphQLSettings()
