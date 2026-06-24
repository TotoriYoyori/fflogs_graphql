import sys
sys.path.append("/Workspace/Users/stan.mng@gmail.com/fflogs_graphql")

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql import DataFrame

from settings import fflogs_settings as settings


# =============== STREAMING TABLE: fflogs_guilds_gold
@dp.table(
    name=f"{settings.gold_schema}.fflogs_guilds_gold",
    comment="Analytic-ready, denormalized streaming table containing guild ranking data."
)
@dp.expect_or_drop("valid_guild_name", "guild_name IS NOT NULL")
@dp.expect_or_drop("valid_server_id", "server_id IS NOT NULL")
@dp.expect_or_drop("valid_region_id", "region_id IS NOT NULL")
def fflogs_guilds_gold() -> DataFrame:
    return (
        spark.readStream.table(
            f"{settings.silver_schema}.fflogs_guilds_silver"
        )
        .select(
            F.col("guild_id"),
            F.col("guild_name"),
            F.col("guild_description"),
            F.col("server_id"),
            F.col("region_id"),
            F.col("world_prog_rank"),
            F.col("region_prog_rank"),
            F.col("world_speed_rank"),
            F.col("region_speed_rank"),
        )
    )