import sys
sys.path.append("/Workspace/Users/stan.mng@gmail.com/fflogs_graphql/jobs")

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql import DataFrame

from settings import fflogs_settings as settings


# =============== STREAMING TABLE: fflogs_encounter_ranking_gold
@dp.table(
    name=f"{settings.gold_schema}.fflogs_encounter_ranking_gold",
    comment="Analytic-ready, denormalized streaming table containing encounter ranking data."
)
@dp.expect_or_drop("valid_encounter_name", "encounter_name IS NOT NULL")
@dp.expect_or_drop("valid_player_name", "player_name IS NOT NULL")
@dp.expect_or_drop("valid_ranked_date", "ranked_date IS NOT NULL")
def fflogs_encounter_ranking_gold() -> DataFrame:
    return (
        spark.readStream.table(
            f"{settings.silver_schema}.fflogs_encounter_ranking_silver"
        )
        .select(
            F.col("encounter_name"),
            F.col("player_name"),
            F.col("class"),
            F.col("rDPS"),
            F.col("aDPS"),
            F.date_format(F.col("ranked_date"), "yyyy-MM-dd")
                .alias("ranked_date"),
            F.col("duration"),
            F.col("lodestoneID"),
            F.col("guild_name"),
            F.col("server_name"),
        )
    )


# =============== VIEW: fflogs_encounter_ranking_presentation
@dp.view(
    name="fflogs_encounter_ranking_presentation",
    comment="Presentation view of encounter rankings. Not meant for analytic use; data science flows should use the gold table. This view is intended for non-technical audiences or quick glance."
)
def fflogs_encounter_ranking_presentation() -> DataFrame:
    return (
        spark.read.table(
            f"{settings.gold_schema}.fflogs_encounter_ranking_gold"
        )
        # ===== Derive MM/SS components then format
        .withColumn("MM", 
            F.lpad(F.floor(F.col("duration") / 60)
                .cast("string"), 2, "0")
        )
        .withColumn("SS", 
            F.lpad((F.col("duration") % 60)
                .cast("string"), 2, "0")
        )
        .withColumn("duration_fmt", 
            F.concat(F.col("MM"), F.lit(":"), F.col("SS"))
        )
        .select(
            "encounter_name",
            "player_name",
            "class",
            "rDPS",
            "aDPS",
            "ranked_date",
            "duration_fmt",
            "guild_name",
            "server_name",
        )
    )
    