import sys
sys.path.append("/Workspace/Users/stan.mng@gmail.com/fflogs_graphql/jobs")

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import DataFrame

from settings import fflogs_settings as settings
from utils import (
    roundfloat,
    to_title_case,
    trim_whitespace,
    epoch_to_timestamp
)

# ================ JSON DATA SCHEMA
worldDataSchema = T.StructType([
    T.StructField("encounter", T.StructType([
        T.StructField("id", T.IntegerType()),
        T.StructField("name", T.StringType()),
        T.StructField("characterRankings", T.StructType([
            T.StructField("rankings", T.ArrayType(T.StructType([
                T.StructField("name", T.StringType()),
                T.StructField("spec", T.StringType()),
                T.StructField("amount", T.FloatType()),
                T.StructField("aDPS", T.FloatType()),
                T.StructField("startTime", T.LongType()),
                T.StructField("duration", T.IntegerType()),
                T.StructField("lodestoneID", T.LongType()),
                T.StructField("guild", T.StructType([
                    T.StructField("id", T.LongType()),
                    T.StructField("name", T.StringType())
                ])),
                T.StructField("server", T.StructType([
                    T.StructField("id", T.IntegerType()),
                    T.StructField("name", T.StringType())
                ]))
            ])))
        ]))
    ]))
])

# ================ STREAMING TABLE: fflogs_encounter_ranking_silver_clean
@dp.table(
    name=f"{settings.silver_schema}._fflogs_encounter_ranking_silver_c",
    comment="Streaming table containing cleaned and structured encounter ranking data with exploded player rankings. Not yet deduplicated."
)
@dp.expect_or_drop("valid_encounter_id", "encounter_id IS NOT NULL")
@dp.expect_or_drop("valid_player_name", "player_name IS NOT NULL")
@dp.expect_or_drop("positive_rdps", "rDPS > 0")
@dp.expect_or_drop("positive_adps", "aDPS > 0")
@dp.expect_or_drop("valid_duration", "duration > 0")
def _fflogs_encounter_ranking_silver_c() -> DataFrame:
    df = (spark.readStream.table(
            f"{settings.bronze_schema}.fflogs_encounters_ranking_raw"
        )
        # ===== Parse JSON string to struct
        .withColumn("worldData_struct", F.from_json(F.col("worldData"), worldDataSchema))
        # ===== Unpack nested struct
        .select(
            "ldts",
            "rsrc",
            F.col("worldData_struct.encounter.id").alias("encounter_id"),
            F.col("worldData_struct.encounter.name").alias("encounter_name"),
            F.explode("worldData_struct.encounter.characterRankings.rankings").alias("r")
        )
        # ===== Flatten ranking struct fields
        .select(
            # ----- Metadata
            F.col("ldts"),
            F.col("rsrc"),
            # ----- Encounter
            F.col("encounter_id"),
            F.col("encounter_name"),
            # ----- Ranking fields
            F.col("r.name").alias("player_name"),
            F.col("r.spec").alias("class"),
            F.col("r.amount").alias("rDPS"),
            F.col("r.aDPS").alias("aDPS"),
            F.col("r.startTime").alias("upload_epoch"),
            F.col("r.duration").alias("duration"),
            # ----- Foreign keys
            F.col("r.lodestoneID").alias("lodestoneID"),
            F.col("r.guild.id").alias("guild_id"),
            F.col("r.guild.name").alias("guild_name"),
            F.col("r.server.id").alias("server_id"),
            F.col("r.server.name").alias("server_name"),
        )
        # ===== Cast & clean
        .select(
            # ----- Metadata
            F.col("ldts"),
            F.col("rsrc"),
            # ----- Encounter
            F.col("encounter_id").cast("int").alias("encounter_id"),
            trim_whitespace(F.initcap(F.col("encounter_name"))).alias("encounter_name"),
            # ----- Player
            trim_whitespace(F.initcap(F.col("player_name"))).alias("player_name"),
            to_title_case(trim_whitespace(F.col("class")), "pascal").alias("class"),
            roundfloat(F.col("rDPS"), 2).alias("rDPS"),
            roundfloat(F.col("aDPS"), 2).alias("aDPS"),
            epoch_to_timestamp(F.col("upload_epoch"), "ms").alias("ranked_date"),
            (F.col("duration") / 1000).cast("int").alias("duration"),
            # ----- Foreign entities
            F.col("lodestoneID").cast("bigint").alias("lodestoneID"),
            F.col("guild_id").cast("bigint").alias("guild_id"),
            trim_whitespace(F.col("guild_name")).alias("guild_name"),
            F.col("server_id").cast("int").alias("server_id"),
            trim_whitespace(F.initcap(F.col("server_name"))).alias("server_name"),
        )
    )

    return df


# ================ DEDUPLICATED SILVER: fflogs_encounter_ranking_silver
dp.create_streaming_table(
    name=f"{settings.silver_schema}.fflogs_encounter_ranking_silver",
    comment="Cleaned encounter rankings, deduplicated by encounter, player, class, and ranked date."
)

dp.create_auto_cdc_flow(
    target=f"{settings.silver_schema}.fflogs_encounter_ranking_silver",
    source="_fflogs_encounter_ranking_silver_c",
    keys=["encounter_id", "player_name", "class", "ranked_date"],
    sequence_by=F.col("ldts"),
    stored_as_scd_type="1"
)
