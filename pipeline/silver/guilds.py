import sys
sys.path.append("/Workspace/Users/stan.mng@gmail.com/fflogs_graphql/jobs")

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import DataFrame

from settings import fflogs_settings as settings
from utils import trim_whitespace

# ================ JSON DATA SCHEMA
guildDataSchema = T.StructType([
    T.StructField("guilds", T.StructType([
        T.StructField("total", T.IntegerType()),
        T.StructField("has_more_pages", T.BooleanType()),
        T.StructField("data", T.ArrayType(T.StructType([
            T.StructField("id", T.IntegerType()),
            T.StructField("name", T.StringType()),
            T.StructField("description", T.StringType()),
            T.StructField("server", T.StructType([
                T.StructField("id", T.IntegerType()),
                T.StructField("region", T.StructType([
                    T.StructField("id", T.IntegerType())
                ]))
            ])),
            T.StructField("zoneRanking", T.StructType([
                T.StructField("progress", T.StructType([
                    T.StructField("worldRank", T.StructType([T.StructField("number", T.IntegerType())])),
                    T.StructField("regionRank", T.StructType([T.StructField("number", T.IntegerType())]))
                ])),
                T.StructField("speed", T.StructType([
                    T.StructField("worldRank", T.StructType([T.StructField("number", T.IntegerType())])),
                    T.StructField("regionRank", T.StructType([T.StructField("number", T.IntegerType())]))
                ]))
            ]))
        ])))
    ]))
])

# ================ STREAMING TABLE: _fflogs_guilds_silver_c
@dp.table(
    name=f"{settings.silver_schema}._fflogs_guilds_silver_c",
    comment="Streaming table containing cleaned and structured guild data. Not yet deduplicated."
)
@dp.expect_or_drop("valid_guild_id", "guild_id IS NOT NULL")
@dp.expect_or_drop("valid_server_id", "server_id IS NOT NULL")
@dp.expect_or_drop("valid_region_id", "region_id IS NOT NULL")
@dp.expect_or_drop("has_any_ranking", 
    "world_prog_rank IS NOT NULL OR region_prog_rank IS NOT NULL OR world_speed_rank IS NOT NULL OR region_speed_rank IS NOT NULL"
)
def _fflogs_guilds_silver_c() -> DataFrame:
    return (spark.readStream.table(
            f"{settings.bronze_schema}.fflogs_guilds_raw"
        )
        # ===== Parse JSON string to struct
        .withColumn("guildData_struct", 
            F.from_json(F.col("guildData"), guildDataSchema)
        )
        # ===== Unpack nested struct
        .select(
            "ldts",
            "rsrc",
            F.explode("guildData_struct.guilds.data").alias("s")
        )
        # ===== Flatten guild struct fields
        .select(
            F.col("ldts"),
            F.col("rsrc"),
            F.col("s.id").alias("guild_id"),
            F.col("s.name").alias("guild_name"),
            F.col("s.description").alias("guild_description"),
            F.col("s.server.id").alias("server_id"),
            F.col("s.server.region.id").alias("region_id"),
            F.col("s.zoneRanking.progress.worldRank.number").alias("world_prog_rank"),
            F.col("s.zoneRanking.progress.regionRank.number").alias("region_prog_rank"),
            F.col("s.zoneRanking.speed.worldRank.number").alias("world_speed_rank"),
            F.col("s.zoneRanking.speed.regionRank.number").alias("region_speed_rank"),
        )
        # ===== Cast & clean
        .select(
            F.col("ldts"),
            F.col("rsrc"),
            F.col("guild_id").cast("int"),
            F.nullif(F.initcap(trim_whitespace(
                F.col("guild_name")
            )), F.lit("")).alias("guild_name"),
            F.nullif(trim_whitespace(
                F.col("guild_description")
            ), F.lit("")).alias("guild_description"),
            F.col("server_id").cast("int"),
            F.col("region_id").cast("int"),
            F.col("world_prog_rank").cast("int"),
            F.col("region_prog_rank").cast("int"),
            F.col("world_speed_rank").cast("int"),
            F.col("region_speed_rank").cast("int"),
        )
    )


# ================ DEDUPLICATED SILVER: fflogs_guilds_silver
dp.create_streaming_table(
    name=f"{settings.silver_schema}.fflogs_guilds_silver",
    comment="Cleaned guild data, deduplicated by guild_id keeping the latest load."
)

dp.create_auto_cdc_flow(
    target=f"{settings.silver_schema}.fflogs_guilds_silver",
    source="_fflogs_guilds_silver_c",
    keys=["guild_id"],
    sequence_by=F.col("ldts"),
    stored_as_scd_type="1"
)