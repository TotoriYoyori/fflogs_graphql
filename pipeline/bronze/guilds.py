import sys
sys.path.append("/Workspace/Users/stan.mng@gmail.com/fflogs_graphql/jobs")

from pyspark import pipelines as dp
from pyspark.sql.functions import current_timestamp, lit
from pyspark.sql import DataFrame

from settings import fflogs_settings as settings

# =============== GUILDS RAW
@dp.table(
    name=f"{settings.bronze_schema}.fflogs_guilds_raw",
    comment="Raw guild data including zone rankings for progress and speed."
)
def fflogs_guilds_raw() -> DataFrame:
    return (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("multiLine", "true")
        .load(f"{settings.guilds_volume}/*.json")
        .withColumn("ldts", current_timestamp())
        .withColumn("rsrc", lit(settings.SECRET_SCOPE))
    )
