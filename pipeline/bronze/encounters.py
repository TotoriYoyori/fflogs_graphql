import sys
sys.path.append("/Workspace/Users/stan.mng@gmail.com/fflogs_graphql/jobs")

from pyspark import pipelines as dp
from pyspark.sql.functions import current_timestamp, lit
from pyspark.sql import DataFrame

from settings import fflogs_settings as settings

# =============== ENCOUNTERS RAW 
@dp.table(
    name=f"{settings.bronze_schema}.fflogs_encounters_ranking_raw", 
    comment="Raw encounter ranking data for all five fights of Heavyweight Tier."
)
def fflogs_encounters_ranking_raw() -> DataFrame:
    return (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("multiLine", "true")
        .load(f"{settings.volume_dir}/*.json")
        .withColumn("ldts", current_timestamp())
        .withColumn("rsrc", lit(settings.SECRET_SCOPE))
    )
