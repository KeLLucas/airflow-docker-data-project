from pathlib import Path
from pyspark.sql import SparkSession
import os

def run():
    RAW_PATH = Path(os.getenv("RAW_DATA_PATH"))
    PROCESSED_PATH = os.getenv("PROCESSED_DATA_PATH")

    if not RAW_PATH or not PROCESSED_PATH:
        raise ValueError("Variáveis de ambiente não configuradas corretamente.")

    spark = SparkSession.builder \
        .appName("UsersPipeline") \
        .getOrCreate()
    
    try:
        df = spark.read.option("multiline", "true").json(str(RAW_PATH))

        df_users = df.selectExpr("explode(users) as user")

        df_final = df_users.select(
        "user.id",
        "user.firstName",
        "user.lastName",
        "user.age",
        "user.gender",
        "user.email",
        "user.phone",
        "user.username"
        )

        df_final.coalesce(1) \
            .write \
            .mode("overwrite") \
            .option("header", True) \
            .csv(PROCESSED_PATH)
        
        
        print("Transformação concluída e CSV salvo.")

    finally:

        spark.stop()

if __name__ == "__main__":
    run()
