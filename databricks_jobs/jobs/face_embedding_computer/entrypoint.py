import argparse
import os
import math
import json
import urllib

import pandas as pd
import pyspark.sql.functions as F

from databricks_jobs.common import Job
from databricks_jobs.jobs.utils.face_processing import extract_face_emb, extract_face_emb_url
from pyspark.sql.types import ArrayType, FloatType
from pyspark.sql import SparkSession
from pyspark.sql.functions import sha2, concat_ws


def create_spark_df_from_data(spark, data):
    df = pd.DataFrame(data)
    return spark.createDataFrame(df)


class FaceEmbeddingJob(Job):

    def __init__(self, path=None, out_path="./export_dir_3"):
        self.image_path = path
        self.output_path = os.path.join(out_path, "face_embedding_dump")
        spark = SparkSession.builder. \
            master("local[3]").getOrCreate()
        super(FaceEmbeddingJob, self).__init__(spark=spark)

    def init_adapter(self):
        if not self.conf:
            self.logger.info(
                "Init configuration was not provided, using configuration from default_init method"
            )

    def prepare_dataframe(self):
        with open(self.image_path) as f:
            lines = f.readlines()

        parsed_data = list(map(lambda x: {k: v for v, k in zip(x.strip().split(";"), ["title", "image_path"])},
                               lines))
        return create_spark_df_from_data(self.spark, parsed_data), int(len(parsed_data) ** 0.33)

    def launch(self):
        self.logger.info("Launching databricks_jobs job")

        df, repartition = self.prepare_dataframe()

        image_df = df. \
            select("image_path").distinct().\
            repartition(repartition, sha2("image_path", 224)).\
            rdd.\
            flatMap(lambda x: extract_face_emb_url(x.image_path)). \
            map(lambda x: ';'.join(map(str, x))). \
            saveAsTextFile(self.output_path)

        self.logger.info("Sample job finished!")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--paths", nargs='+', help="the file to parse",
                        type=str)
    parser.add_argument("--out_paths", nargs='+', help="the out dir",
                        type=str)
    args = parser.parse_args()
    for path, out_path in zip(args.paths, args.out_paths):
        job = FaceEmbeddingJob(path=path, out_path=out_path)
        job.launch()
