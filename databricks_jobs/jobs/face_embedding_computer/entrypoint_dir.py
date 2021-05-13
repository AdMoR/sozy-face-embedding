import argparse
import os
import math
import json
import urllib

import pandas as pd
import pyspark.sql.functions as F

from databricks_jobs.common import Job
from databricks_jobs.jobs.utils.face_processing import extract_face_emb
from pyspark.sql.types import ArrayType, FloatType
from pyspark.sql import SparkSession
from pyspark.sql.functions import sha2, concat_ws


def create_spark_df_from_data(spark, data):
    df = pd.DataFrame(data)
    return spark.createDataFrame(df)


class FaceEmbeddingJob(Job):

    def __init__(self, path=None, out_path="./export_dir"):
        self.image_path = path
        self.output_path = os.path.join(out_path, "face_embedding_dump")
        spark = SparkSession.builder. \
            master("local[1]").getOrCreate()
        super(FaceEmbeddingJob, self).__init__(spark=spark)

    def init_adapter(self):
        if not self.conf:
            self.logger.info(
                "Init configuration was not provided, using configuration from default_init method"
            )

    def prepare_dataframe(self):
        is_real_image_directory = os.path.isdir(self.image_path) and len(os.listdir(self.image_path)) > 0 and \
            any([os.path.splitext(f)[1][1:].lower() in ["jpg", "png", "jpeg"]
                 for f in os.listdir(self.image_path)])

        if is_real_image_directory:
            image_paths = [os.path.join(self.image_path, f) for f in os.listdir(self.image_path)]
        else:
            data = json.load(open(self.image_path))
            image_paths = list(map(lambda x: x["image"]["file_url"], data.values()))
        data = {"image_path": image_paths}
        return create_spark_df_from_data(self.spark, data), len(image_paths) ** 0.25

    def launch(self):
        self.logger.info("Launching databricks_jobs job")

        df, repartition = self.prepare_dataframe()

        image_df = df. \
            repartition(repartition, sha2("image_path", 224)).\
            rdd.\
            flatMap(lambda x: extract_face_emb(x.image_path)). \
            map(lambda x: ';'.join(map(str, x))).\
            saveAsTextFile(self.output_path)

        self.logger.info("Sample job finished!")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--path", help="display a square of a given number",
                        type=str)
    args = parser.parse_args()
    job = FaceEmbeddingJob(path=args.path)
    job.launch()
