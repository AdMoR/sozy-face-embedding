import argparse
import os
import math

import pandas as pd
import pyspark.sql.functions as F
from databricks_jobs.common import Job
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
            any([os.path.splitext("jean.jpg")[1][1:].lower() in ["jpg", "png", "jpeg"]
                 for f in os.listdir(self.image_path)])

        if is_real_image_directory:
            image_paths = [os.path.join(self.image_path, f) for f in os.listdir(self.image_path)]
            data = {"image_path": image_paths}
            return create_spark_df_from_data(self.spark, data)
        else:
            return self.spark.read.json(self.image_path)

    def launch(self):
        self.logger.info("Launching databricks_jobs job")

        def extract_face_emb(path):
            """
            Given an image, we can find multiple faces
            Each face will generate one 128-sized embedding

            To go back to a format spark understand, we need to transform np array into lists

            :param path: path of the image
            :return: List(tuple(image_path: str,
                                face_location: tuple(x, y, dx, dy),
                                face_embedding: List[float]))
            """
            import face_recognition
            if path:
                to_open = path if not path.startswith("http") else urllib2.urlopen(path)
                image = face_recognition.load_image_file(to_open)
                face_locations = face_recognition.api.face_locations(image)
                face_embeddings = face_recognition.face_encodings(image, known_face_locations=face_locations)
                return list(zip(
                    [path] * len(face_locations),
                    face_locations,
                    map(lambda x: x.tolist(), face_embeddings))
                )
            else:
                return []

        df = self.prepare_dataframe().repartition(1000)
        df.printSchema()
        print(df)
        repartition = math.sqrt(df.count())

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
