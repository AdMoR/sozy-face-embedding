import argparse
import os

import pandas as pd
import pyspark.sql.functions as F
from databricks_jobs.common import Job
from pyspark.sql.types import ArrayType, FloatType


def create_spark_df_from_data(spark, data):
    df = pd.DataFrame(data)
    return spark.createDataFrame(df)


class FaceEmbeddingJob(Job):

    def __init__(self, path=None, out_path="./export_dir"):
        self.image_path = path
        self.output_path = os.path.join(out_path, "face_embedding_dump")
        super(FaceEmbeddingJob, self).__init__()

    def init_adapter(self):
        if not self.conf:
            self.logger.info(
                "Init configuration was not provided, using configuration from default_init method"
            )

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
                image = face_recognition.load_image_file(path)
                face_locations = face_recognition.api.face_locations(image)
                face_embeddings = face_recognition.face_encodings(image, known_face_locations=face_locations)
                return list(zip(
                    [path] * len(face_locations),
                    face_locations,
                    map(lambda x: x.tolist(), face_embeddings))
                )
            else:
                return []

        image_paths = [os.path.join(self.image_path, f) for f in os.listdir(self.image_path)][:100]
        data = {"image_path": image_paths}

        image_df = create_spark_df_from_data(self.spark, data). \
            repartition(int(len(image_paths) ** 0.5), "image_path").\
            rdd.\
            flatMap(lambda x: extract_face_emb(x.image_path)). \
            saveAsTextFile(self.output_path)

        self.logger.info("Sample job finished!")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--path", help="display a square of a given number",
                        type=str)
    args = parser.parse_args()
    job = FaceEmbeddingJob(args.path)
    job.launch()
