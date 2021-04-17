import unittest
import tempfile
import os
import shutil
import pyspark.sql.functions as F

from databricks_jobs.jobs.face_embedding_computer.entrypoint import FaceEmbeddingJob
from pyspark.sql import SparkSession
from unittest.mock import MagicMock


class SampleJobUnitTest(unittest.TestCase):

    def setUp(self):
        os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
        os.environ["OBJC_DISABLE_INITIALIZE_FORK_SAFETY"] = "YES"

    def test_sample(self):

        with tempfile.TemporaryDirectory() as d:
            self.job = FaceEmbeddingJob("/Users/amorvan/Downloads/img_align_celeba", d)
            self.job.dbutils = MagicMock()
            self.job.launch()
            self.assertTrue(os.path.exists(self.job.output_path))
            print(os.listdir(self.job.output_path))

    def tearDown(self):
        pass


if __name__ == "__main__":
    unittest.main()
