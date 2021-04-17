from setuptools import find_packages, setup

setup(
    name="databricks_jobs",
    packages=find_packages(exclude=["tests", "tests.*"]),
    setup_requires=["wheel"],
    version=1.0,
    description="Batch job to compute embedding for all the face images",
    author="admor",
)
