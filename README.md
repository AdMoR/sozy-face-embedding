# face-embedding-computer

This is a sample project for Databricks, generated via cookiecutter.

While using this project, you need Python 3.X and `pip` or `conda` for package management.

## Installing project requirements

```bash
pip install -r unit-requirements.txt
```

## Install project package in a developer mode

System installs

```
brew install jpeg
```

```bash
pip install -r requirements.txt
pip install -e .
```

For an integration test on interactive cluster, use the following command:
```
dbx execute --cluster-name=<name of interactive cluster> --job=face-embedding-computer-sample-integration-test
```

For a test on an automated job cluster, deploy the job files and then launch:
```
dbx deploy --job=face-embedding-computer-sample-integration-test --files-only
dbx launch --job=face-embedding-computer-sample-integration-test --as-run-submit --trace
```


```bash
dbx deploy --files-only
```

To launch the file-based deployment:
```
dbx launch --as-run-submit --trace
```

This type of deployment is handy for working in different branches, not to affect the main job definition.

## Deployment for Run Now API

To deploy files and update the job definitions:

```bash
dbx deploy
```

To launch the file-based deployment:
```
dbx launch --job=<job-name>
```


# CoZy

## Build the docker 

```
cd SPTAG
docker build . -t sptag
```


## Compute the embedding based on an image directory

```
cd face-embedding-computer
python3 databricks_jobs/jobs/face_embedding_computer/entrypoint.py --path /Users/amorvan/Downloads/img_align_celeba
```

# Launching all the steps

## Build index 

```
python3 search_service/build_index.py --paths export_dir/face_embedding_dump/ export_dir_2/face_embedding_dump/ export_dir_3/face_embedding_dump/
```

## Serve the results 

```
 cd search_service/ && uvicorn search_server:app --reload
```

## Display the results

```
<img id="example-element" src="{{element}}"
 style="object-position: -{{bb[0]}}px -{{bb[3]}}px;object-fit: none; width: {{bb[1] - bb[3]}}px; height: {{bb[2] - bb[0]}}px">
```