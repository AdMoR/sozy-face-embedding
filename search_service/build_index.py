import os
import numpy as np
import json
import SPTAG


def load_vectors_and_metadata(doc_path):
    with open(doc_path) as f:
        lines = f.readlines()

    def extract_fn(x):
        fields = x.split(";")
        return ";".join(fields[0:2]), fields[2]

    metadata, embeddings = zip(*map(extract_fn, lines))
    return "\n".join(metadata), \
           np.concatenate(list(map(lambda x: np.array([json.loads(x)]), embeddings)), axis=0)

def prepare_data(data_path):
    """
    Data is expected to be outputed by the spark job.
    So we get a directory with part files
    """
    file_names = [os.path.join(data_path, f) for f in os.listdir(data_path)
                  if "part" in f]
    metadata_array, embeddings_array = list(zip(*map(load_vectors_and_metadata, file_names)))
    return "\n".join(metadata_array), np.concatenate(embeddings_array, axis=0)


doc_path = "./"
meta, vec = prepare_data(doc_path)
index = SPTAG.AnnIndex('BKT', 'Float', 128)
index.SetBuildParam("NumberOfThreads", '4')
index.SetBuildParam("DistCalcMethod", 'Cosine')


if index.BuildWithMetaData(vec, meta, vec.shape[0], False):
    index.Save("sptag_index") # Save the index to the disk

os.listdir('sptag_index')