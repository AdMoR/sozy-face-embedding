import os
import numpy as np
import json
import itertools
from functools import reduce
from annoy import AnnoyIndex
import argparse


def load_vectors_and_metadata(doc_path):
	with open(doc_path) as f:
		lines = f.readlines()

	def extract_fn(x):
		fields = x.split(";")
		return ";".join(fields[0:2]), fields[2]

	metadata, embeddings = zip(*map(extract_fn, lines))
	return list(metadata), \
		   np.concatenate(list(map(lambda x: np.array([json.loads(x)]), embeddings)), axis=0)

def prepare_data(data_path):
	"""
	Data is expected to be outputed by the spark job.
	So we get a directory with part files
	"""
	file_names = [os.path.join(data_path, f) for f in os.listdir(data_path)
				  if f.startswith("part")]
	metadata_array, embeddings_array = list(zip(*map(load_vectors_and_metadata, file_names)))
	return list(itertools.chain.from_iterable(metadata_array)), np.concatenate(embeddings_array, axis=0)


if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument("--paths", nargs='+', help="folder of parsed pictures")
	args = parser.parse_args()
	metadata_array = list()
	embeddings_array = list()
	for doc_path in args.paths:
		meta_, vec_ = prepare_data(doc_path)
		metadata_array.append(meta_)
		embeddings_array.append(vec_)
		
	meta, vec = list(itertools.chain.from_iterable(metadata_array)), np.concatenate(embeddings_array, axis=0)

	f = vec.shape[1]
	t = AnnoyIndex(f, 'angular')  # Length of item vector that will be indexed
	for i in range(vec.shape[0]):
		t.add_item(i, vec[i])

	t.build(30) # 30 trees
	t.save('test.ann')
	json.dump(meta, open("metadata.json", "w"))
