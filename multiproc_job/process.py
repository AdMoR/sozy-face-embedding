import multiprocessing as mp
from multiprocessing import Pool, Process, Queue, Lock
from databricks_jobs.jobs.utils.image_downloader import detect_filename
import argparse
import os
import face_recognition
import time
from multiproc_job.bing_fetcher import fetch_fn
from urllib import request


def get_urls(path, q):
    with open(path) as f:
        return list(map(lambda x: q.put(" ".join(x.strip().split(","))),
                        f.readlines()[1:]))


def download(url):
    r = request.urlopen(url, block=True, timeout=10)
    filename = os.path.join("download", detect_filename(url))
    with open(filename, "wb") as f:
        f.write(r.read())
    return filename


def download_worker(q_in, q_out):
    while True:
        url = q_in.get(block=True, timeout=60)
        try:
            local_path = download(url)
            while q_out.full():
                time.sleep(0.5)
            q_out.put(";".join([url, local_path]))
        except Exception as e:
            print(e)


def extract_face_emb_url(queue_in):
    """
    Given an image, we can find multiple faces
    Each face will generate one 128-sized embedding

    :param queue_in: multiprocessing queue where we fetch the url to download from
    :return: List(tuple(image_path: str,
                        face_location: tuple(x, y, dx, dy),
                        face_embedding: List[float]))
    """
    with open("result.txt", "w") as out_file:
        while True:
            try:
                msg = queue_in.get(block=True, timeout=60)
                path, local_path = msg.split(";")
                image = face_recognition.load_image_file(local_path)
                face_locations = face_recognition.api.face_locations(image, model="cnn")

                def min_size(face_loc):
                    return (face_loc[1] - face_loc[3]) * (face_loc[2] - face_loc[0]) > 100 ** 2
                face_locations = list(filter(min_size, face_locations))

                if len(face_locations) == 0:
                    continue

                face_embeddings = face_recognition.face_encodings(image,
                                                                  known_face_locations=face_locations, model="large")

                result = list(zip(
                    [path] * len(face_locations),
                    face_locations,
                    map(lambda x: x.tolist(), face_embeddings))
                )
                out_file.write(",".join(map(str, result)) + "\n")
            except queue.Empty:
                time.sleep(0.5)
            except Exception as e:
                print("GPU process : ", e)
            finally:
                if local_path != "download.wget" and os.path.exists(local_path):
                    os.remove(local_path)


if __name__ == '__main__':
    mp.set_start_method('forkserver', force=True)
    parser = argparse.ArgumentParser()
    parser.add_argument("--path", help="file with celebrity names")
    args = parser.parse_args()

    if not os.path.exists("download"):
        os.mkdir("download")

    q_in_queries = Queue()
    q_in_url = Queue(maxsize=1500)
    q_out = Queue(maxsize=150)

    get_urls(args.path, q_in_queries)

    queriers = [Process(target=fetch_fn, args=(q_in_queries, q_in_url, i)) for i in range(5)]
    for p in queriers:
        p.start()

    downloaders = [Process(target=download_worker, args=(q_in_url, q_out)) for _ in range(6)]
    for p in downloaders:
        p.start()

    gpu_p = Process(target=extract_face_emb_url, args=(q_out,))
    gpu_p.start()

    for p in queriers:
        p.join()
    for p in downloaders:
        p.kill()
    gpu_p.kill()
