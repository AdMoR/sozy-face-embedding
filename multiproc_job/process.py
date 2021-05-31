import multiprocessing as mp
from multiprocessing import Pool, Process, Queue, Lock
from queue import Empty
from databricks_jobs.jobs.utils.image_downloader import detect_filename
import argparse
import os
import signal
import face_recognition
import time
from multiproc_job.bing_fetcher import BingUrls
from urllib import request


DELIMITER = "|"


def get_urls(path, q):
    with open(path) as f:
        return list(map(lambda x: q.put(" ".join(x.strip().split(","))),
                        f.readlines()[1:]))


def fetch_fn(q_in, q_out, i):
    while not q_in.empty():
        query = q_in.get()
        print(f"Querier : doing {query}")
        for elem in BingUrls(query, 50, "").run(f"all_urls_{i}.txt"):
            while q_out.full():
                time.sleep(0.5)
            q_out.put(elem)
    print("---> Querier exiting")


def download(url):
    headers = {'User-Agent': 'Mozilla/5.0 (X11; Fedora; Linux x86_64; rv:60.0) Gecko/20100101 Firefox/60.0'}
    req = request.Request(url, None, headers=headers)
    r = request.urlopen(req, timeout=3)
    filename = os.path.join("download", detect_filename(url))
    with open(filename, "wb") as f:
        f.write(r.read())
    return filename


def download_worker(q_in, q_out, i):
    fail_count = 0
    while fail_count < 100:
        try:
            url = q_in.get(block=True, timeout=60)
            try:
                local_path = download(url)
                while q_out.full():
                    time.sleep(0.5)
                q_out.put(DELIMITER.join([url, local_path]))
            except Exception as e:
                print(f"Downloader : {e} from {url}")
        except Empty:
            print("No download job for 60 secs")
            fail_count += 1
    print(f"Downloader {i} exiting after {fail_count} job retrieval failures")


def extract_face_emb_url(queue_in):
    """
    Given an image, we can find multiple faces
    Each face will generate one 128-sized embedding

    :param queue_in: multiprocessing queue where we fetch the url to download from
    :return: List(tuple(image_path: str,
                        face_location: tuple(x, y, dx, dy),
                        face_embedding: List[float]))
    """
    failures = 0
    local_path = ""
    with open("result.txt", "w") as out_file:
        while failures < 1000:
            try:
                msg = queue_in.get(block=True, timeout=120)
                path, local_path = msg.split(DELIMITER)
                image = face_recognition.load_image_file(local_path)
                face_locations = face_recognition.api.face_locations(image, number_of_times_to_upsample=0,
                                                                     model="cnn")

                def min_size(face_loc):
                    return (face_loc[1] - face_loc[3]) * (face_loc[2] - face_loc[0]) > 100 ** 2
                face_locations = list(filter(min_size, face_locations))

                if len(face_locations) == 0:
                    continue

                face_embeddings = face_recognition.face_encodings(
                    image,
                    known_face_locations=face_locations,
                    model="large"
                )

                result = list(zip(
                    [path] * len(face_locations),
                    face_locations,
                    map(lambda x: x.tolist(), face_embeddings))
                )
                out_file.write("\n".join(map(lambda x: DELIMITER.join(map(str, x)), result)) + "\n")
            except Empty:
                print("No gpu job for 120 secs")
                failures += 1
            except Exception as e:
                print("GPU process : ", e)
            finally:
                if local_path != "download.wget" and os.path.exists(local_path):
                    os.remove(local_path)
    print("Exiting Gpu processing")


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

    queriers = [Process(target=fetch_fn, args=(q_in_queries, q_in_url, i)) for i in range(4)]
    for p in queriers:
        p.start()

    downloaders = [Process(target=download_worker, args=(q_in_url, q_out, i)) for i in range(4)]
    for p in downloaders:
        p.start()

    gpu_p = Process(target=extract_face_emb_url, args=(q_out,))
    gpu_p.start()

    gpu_p.join()
    print("Gpu process failed, killing all others processes")
    for p in queriers:
        p.join()
    for p in downloaders:
        p.join()
