import face_recognition
import os
import wget


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
    if path:
        image = face_recognition.load_image_file(path)
        face_locations = face_recognition.api.face_locations(image)

        def min_size(face_loc):
            return (face_loc[1] - face_loc[3]) * (face_loc[2] - face_loc[0]) > 60 ** 2
        face_locations = list(filter(min_size, face_locations))
        face_embeddings = face_recognition.face_encodings(image, known_face_locations=face_locations)
        return list(zip(
            [path] * len(face_locations),
            face_locations,
            map(lambda x: x.tolist(), face_embeddings))
        )
    else:
        return []


def extract_face_emb_url(path):
    """
    Given an image, we can find multiple faces
    Each face will generate one 128-sized embedding

    To go back to a format spark understand, we need to transform np array into lists

    :param path: path of the image
    :return: List(tuple(image_path: str,
                        face_location: tuple(x, y, dx, dy),
                        face_embedding: List[float]))
    """
    result = []
    local_path = ""
    if path.startswith("http"):
        try:
            local_path = wget.download(path)
            image = face_recognition.load_image_file(local_path)
            face_locations = face_recognition.api.face_locations(image, model="cnn")
            face_embeddings = face_recognition.face_encodings(image, known_face_locations=face_locations, model="large")
            result = list(zip(
                [path] * len(face_locations),
                face_locations,
                map(lambda x: x.tolist(), face_embeddings))
            )
        except Exception as e:
            print(e)
        finally:
            if os.path.exists(local_path):
                os.remove(local_path)
    return result

