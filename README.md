# Sozy

# Launching all the steps

## Build index 🔨

```
python3 search_service/build_index.py --paths export_dir/face_embedding_dump/ export_dir_2/face_embedding_dump/ export_dir_3/face_embedding_dump/
```

## Serve the results 👩‍💻

```
 cd search_service/ && uvicorn search_server:app --reload
```

## Display the results in HTML with the face bounding box 🖼️

```
<img id="example-element" src="{{element}}"
 style="object-position: -{{bb[0]}}px -{{bb[3]}}px;object-fit: none; width: {{bb[1] - bb[3]}}px; height: {{bb[2] - bb[0]}}px">
```
