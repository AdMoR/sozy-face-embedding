import flickr_api
import os
import argparse


class FlickrCrawler():
    filename = "my_flickr_auth.txt"
    flickr_api.set_keys(api_key = 'd8827d1d71d367b86b324b1a1b6350e2', api_secret = '1634c68f986a36bc')
    def __init__(self):
        if not os.path.exists(self.filename):
            a = flickr_api.auth.AuthHandler()
            perms = "read"
            url = a.get_authorization_url(perms)
            print(f"Go to {url}")
            verifier_code = input("Please input the verifier code : ")
            a.set_verifier(verifier_code)
            a.save(self.filename)
            flickr_api.set_auth_handler(a)
        else:
            flickr_api.set_auth_handler(self.filename)
    def crawl_tag(self, tag="person", max_iter=500000):
        w = flickr_api.Walker(flickr_api.Photo.search, tags=tag)
        for i, photo in enumerate(w):
            try:
                #print(photo.url_z)
                yield photo.title, photo.url_z
            except AttributeError:
                continue
            if i>=max_iter:
                break

if __name__ == "__main__":
	fc = FlickrCrawler()
	

	parser = argparse.ArgumentParser()
	parser.add_argument("--tag", help="tag for flickr api")
	args = parser.parse_args()

	try:
		with open(f"flickr_{args.tag}.txt", "w") as f:
		    for elem in fc.crawl_tag(tag=args.tag):
		        f.write(";".join(elem) + "\n")
	except:
		print("failed")
