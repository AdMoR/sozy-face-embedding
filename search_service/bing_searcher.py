from typing import NamedTuple
from urllib import parse, request
import re
import argparse
import time


class BingUrls(NamedTuple):
    query: str
    limit: int
    filters: str = ''

    def run(self, file_path):
        headers = {'User-Agent': 'Mozilla/5.0 (X11; Fedora; Linux x86_64; rv:60.0) Gecko/20100101 Firefox/60.0'}
        i = 0
        page_counter = 1

        try:
            with open(file_path, "a") as f:

                while i < self.limit:
                    # Parse the page source and download pics
                    request_url = 'https://www.bing.com/images/async?q=' + parse.quote_plus(self.query) \
                                  + '&first=' + str(page_counter) + '&count=' + str(self.limit) \
                                  + '&adlt=' + "0" + '&qft=' + self.filters
                    req = request.Request(request_url, None, headers=headers)
                    response = request.urlopen(req)
                    html = response.read().decode('utf8')
                    links = re.findall('murl&quot;:&quot;(.*?)&quot;', html)

                    for link in links:
                        #print(link)
                        f.write(";".join([self.query, link]) + "\n")
                        i += 1
                    page_counter += 1
        except Exception as e:
            print(e)

        time.sleep(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--path", help="file with celebrity names")
    args = parser.parse_args()

    with open(args.path) as f:
        lines = f.readlines()

    with open("done_queries.txt", "r") as g:
        dones = set(g.readlines())

    with open("done_queries.txt", "a") as g:
        for i, line in enumerate(lines):
            print("Doing ", line)
            if i == 0:
                continue
            if line in dones:
                continue

            name = " ".join(line.strip().split(","))
            url_fetcher = BingUrls(name, 50)
            url_fetcher.run("celebrity_urls.txt")

            g.write(line + "\n")
