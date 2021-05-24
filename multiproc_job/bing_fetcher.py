from typing import NamedTuple
from urllib import parse, request
import re
import time


class BingUrls(NamedTuple):
    query: str
    limit: int
    filters: str = ''

    def run(self, file_path="all_urls.txt"):
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
                        out = ";".join([self.query, link])
                        f.write(out + "\n")
                        yield link
                        i += 1
                    page_counter += 1
        except Exception as e:
            print(e)

        time.sleep(1)


def fetch_fn(q_in, q_out, i):
    while not q_in.empty():
        query = q_in.get()
        for elem in BingUrls(query, 50, "").run(f"all_urls_{i}.txt"):
            while q_out.full():
                time.sleep(0.5)
            q_out.put(elem)
