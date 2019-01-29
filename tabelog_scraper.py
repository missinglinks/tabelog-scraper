"""
Scraper for Tabelog user reviews

Based on a area (and its entry URL), this script 
fetches all available comments for this area.

For each area, a maximum of 1200 restaurants can be 
retrieved, and for each restaurant, max. 1200 comments
are available.

Restaurants are prioritized by their comment count.
Comments are prioritized by their date (starting with the
newest).

The (full) comment pages will be saved in a zip
archive. 

"""

import requests
import json
import os
import sys 
import click
import pandas as pd

from tqdm import tqdm
from bs4 import BeautifulSoup
from zip_archive import ZipArchive

HEADERS = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}

RST_URL = "{base}rstLst/{page}/?Srt=D&SrtT=rvcn"
COMMENT_URL = "{url}dtlrvwlst/COND-0/smp0/?lc=2&rvw_part=all&PG={page}"

RST_ARCHIVE = "data/rst_links.zip"
EXPORT_DIR = "data"


def json_file(name):
    return "{}.json".format(name)

class TabelogScraper:

    def __init__(self, export_dir=EXPORT_DIR ):
        self.export_dir = export_dir
        self.comments_dir = os.path.join(self.export_dir, "comments")
        
        if not os.path.exists(self.export_dir):
            os.makedirs(self.export_dir)
        if not os.path.exists(self.comments_dir):
            os.makedirs(self.comments_dir)

        rst_archive_filename = os.path.join(self.export_dir, "rst_links.zip")
        self.rst_archive = ZipArchive(rst_archive_filename)

    def _get_rst_links(self, url):
        links = []
        for page in range(60):
            print("\t Page: ", page+1, "\t", RST_URL.format(base=url, page=page+1))
            rsp = requests.get(RST_URL.format(base=url, page=page+1), headers=HEADERS)
            soup = BeautifulSoup(rsp.text, "html.parser")

            count = soup.find("span", {"class": "list-condition__count"})
            if str(count.text) == "0":
                break

            links += [ (x.text, x["href"]) for x in soup.find_all("a", { "class": "list-rst__rst-name-target"}) ]
        return links


    def fetch_restaurant_links(self, area_name, url):

        print("Fetching restaurant links for <{}> ...".format(area_name))
        if not self.rst_archive.contains(json_file(area_name)):

            rst_links = []

            rst_links += self._get_rst_links(url)

            print("\t No. of restaurants: ", len(rst_links))
            print(" ")

            payload = {
                "area": area_name,
                "rst_links": rst_links
            }
            self.rst_archive.add(json_file(area_name), payload)

    def _load_rst_links(self, area_name):
        filename = json_file(area_name)
        if self.rst_archive.contains(filename):
            data = self.rst_archive.get(filename)
            return data
        else:
            return None

    def _get_comment_links(self, url):
        comment_links = []
        for page in range(12):
            rsp = requests.get(COMMENT_URL.format(url=url, page=page+1), headers=HEADERS)
            soup = BeautifulSoup(rsp.text, "html.parser")
            
            error = soup.find("h2", { "class": "error-common__title" })
            if error:
                break

            links = soup.find_all("div", { "class": "rvw-simple-item" })
            comment_links += [ x["data-detail-url"] for x in links ]
            
            if len(links) < 90:
                break
        return comment_links    

    def fetch_comment_links(self, area_name):
        rst = self._load_rst_links(area_name)

        print("Fetching comment links for <{}> ...".format(area_name))

        out_filename = "{}_links.zip".format(area_name)
        out_path = os.path.join(self.comments_dir, out_filename)
        link_archive = ZipArchive(out_path)
        
        #gether comment links
        for name, link in rst["rst_links"]:
            id_ = link.split("/")[-2]
            
            data_file = "{}.json".format(id_)
            if not link_archive.contains(data_file):
                
                comment_links = self._get_comment_links(link)
                print("\t", "<{}>: {}".format(name, len(comment_links)))
                link_archive.add(data_file, comment_links)
            
        link_archive.close()    

    def fetch_comment_html(self, area_name):

        links_filepath = os.path.join(self.comments_dir, "{}_links.zip".format(area_name))
        links_archive = ZipArchive(links_filepath)

        out_filepath = os.path.join(self.comments_dir, "{}.zip".format(area_name))
        html_archive = ZipArchive(out_filepath)
        
        print("Fetching comment html files for <{}> ...".format(area_name))

        for filename in links_archive:
            sub_dir = filename.replace(".json", "")
            rst_links = links_archive.get(filename)
            
            for rst_link in tqdm(rst_links):
                id_ = rst_link.split("/?")[0].split("/")[-1]
                html_filepath = "{}/{}.txt".format(sub_dir, id_)
                
                if not html_archive.contains(html_filepath):
                    rsp = requests.get("https://tabelog.com{}".format(rst_link), headers=HEADERS)
                    html_archive.add(html_filepath, str(rsp.text))

    def fetch(self, area_name, url):
        self.fetch_restaurant_links(area_name, url)
        self.fetch_comment_links(area_name)
        self.fetch_comment_html(area_name)



def _iter_area_list(area_list_filepath):
    df = pd.read_csv(area_list_filepath, sep="\t")
    print(area_list_filepath)
    areas = json.loads(df.to_json(orient="records"))
    for area in areas:
        print(area)
        yield (area["area"], area["url"])

@click.command()
@click.argument("area_list", default="area_list.csv")
@click.option("--out_dir", "-o", default="data")
def cli(area_list, out_dir):
    if not os.path.exists(area_list):
        sys.exit(1)
    
    tls = TabelogScraper(export_dir=out_dir)

    for area_name, url in _iter_area_list(area_list):
        tls.fetch(area_name, url)

if __name__ == "__main__":
    cli()
