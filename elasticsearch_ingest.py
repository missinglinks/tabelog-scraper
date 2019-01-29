import json
import os
import click

from zip_archive import ZipArchive
from elasticsearch import Elasticsearch
from config import ES_SERVER
from bs4 import BeautifulSoup
from tqdm import tqdm

INDEX = "tabelog_comments"
DOC_TYPE = "comment"
MAPPING = {
    "comment": {
        "properties": {
            "rst_id": { "type": "keyword" },
            "rst_name": { "type": "keyword" },
            "rst_address": { "type": "text" },
            "rst_loc": { "type": "geo_point" },
            "rst_genre": { "type": "keyword" },
            "usr_name": { "type": "keyword" },
            "usr_id": { "type": "keyword"},
            "cmt_date": { "type": "date" },
            "cmt_id": { "type": "keyword" },
            "cmt_title": { "type": "text" },
            "cmt_text": { "type": "text" },
        }
    }
}


def get_genres(soup):
    genres = []
    for info in soup.find_all("dl", {"class": "rdheader-subinfo__item"}):
        if info.find("dt").text == "ジャンル：":
            return [ x.text.strip() for x in info.find_all("a", {"class": "linktree__parent-target"})]

def get_location(soup):
    address = soup.find("p", { "class": "rstinfo-table__address" }).text
    map_ = soup.find("div", { "class": "rstinfo-table__map" })
    img = map_.find("img")
    loc = img["data-original"].split("&zoom=")[0].split("red%7C")[-1].split(",")
    return {
        "address": address,
        "lat": float(loc[0]),
        "lng": float(loc[1])
    }

def init_es(es):

    if es.indices.exists(index=INDEX):
        es.indices.delete(index=INDEX)
    
    es.indices.create(INDEX)
    es.indices.put_mapping(index=INDEX, doc_type=DOC_TYPE, body=MAPPING)    

def ingest(es, filepath):
    
    html_archive = ZipArchive(filepath)
    for filename in tqdm(html_archive):
        
        rst_id, cmt_id = filename.split("/")
        cmt_id = cmt_id.replace(".txt", "")
        data = html_archive.get(filename)
        soup = BeautifulSoup(data, "html.parser")        
        loc = get_location(soup)
        genres =  get_genres(soup)
        
        table = soup.find("table", { "class": "c-table"})
        rst_name = table.find("td").text.strip().replace("\n", "")
        rst_name = " ".join(rst_name.split())
        
        
        usr = soup.find("p", { "class": "rvw-item__rvwr-name" })
        usr_id = usr.find("a")["href"]
        usr_name = usr.find("span").find("span").text
        
        
        for review in soup.find_all("div", { "class": "rvw-item"}):
            title = review.find("p", {"class": "rvw-item__title"})
            if title:
                title = title.text
            else:
                title = ""
            text =  review.find("div", {"class": "rvw-item__rvw-comment"}).text
            date = review.find("div", {"class": "rvw-item__single-date"})
            date = date.text.strip()[:7].replace("/", "-")
        
        
            doc = {
                "rst_id": rst_id,
                "rst_name": rst_name,
                "rst_address": loc["address"],
                "rst_loc": "{},{}".format(loc["lat"], loc["lng"]),
                "rst_genre": genres,
                "cmt_id": cmt_id,
                "cmt_date": date,
                "cmt_text": text,
                "cmt_title": title,
                "usr_id": usr_id,
                "usr_name": usr_name
            }
            while True:
                try:
                    es.index(index=INDEX, doc_type=DOC_TYPE, body=doc, id=cmt_id)
                    break
                except:
                    print("retrying insert ...")
                    print(doc)
                    continue
                    
@click.command()
@click.argument("comments_dir") 
@click.option("--rebuild/--no-rebuild", default=False)
@click.option("--area", "-a", default=".zip")
def ingest_comments(comments_dir, rebuild, area):
    
    es = Elasticsearch(ES_SERVER)
    if rebuild:
        init_es(es)

    for filename in os.listdir(comments_dir):
        if "_links" not in filename and area in filename:

            print("Ingest <{}> ...".format(filename))
            filepath = os.path.join(comments_dir, filename)
            ingest(es, filepath)

if __name__ == "__main__":
    ingest_comments()