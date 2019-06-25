# Tabelog Scraper

Simple webscraper for tabelog user reviews.

### Usage

1. Download repo

2. Setting up new python evironment is recommended

3. Install python modules

```
$ pip install -r requirements.txt
```

4. Add areas (and area links) to area_list.csv (and remove example entries)

5. Run scraper

```
$ python tabelog_scraper.py
```

### Elasticsearch ingest

1. Create config.py

2. Add elasticsearch server to config.py

```
ES_SERVER = "http://user:pwd@1.1.1.1:9200"
```

3. Run ingest script with your comments directory (default="/data/comments") as an argument 

```
$ python elasticsearch_ingest.py <comments_dir>
```

### License

GNU General Public License v3.0
