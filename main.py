from gcs.gcs import GCSDownloader
from dotenv import load_dotenv
import os
import logging
import sys
from scraper.scraper import clean_content, article_to_content, download_and_clean
import argparse
import dataclasses

logging.basicConfig(stream=sys.stdout, level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger()

load_dotenv()

project = os.getenv('GCLOUD_PROJECT')
bucket_name = os.getenv('GCS_BUCKET')


def clean_obj(object_name):
    downloader = GCSDownloader(gcp_project=project)
    res = downloader.download(bucket_name, object_name)
    if res is None:
        log.error("resource not found: %s/%s", bucket_name, object_name)
        return

    article = clean_content(res)
    if article is None:
        log.error("resource not cleaned: %s/%s", bucket_name, object_name)

    content = article_to_content(article)
    print(dataclasses.asdict(content))

    log.info("success: %s/%s", bucket_name, object_name)


def main():
    # todo
    # load csv with url, source id cleaned id
    # generate scraped id from url + source id
    # try to clean
    # try to clean from url directly
    # compare with data from cleaned id

    parser = argparse.ArgumentParser(description='newspaper checker')
    parser.add_argument('--obj', type=str,
                        help='gcs object id', required=False)
    parser.add_argument('--file', type=str,
                        help='file with list of objects (not implemented)')
    parser.add_argument('--url', type=str,
                        help='url to check')

    args = parser.parse_args()

    if args.url is not None:
        article = download_and_clean(args.url)
        print(article.title)
        print(article.text)
        print(article.is_valid_body())
        return

    if args.obj is not None:
        clean_obj(args.obj)  # "003503d1-e865-59d3-8607-9f99528d1740.html"
        return


if __name__ == "__main__":
    main()

