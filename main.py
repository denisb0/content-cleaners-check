from gcs.gcs import GCSDownloader
from dotenv import load_dotenv
import os
import logging
import sys
import argparse
import uuid
from cleaner_trf.cleaner import clean_content, clean_content_url
import csv
import json

logging.basicConfig(stream=sys.stdout, level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger()

load_dotenv()

project = os.getenv('GCLOUD_PROJECT')
scraped_bucket_name = os.getenv('GCS_SCRAPED_BUCKET')
cleaned_bucket_name = os.getenv('GCS_CLEANED_BUCKET')

downloader = GCSDownloader(gcp_project=project)


def save_scraped(data, file_name):
    log.info("saving scraped %s", file_name)

    with open(file_name, "wb") as binary_file:
        binary_file.write(data)


def save_processed_data(data_map, file_name):
    log.info("saving %s; %s; %s", data_map["id"], data_map["source_id"], data_map["url"])

    json_object = json.dumps(data_map, indent=4)

    # Writing to sample.json
    with open(file_name, "w") as outfile:
        outfile.write(json_object)


def get_processed_data(id, source_id, url, cleaned_id):
    log.info("getting %s; %s; %s", id, source_id, url)

    scraped_uid = scraped_uuid(url=url, source_id=source_id)

    result = {
        "id": id,
        "source_id": source_id,
        "url": url,
        "scraped_uid": scraped_uid
    }

    scraped_object_name = scraped_uid+".html"
    scraped_obj = downloader.download(scraped_bucket_name, scraped_object_name)
    if scraped_obj is None:
        log.error("resource not found: %s/%s", scraped_bucket_name, scraped_object_name)
    else:
        result["scraped_kb"] = round(len(scraped_obj)/1024, 2)
        result["scraped"] = "ok" #scraped_obj.decode()

    cleaned_obj = downloader.download(cleaned_bucket_name, cleaned_id)
    if cleaned_obj is None:
        log.error("resource not found: %s/%s", cleaned_bucket_name, cleaned_id)
    else:
        result["cleaned_kb"] = round(len(cleaned_obj)/1024, 2)
        result["cleaned"] = cleaned_obj.decode()

    return result, scraped_obj, cleaned_obj


def process_file(file_name):
    limit = 100
    # read file
    # for each entry make a map with id, source_id, url, scraped, cleaned
    with open(file_name) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        line_count = 0
        for row in csv_reader:
            d, scraped, cleaned = get_processed_data(row[0], row[1], row[2], row[3])
            if d is None:
                log.error("error getting data for %s", row)
                continue
            save_processed_data(d, row[0]+".json")
            if len(cleaned) < 1000:
                save_scraped(scraped, "scraped/"+d["scraped_uid"]+".html")
            line_count += 1
            if line_count >= limit:
                break
        print(f'Processed {line_count} lines.')


def scraped_uuid(url, source_id):
    name = source_id+":"+url
    return str(uuid.uuid5(uuid.UUID(int=0), name))


def main():
    # todo
    # load csv with url, source id cleaned id
    # generate scraped id from url + source id
    # try to clean
    # try to clean from url directly
    # compare with data from cleaned id

    parser = argparse.ArgumentParser(description='cleaner checker')
    parser.add_argument('--file', type=str,
                        help='file with list of objects (not implemented)')
    parser.add_argument('--url', type=str,
                        help='url to check')
    parser.add_argument('--source_id', type=str,
                        help='url to check')

    args = parser.parse_args()

    if args.file is not None:
        log.info("file: %s", args.file)
        process_file(args.file)
        return

    if args.url is not None and args.source_id is not None:
        # load scraped data and try to clean
        scraped_uid = scraped_uuid(args.url, args.source_id)
        log.info("url: %s, source_id: %s, uuid: %s", args.url, args.source_id, scraped_uuid(args.url, args.source_id))

        object_name = scraped_uid+".html"
        res = downloader.download(scraped_bucket_name, object_name)
        if res is None:
            log.error("resource not found: %s/%s", scraped_bucket_name, object_name)
            return
        # log.info("scraped:\n%s", res)

        with open("scraped.html", "wb") as binary_file:
            binary_file.write(res)

        log.info("====================")
        cleaned = clean_content(res)
        if cleaned is None:
            log.error("cleanup failed: %s/%s", args.url)
            return
        log.info("cleaned:\n%s", cleaned)
        return

    log.error("unexpected args combination: %s", args)


if __name__ == "__main__":
    main()
