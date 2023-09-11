from copy import deepcopy

from trafilatura import fetch_url, extract
from trafilatura.settings import DEFAULT_CONFIG

import logging
import sys

logging.basicConfig(stream=sys.stdout, level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger()

zero_config = deepcopy(DEFAULT_CONFIG)
zero_config['DEFAULT']['MIN_OUTPUT_SIZE'] = '0'
zero_config['DEFAULT']['MIN_EXTRACTED_SIZE'] = '0'


def clean_content(data):
    return extract(data,
                   config=zero_config,
                   output_format='xml',
                   include_formatting=True,
                   include_links=True)


def clean_content_url(url):
    resp = fetch_url(url, config=zero_config)
    # log.info("resp: %s", resp)

    if resp is None:
        log.error("failed to fetch url: %s", url)
        return

    # if resp.status != "200":
    #     log.error("invalid status: %s", status)
    #     return

    return extract(resp,
                   config=zero_config,
                   output_format='xml',
                   include_formatting=True,
                   include_links=True)