from newspaper import Article, ArticleException, Config
import logging
import sys
import re
from dataclasses import dataclass

logging.basicConfig(stream=sys.stdout, level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger()


@dataclass
class CleanedContent:
    body: list[str] = None  # array of paragraphs
    title: str = None
    authors: list[str] = None
    main_image: str = None
    images: list[str] = None
    canonical_link: str = None


def article_to_content(article):
    paragraphs = re.sub(r'\n\s*\n', '\n', article.text).splitlines()  # clean up extra whitespace and split by newline

    return CleanedContent(
        title=article.title,
        body=paragraphs,
        authors=article.authors,
        main_image=article.top_img,
        images=sorted(article.images),
        canonical_link=article.canonical_link,
    )


def valid_article(article):
    if not article.is_parsed:
        return False, "article not parsed"

    if article.text == '' or article.title == '':
        return False, "article body or title is missing"

    return True, ''


def download_and_clean(url):
    article = Article(url=url)
    article.download()
    article.parse()
    return article

def clean_content(data):
    config = Config()
    config.follow_meta_refresh = False
    config.keep_article_html = True
    config.ignored_images_suffix_list = ['(.*)\.ico']

    try:
        # need to provide some url, even when processing already downloaded content
        article = Article("", config=config)
        article.download(input_html=data)
        article.parse()

        return article
        # valid, err = valid_article(article)
        # if not valid:
        #     log.warning('failed to parse article: %s', err)
        #     return None
        #
        # return article_to_content(article)
    except ArticleException as err:
        log.warning('failed to process article %s', err)
        return None
