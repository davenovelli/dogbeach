import os
import sys
import json
import atexit
import logging
import pandas as pd
from pathlib import Path
from util import doglog
from util.dogdriver import DogDriver
from bs4 import BeautifulSoup

ARTICLE_TEMPLATE = 'https://www.stabmag.com/news/{}'

_logger = None
_driver = None


def get_logger():
    """

    :return:
    """
    global _logger
    if _logger is None:
        logfile = Path(os.path.dirname(os.path.realpath(__file__))) / "log/stabmag_articles.log"
        _logger = doglog.setup_logger("stabmag_article", logfile, clevel=logging.INFO)
    return _logger


def get_driver():
    """

    :return:
    """
    global _driver
    if _driver is None:
        _driver = DogDriver(get_logger())
    return _driver


def save_article_to_json(tup):
    """ Take a tuple containing all the useful attributes from an article and save it to it's own json file

    :param tup:
    :return:
    """
    # First we'll store the tuple as a dictionary:
    article = {
        'slug': tup[0],
        'title': tup[1],
        'subtitle': tup[2],
        'published': tup[3],
        'author': tup[4],
        'content': tup[5]
    }

    with open("data/article_json/{}.json".format(tup[0]), 'w') as f:
        json.dump(article, f)


def scrape_article(slug):
    """ For the provided slug, load the article and find whatever data is available

    :param driver:
    :param url:
    :return:
    """
    # Load the article and wait for it to load
    url = ARTICLE_TEMPLATE.format(slug)

    if not get_driver().get_url(url):
        # We'll just have to skip this slug, can't load it even with retries
        return

    source = get_driver().driver.page_source.replace('\u201c', '"').replace('\u201d', '"').replace('\u2019', "'")
    soup = BeautifulSoup(source, "html.parser")
    article_soup = soup.find("article", class_="container")

    # Get the title and subtitle
    title = article_soup.find("h1").string.strip()
    subtitle_div = article_soup.find("div", class_="featured-summary")
    subtitle = '' if subtitle_div is None else subtitle_div.get_text().strip()

    # The post date should always be available
    post_meta_div = article_soup.find("div", class_="blog-post-meta")
    post_date_div = post_meta_div.find_all("div")[0]
    post_date = post_date_div.find_all("span")[1].find("a").get('href').replace("/news/archive/", "").replace("/", "-")

    # Get the content
    content_div = article_soup.find("div", {"class": "content editable"})
    content = content_div.get_text().strip()
    content = content.replace(u'\xa0', u' ')

    # There may or may not be a listed author
    post_author = ''
    if len(post_meta_div.find_all("div")) > 1:
        post_author_divs = post_meta_div.find_all("div")
        post_author_div = post_author_divs[1]
        post_author_spans = post_author_div.find_all("span")
        post_author_span = post_author_spans[1]

        # Sometimes the author is "STAB" or has no link
        if len(post_author_span.find_all("a")) > 0:
            post_author = post_author_span.find("a").get('href').replace("/news/profile/", "")
        elif len(content_div.find_all(text="Story by")) > 0:
            author_element = content_div.find_all(text="Story by")[0]
            post_author = author_element.get_text().replace('Story by', '')
            content.replace(author_element.get_text(), '')

    tup = (slug, title, subtitle, post_date, post_author, content)
    save_article_to_json(tup)


def scrape_articles():
    """ Load each article url and extract all useful info from it, saving each article separately to a json file

    :return:
    """
    # Read in the list of all articles from CSV
    articles_list_df = pd.read_csv('data/stabmag_articles.csv')
    articles_list_df = articles_list_df[pd.notnull(articles_list_df.url)]
    articles_list = articles_list_df.url.astype(str).apply(lambda x: x.replace("/news/", "").replace("/", "")).unique()

    get_logger().debug("{} articles have been found: \n{}".format(len(articles_list), articles_list))

    # Then read in the list of article slugs that have been scraped already
    slugs = []
    for filename in os.listdir("data/article_json"):
        slug = filename.split('.')[0]
        slugs += [slug]
    get_logger().debug("{} articles have been scraped: \n{}".format(len(slugs), "\n".join(slugs)))

    # Find any articles whose content hasn't been scraped yet and scrape it, adding it to the csv
    slugs = list(set(articles_list) - set(slugs))
    get_logger().info("{} slugs haven't been crawled yet: \n{}".format(len(slugs), slugs))

    for slug in slugs:
        get_logger().info("Scraping slug: {}".format(slug))
        scrape_article(slug)


def cleanup():
    get_driver().driver.quit()


if __name__ == "__main__":
    atexit.register(cleanup)

    # To get the script to see files in this directory (including chromedriver)
    os.chdir(os.path.dirname(sys.argv[0]))

    scrape_articles()
