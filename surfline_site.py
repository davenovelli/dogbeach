import os
import sys
import json
import yaml
import pprint
import logging
import requests

from retry import retry
from pathlib import Path
from bs4 import BeautifulSoup
from time import sleep, strftime
from dateutil.parser import parse
from playwright.sync_api import sync_playwright, Error, TimeoutError

from dogbeach import doglog
_logger = None

##################################### Config

with open("config.yml", "r") as ymlfile:
    config = yaml.load(ymlfile, Loader=yaml.FullLoader)

PUBLISHER = 'surfline.com'
BASE_URL = config[PUBLISHER]['base_url']
LIMIT = config[PUBLISHER]['limit']
PRODUCTION = config[PUBLISHER]['production']

if PRODUCTION:
    os.chdir(os.path.dirname(sys.argv[0]))

# What is the API endpoint
REST_API_PROTOCOL = config['common']['rest_api']['protocol']
REST_API_HOST = config['common']['rest_api']['host']
REST_API_PORT = config['common']['rest_api']['port']
REST_API_URL = f"{REST_API_PROTOCOL}://{REST_API_HOST}:{REST_API_PORT}"
CREATE_ENDPOINT = f"{REST_API_URL}/article"
PUBLISHER_ARTICLES_ENDPOINT = f"{REST_API_URL}/articleUrlsByPublisher?publisher={PUBLISHER}"

# UserID and BrowswerID are required fields for creating articles, this User is the ID tied to the system account
SYSTEM_USER_ID = config['common']['system_user_id']

# This is the "blank" UUID
SCRAPER_BROWSER_ID = config['common']['browser_id']

# Are we scraping full history, or only new articles?
NEW_ONLY = config[PUBLISHER]['new_only']

# Maximum number of empty pages to load before quitting
MAX_EMPTY_PAGES = config[PUBLISHER]['max_empty_pages']

# User Agent to use for the requests
AGENT = config['common']['agent']

##################################### Globals

# A list of all the articles that have been scraped already, so we don't duplicate our efforts
already_scraped = set()

##################################### Logging
def get_logger():
    """ Initialize and/or return existing logger object

    :return: a DogLog logger object
    """
    global _logger
    if _logger is None:
        logfile = Path(os.path.dirname(os.path.realpath("__file__"))) / f"log/{PUBLISHER}_site.log"
        _logger = doglog.setup_logger(f'{PUBLISHER}_site', logfile, clevel=logging.DEBUG)
    return _logger

##################################### Helper Functions

def str_list(L: list) -> str:
    if len(L):
        l = str(list(dict.fromkeys(L))) # remove duplicates
    else:
        l = []
    return l

# def abort_or_continue(route, request):
#     if request.resource_type in ['document']:
#         route.continue_()
#     else:
#         route.abort()

def load_already_scraped_articles():
    """ Query the database for all articles that have already been scraped

    :return: a list of urls of articles that have already been scraped
    """
    global already_scraped

    r = requests.get(PUBLISHER_ARTICLES_ENDPOINT)
    urls_json = r.json()

    already_scraped = set([x['url'] for x in urls_json])

    get_logger().debug("Found {} articles already scraped".format(len(already_scraped)))


################################################################################ Scraping

def create_article(article):
    """
    Push this article to the database through the REST API

    :param articles:
    :return:
    """
    # Add some common fields
    article['userId'] = SYSTEM_USER_ID
    article['browserId'] = SCRAPER_BROWSER_ID
    article['publisher'] = PUBLISHER
    get_logger().debug("Writing article to RDS...\n{}".format(article))

    header = { "Content-Type": "application/json" }
    json_data = json.dumps(article, default=str)
    r = requests.post(CREATE_ENDPOINT, headers=header, data=json_data)
    get_logger().debug("\n\n=================================================================================\n\n")

    try:
        r.raise_for_status()
    except Exception as ex:
        get_logger().error(f"There was a {type(ex)} error while creating article {article['url']}:...\n{r.json()}")


@retry(Error, tries=6, delay=3, backoff=1.4, max_delay=30)
def extract_article(post, categories):
    article_id = post["id"]
    permalink = post["permalink"]

    get_logger().info(permalink)

    url = f'https://www.surfline.com/surf-news/{article_id}'
    r = requests.get(url, timeout=None)
    status_code = r.status_code

    if status_code == 200:
        soup = BeautifulSoup(r.text, "lxml")

        if post["media"]["type"] == "image":
            thumbnail = post["media"]["feed1x"]
        else:
            thumbnail = ""

        author_name = soup.select("div.sl-editorial-author__details__name")[0].get_text() # Surfline

        article_video = [v.find("iframe")["src"] for v in soup.select(".video-wrap") if v.find("iframe") is not None] # ["https://www.youtube.com/embed/nF2y6MjpOQ4?feature=oembed"]

        content = ". ".join([p.get_text(strip=True) for p in soup.select("p.p1") if len(p.get_text(strip=True)) > 0]).replace('..', '.') # or "\n".join(...)

        series = [s["name"] for s in post["series"]]
        atags = [a["href"].split("/")[-1] for a in soup.select("ul.sl-article-tags")[0].select("a")]

        tags = categories + series + atags
        tags = [t.lower() for t in tags]
        tags = list(dict.fromkeys(tags)) # remove duplicates

        article_json = {
            'permalink': permalink,
            'createdAt': post["createdAt"],
            'category': categories[0],
            'tags': tags,
            'title': post["title"],
            'subtitle' : post["subtitle"],
            'thumb': thumbnail,
            'article_video': article_video,
            'author_name': author_name,
            'text_content': content,
        }
        get_logger().debug(pprint.pformat(article_json, sort_dicts=False, width=200))
        return article_json
    else:
        get_logger().error(f"Error: {status} status retrieving page")
        return None

def scrape():
    """ Main function driving the scraping process
    """

    offset = 0

    while(1):

        r = requests.get(f'https://www.surfline.com/wp-json/sl/v1/taxonomy/posts/category?limit={LIMIT}&offset={offset}', timeout=None) # timeout=None # for slowest sites, most stable
        data = r.json()

        if data != None:
            posts = data["posts"]

            for i in range(len(posts)): # = limit for all the iterations, except last one
                post = posts[i]

                premium = post["premium"]
                categories = [c["name"] for c in post["categories"]]
                category = categories[0] # 1st category

                if premium != True:
                    for category in categories:
                        if category in ["Español","Português","Premium"]:
                            break
                        elif category == categories[-1]:
                            article = extract_article(post, categories)
                            if PRODUCTION:
                                if article is None:
                                    continue
                                create_article(article)
                                sleep(3)
        else:
            return
        offset += LIMIT
        print(f"\noffset: {offset}\n", flush=True)

if __name__ == '__main__':
    if PRODUCTION:
        # Query all the urls already scraped for this publisher
        load_already_scraped_articles()

    # Extract and save any new articles
    scrape()

    get_logger().info("\nDone.")
