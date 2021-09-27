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
os.chdir(os.path.dirname(sys.argv[0]))
with open("../config.yml", "r") as ymlfile:
    config = yaml.load(ymlfile, Loader=yaml.FullLoader)

PUBLISHER = 'magicseaweed.com'
BASE_URL = config[PUBLISHER]['base_url']

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
        logfile = Path(os.path.dirname(os.path.realpath("__file__"))) / f"../log/{PUBLISHER}_site.log"
        _logger = doglog.setup_logger(f'{PUBLISHER}_site', logfile, clevel=logging.DEBUG)
    return _logger

##################################### Helper Functions

def str_list(L: list) -> str:
    if len(L):
        l = str(list(dict.fromkeys(L))) # remove duplicates
    else:
        l = []
    return l

def abort_or_continue(route, request):
    if request.resource_type in ['document']:
        route.continue_()
    else:
        route.abort()

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
def extract_article(page, url):
    get_logger().info(url)
    
    global status
    def set_status(status_code):
        global status
        status = status_code

    page.on("response", lambda response: set_status(response.status))
    page.goto(url)
    if status == 200:
        publish_date = page.text_content("time")  # Ex: 10th February 2021
        publish_date = parse(publish_date).strftime('%Y-%m-%d')
        
        author_name = page.text_content(".media-body a")
        author_url = f'{BASE_URL}{page.query_selector(".media-body a").get_attribute("href")}'

        thumbnail = page.query_selector('meta[name="thumbnail"]').get_attribute("content")
        if "_SQUARE" in thumbnail[-7:]:
            thumbnail = thumbnail[:-7]
        
        if len(url.split("/"))>3:
            post_category = url.split("/")[3]
        else:
            post_category = ""

        title = page.title().replace(' - Magicseaweed', '')

        html = page.query_selector(".editorial-content").inner_html()
        soup = BeautifulSoup(html, "lxml")
        [s.extract() for s in soup('small')]
        content = ". ".join([p.get_text(strip=True) for p in soup.select("p") if len(p.get_text(strip=True)) > 0]).replace('..', '.') # or "\n".join(...)
        # get_logger().info(content)

        article_video = [v.find("iframe")["src"] for v in soup.select(".video") if v.find("iframe") is not None] # ["//www.youtube.com/embed/yCICYEGXdVg"]
        article_video = [v if "/" not in v[0] else f"https:{v}" for v in article_video] # ["https://www.youtube.com/embed/yCICYEGXdVg"]
        article_video = article_video

        article_insta = [a["href"] for a in soup.select("a[href]") if "https://www.instagram.com/" in a["href"]] # ["https://www.instagram.com/......."]
        article_insta = str_list(article_insta)

        article_json = {
            'url': url, 
            'publishedAt': publish_date, 
            'category': post_category,
            'title': title, 
            'thumb': thumbnail,
            'article_insta': article_insta, 
            'article_video': article_video, 
            'author_name': author_name, 
            'author_url': author_url,
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
    with sync_playwright() as p:
        get_logger().info(f"Start time: {strftime('%H:%M:%S')}\n")
        
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(user_agent=AGENT)
        page = context.new_page()
        page.route('**/*', lambda route, request: abort_or_continue(route, request))

        page_url = f"{BASE_URL}/news/features/?page=0"
        page.goto(page_url)
        
        last_page_num = int(page.query_selector("text=/.*Last.*/").get_attribute("href").split("/")[-2])
        
        empty_page_count = 0
        for page_n in range(1, last_page_num + 1):
            get_logger().info(f"\npage: {page_n} of {last_page_num}\n")

            if page_n > 1:
                page_url = f"https://magicseaweed.com/news/features/?page={page_n}"
                page.goto(page_url)

            loadmore_group = page.query_selector(".msw-js-loadmore-group")
            loadmore_links = loadmore_group.query_selector_all("a.editorial-item, a.msw-js-live-content")
            urls = [f'{BASE_URL}{a.get_attribute("href")}' for a in loadmore_links if "http://" not in a.get_attribute("href") and "www." not in a.get_attribute("href")]
            urls = [url for url in urls if url not in already_scraped]
            if len(urls) > 1:
                url_list = "\n".join(urls)
                get_logger().info(f"{len(urls)} new URLs to scrape:\n{url_list}")
                empty_page_count = 0
            else:
                empty_page_count += 1

                if empty_page_count == MAX_EMPTY_PAGES and NEW_ONLY:
                    get_logger().info("Max number of empty pages reached, quitting...")
                    break
                else:
                    continue

            for url in urls:
                article = extract_article(page, url)
                if article is None:
                    continue
                create_article(article)
                sleep(3)

        get_logger().info(f"End Time: {strftime('%H:%M:%S')}\n")
        browser.close()


if __name__ == '__main__':
    # Query all the urls already scraped for this publisher
    load_already_scraped_articles()

    # Extract and save any new articles
    scrape()

    get_logger().info("\nDone.")
