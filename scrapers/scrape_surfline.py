import os
import re
import sys
import json
import yaml
import pprint
import logging
import requests
import pandas as pd

from retry import retry
from pathlib import Path
from requests import Timeout
from bs4 import BeautifulSoup
from time import sleep, strftime
from dateutil.parser import parse
from scrapy.selector import Selector
from playwright.sync_api import sync_playwright


# Config
os.chdir(os.path.dirname(sys.argv[0]))
with open("../config.yml", "r") as ymlfile:
    config = yaml.load(ymlfile, Loader=yaml.FullLoader)

# Import Doglog
sys.path.append('..')
from dogbeach import doglog
_logger = None


PUBLISHER = 'surfline.com'
BASE_URL = config[PUBLISHER]['base_url']
LIMIT = config[PUBLISHER]['limit']

# What is the API endpoint
REST_API_PROTOCOL = config['common']['rest_api']['protocol']
REST_API_HOST = config['common']['rest_api']['host']
REST_API_PORT = config['common']['rest_api']['port']
REST_API_URL = f"{REST_API_PROTOCOL}://{REST_API_HOST}:{REST_API_PORT}"
CREATE_ENDPOINT = f"{REST_API_URL}/article"
PUBLISHER_ARTICLES_ENDPOINT = f"{REST_API_URL}/articleUrlsByPublisher?publisher={PUBLISHER}"

# User Agent to use for the requests
AGENT = config['common']['agent']

# UserID and BrowswerID are required fields for creating articles, this User is the ID tied to the system account
SYSTEM_USER_ID = config['common']['system_user_id']

# This is the "blank" UUID
SCRAPER_BROWSER_ID = config['common']['browser_id']

# Maximum number of empty pages to load before quitting
MAX_EMPTY_PAGES = config[PUBLISHER]['max_empty_pages']

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

def parse_tags(tags_list: list) -> str:
    print(f"Starting with {len(tags_list)} raw_tags: {tags_list}")
    if len(tags_list):
        # Convert all the tags to lower case
        tags = [t.lower() for t in tags_list]
        
        # Drop duplicates and sort
        tags = sorted(list(set(tags)))

        # Convert from an array to a string
        tags = ', '.join(tags)
    else:
        tags = ''
    
    print(f"Extracted {len(tags)} tags: {tags}")
    return tags


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

    header = { "Content-Type": "application/json" }
    json_data = json.dumps(article, default=str)
    r = requests.post(CREATE_ENDPOINT, headers=header, data=json_data)
    try:
        r.raise_for_status()
    except Exception as ex:
        get_logger().error(f"There was a {type(ex)} error while creating article {article['url']}:...\n{r.json()}")

    get_logger().debug("\n\n=================================================================================\n\n")

################################################################################

@retry(Timeout, tries=6, delay=3, backoff=1.4, max_delay=30)
def extract_article(page, post):
    """
    :param page: the playwright page object used to load the url
    :param post: a dict containing the content already extracted from the category page
    :return: a dict containing all the data extracted from the page
    """
    permalink = post["permalink"].replace('#038;', '')
    get_logger().info(f"extracting: {permalink}")

    r = page.goto(permalink)
    sleep(2)

    if r.status == 200:
        soup = BeautifulSoup(doglog.clean_unicode(r.text()), "lxml")

        if post["media"]["type"] == "image":
            thumbnail = post["media"]["feed1x"].replace('https://', '')
        else:
            thumbnail = ""

        if soup.select("div.sl-editorial-author__details__name"):
            author_name = soup.select("div.sl-editorial-author__details__name")[0].get_text() # Surfline
        else:
            author_name = ""

        article_video = [v.find("iframe")["src"] for v in soup.select(".video-wrap") if v.find("iframe") is not None] # ["https://www.youtube.com/embed/nF2y6MjpOQ4?feature=oembed"]

        if len(soup.select("div#sl-editorial-article-body")) > 0:
            # We have a standard page, pull out the text in the normal div
            content = ". ".join([p.get_text(separator="\n", strip=True) for p in soup.select("div#sl-editorial-article-body")[0].select("p.p1") if len(p.get_text(strip=True)) > 0]).replace('..', '.') # or "\n".join(...)
            if not len(content):
                content = ". ".join([p.get_text(separator="\n", strip=True) for p in soup.select("div#sl-editorial-article-body")[0].select("p") if len(p.get_text(strip=True)) > 0]).replace('..', '.')
        elif len(soup.findAll("header", {"data-testid": "travel-zone-navbar"})) > 0:
            # We have a special "travel guide" page, extracting the content on this one will take some extra work
            sections = [
                soup.find("section", {"data-testid": "travel-hero"}),
                soup.find("section", {"data-testid": "surf-zone"}),
                soup.find("section", {"data-testid": "travel-interview"}),
                soup.find("section", {"data-testid": "travel-zone-when-to-score"}),
                soup.find("section", {"data-testid": "travel-local-knowledge"}),
                soup.find("section", {"data-testid": "travel-essentials"}),
                soup.find("section", {"data-testid": "spaghetti-time"})
            ]
            content = "\n\n".join([s.get_text(separator="\n", strip=True) for s in sections if s])

        # Build full tags list from the categories, series, and existing tags
        categories = [c["name"] for c in post["categories"]]
        series = [s["name"] for s in post["series"]]
        atags = [a["href"].split("/")[-1] for a in soup.select("ul.sl-article-tags")[0].select("a")] if soup.select("ul.sl-article-tags") else []
        tags = parse_tags(categories + series + atags)
        
        article_json = {
            'url': permalink,
            'publishedAt': post["createdAt"].replace(' ', 'T'),
            'category': post['category'],
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
        get_logger().error(f"Error: {r.status} status retrieving page: {permalink}")
        return None

def scrub_url(url):
    """ Remove any useless querystrings
    """
    print(url)
    url = url.replace('#038;', '')
    
    utm_regex_str = r'(\\?)utm[^&]*(?:&utm[^&]*)*&(?=(?!utm[^\s&=]*=)[^\s&=]+=)|\\?utm[^&]*(?:&utm[^&]*)*$|&utm[^&]*/g'
    utm_regex = re.compile(utm_regex_str, re.IGNORECASE)

    scrubbed = re.sub(utm_regex, r'\1', url).rstrip('?')
    
    print(scrubbed)
    return scrubbed

def abort_or_continue(route, request):
    if request.resource_type in ['document']:
        route.continue_()
    else:
        route.abort()

def scrape():
    """ Main function driving the scraping process
    """
    offset = config[PUBLISHER]['offset']

    df = pd.read_csv(f"../data/{PUBLISHER}/alltags_ordered.csv", header=None)
    ranked_categories = [row[0] for index,row in df.iterrows()]

    empty_pages = 0
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(user_agent=AGENT)
        page = context.new_page()
        page.route('**/*', lambda route, request: abort_or_continue(route, request))

        while(1):
            get_logger().debug(f"Grabbing next {LIMIT} articles starting at offset {offset}")
            url = f'https://www.surfline.com/wp-json/sl/v1/taxonomy/posts/category?limit={LIMIT}&offset={offset}'
            page.goto(url)
            sleep(2)

            source = doglog.clean_unicode(page.content())
            sel = Selector(text=source)
            json_str = sel.xpath("*//pre//text()").extract_first()
            data = json.loads(json_str)

            if data != None:
                posts = data["posts"]

                new_articles_found = 0
                for i in range(len(posts)): # = limit for all the iterations, except last one
                    post = posts[i]

                    # There appear to be promos from other sites (worldsurfleague.com is one I found) and we don't want to include that
                    if 'surfline.com' not in post['permalink']:
                        continue

                    if 'utm' in post['permalink']:
                        post['permalink'] = scrub_url(post['permalink'])
                    if post['permalink'] in already_scraped:
                        continue

                    premium = post["premium"]
                    categories = [c["name"] for c in post["categories"]]
                    series = [s["name"] for s in post["series"]]
                    tags = set(categories)
                    tags.update(set(series))

                    # Find the highest ranked tag that is present for this article
                    category = None
                    for cat in ranked_categories:
                        if cat in tags:
                            category = cat
                            break
                    
                    # If we didn't find any tag in the rankings, choose the first category
                    if category == None:
                        category = list(tags)[0]

                    # Set the category for the post
                    post['category'] = category

                    # If the article is premium or not in English then skip it
                    if premium == False and len(tags.intersection({"Español", "Português", "Premium"})) == 0:
                        article = extract_article(page, post)
                        if article is None:
                            continue

                        create_article(article)
                        new_articles_found += 1
            else:
                return
            
            # Keep track of if we should stop due to no new articles found...
            if new_articles_found > 1:
                empty_pages = 0
            else:
                empty_pages += 1
                if empty_pages >= MAX_EMPTY_PAGES:
                    get_logger().info("Max number of empty pages reached, quitting.")
                    return
            
            # Update to get the next page worth of articles
            offset += LIMIT

if __name__ == '__main__':
    # Query all the urls already scraped for this publisher
    load_already_scraped_articles()

    # Extract and save any new articles
    try:
        scrape()
    finally:
        # DbToCsv(db)
        get_logger().info("\nDone.")