"""Pure parsers for The Inertia's category cards and current article layout."""
from urllib.parse import urljoin, urlsplit
from bs4 import BeautifulSoup

ORIGIN = 'https://www.theinertia.com/'


def image_url(image):
    if image is None:
        return None
    source = image.get('data-src') or image.get('src')
    if not source:
        return None
    url = urljoin(ORIGIN, source)
    return url if urlsplit(url).scheme in ('http', 'https') else None


def extract_cards(category, source):
    soup = BeautifulSoup(source, 'html.parser')
    cards = soup.select('article.card, div.inertia-item, div.item')
    articles = []
    seen = set()
    for card in cards:
        link = card.select_one('a.card__link[href]') or card.find('a', href=True)
        if not link:
            continue
        url = urljoin(ORIGIN, link['href']).rstrip('/')
        if urlsplit(url).hostname != 'www.theinertia.com' or url in seen:
            continue
        seen.add(url)
        articles.append({'url': url, 'category': category, 'thumb': image_url(card.find('img'))})
    return articles


def current_article(soup):
    article = soup.select_one('main#primary > article.post')
    if article is None:
        return None
    title = article.select_one('h1.entry-title')
    date = article.select_one('time.entry-meta__date[datetime]')
    body = article.select_one('.entry-content')
    if title is None or date is None or body is None:
        return None
    category = article.select_one('.entry-header a[rel="tag"]')
    author = article.select_one('a.entry-author')
    author_type = article.select_one('.entry-meta__community')
    return article, {
        'title': title.get_text(' ', strip=True),
        'publishedAt': date['datetime'][:10],
        'category': category.get_text(' ', strip=True) if category else 'surf',
        'author_name': author.get_text(' ', strip=True) if author else None,
        'author_url': urljoin(ORIGIN, author['href']) if author else None,
        'author_type': author_type.get_text(' ', strip=True) if author_type else None,
    }
