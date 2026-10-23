import unittest
from bs4 import BeautifulSoup
from dogbeach.inertia import extract_cards, current_article


class InertiaTests(unittest.TestCase):
    def test_current_cards_preserve_publisher_https_and_deduplicate(self):
        card = '''<article class="card"><a class="card__link" href="https://www.theinertia.com/surf/story/">Story</a>
          <img src="https://www.theinertia.com/wp-content/uploads/2026/10/story-700x700.webp"></article>'''
        rows = extract_cards('surf', card + card)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]['url'], 'https://www.theinertia.com/surf/story')
        self.assertEqual(rows[0]['thumb'], 'https://www.theinertia.com/wp-content/uploads/2026/10/story-700x700.webp')

    def test_legacy_lazy_and_relative_images(self):
        for css in ('item', 'inertia-item'):
            rows = extract_cards('surf', f'''<div class="{css}"><a href="/surf/story/">Story</a>
              <img src="data:image/gif;base64,placeholder" data-src="//www.theinertia.com/photo.jpg"></div>''')
            self.assertEqual(rows[0]['thumb'], 'https://www.theinertia.com/photo.jpg')
        rows = extract_cards('surf', '<article class="card"><a href="/surf/story/">Story</a><img src="/photo.jpg"></article>')
        self.assertEqual(rows[0]['thumb'], 'https://www.theinertia.com/photo.jpg')

    def test_current_article_fields_and_optional_author(self):
        soup = BeautifulSoup('''<main id="primary"><article class="post"><header class="entry-header">
          <a rel="tag">Surf</a><h1 class="entry-title">A real headline</h1>
          <a class="entry-author" href="/author/name/">A Writer</a><span class="entry-meta__community">Staff</span>
          <time class="entry-meta__date" datetime="2026-10-22T11:49:50-07:00"></time></header>
          <div class="entry-content"><p>Original body</p></div></article></main>''', 'html.parser')
        article, fields = current_article(soup)
        self.assertEqual(fields, {'title': 'A real headline', 'publishedAt': '2026-10-22', 'category': 'Surf',
            'author_name': 'A Writer', 'author_url': 'https://www.theinertia.com/author/name/', 'author_type': 'Staff'})
        self.assertEqual(article.select_one('.entry-content').get_text(), 'Original body')
        soup.select_one('.entry-author').decompose()
        self.assertIsNone(current_article(soup)[1]['author_name'])
        soup.select_one('.entry-content').decompose()
        self.assertIsNone(current_article(soup))

    def test_non_articles_and_executable_images_are_not_accepted(self):
        self.assertEqual(extract_cards('surf', '<p>No posts</p>'), [])
        self.assertIsNone(current_article(BeautifulSoup('<main>Missing article</main>', 'html.parser')))
        rows = extract_cards('surf', '<article class="card"><a href="/surf/story/">Story</a><img src="javascript:bad()"></article>')
        self.assertIsNone(rows[0]['thumb'])
