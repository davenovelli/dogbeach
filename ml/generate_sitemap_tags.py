import os
import sys
import pandas as pd

##################################### Config
os.chdir(os.path.dirname(sys.argv[0]))

df = pd.read_csv('./published_tags.csv')

sitemap_template = """<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
{}
</urlset>
"""

url_template = """  <url>
    <loc>https://yewreview.com/tag/{}</loc>
    <changefreq>weekly</changefreq>
  </url>"""

formatted = "\n".join([url_template.format(tag) for tag in df.tag.values])
sitemap = sitemap_template.format(formatted)

print(df)
print(sitemap)

with open('sitemap.xml', 'w', encoding='utf-8') as f:
    f.write(sitemap)