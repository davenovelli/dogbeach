import re
import pandas as pd

import os
abspath = os.path.abspath(__file__)
dname = os.path.dirname(abspath)
os.chdir(dname)


ARTICLE_FILENAME = './latest_articles_to_model_df.csv'
APOSTROPHE_REGEX = r"(?P<term>[a-zA-Z]+'[a-rt-zA-RT-Z]+)"


df = pd.read_csv(ARTICLE_FILENAME)

alltermsdf = (
    df
    .text_content
    .str
    .extractall(APOSTROPHE_REGEX)
    .reset_index()
)
print(alltermsdf)

termsdf = (
    alltermsdf
    .assign(term=lambda df: df.term.str.lower())
    .groupby('term')
    .count()
    .match
    .reset_index()
    .rename(columns={'match': 'termCount'})
    .sort_values('termCount', ascending=False)
    .query('termCount > 5')
)
print(termsdf)

termsdf.to_csv('./apostrophe_terms.csv', index=False)