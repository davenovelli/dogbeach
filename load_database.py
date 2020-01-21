import os
import json
import numpy as np
import pandas as pd

from datetime import datetime
from sqlalchemy import create_engine

np.set_printoptions(linewidth=100000)
pd.set_option('display.width', None)
pd.set_option('display.max_columns', 1000)
pd.set_option('display.max_colwidth', 500)

user = 'admin'
passw = 'Y3wR3view9!'
host = 'yew-review-test.c2usv7nad3n6.us-west-2.rds.amazonaws.com'
port = 3306
database = 'yewreview'
table = 'articles'

engine = create_engine('mysql+pymysql://{}:{}@{}:{}/{}'.format(user, passw, host, port, database), encoding='utf8', convert_unicode=True)


def load_stabmag():
    records = []
    stabmag_articles = os.listdir('./data/article_json/stabmag')
    for article in stabmag_articles:
        if article.endswith('.json'):
            path = './data/article_json/stabmag/{}'.format(article)
            print(article)

            with open(path) as f:
                line = f.readline()
                print(line)

            article_json = json.loads(line.replace("\n", "\\n"))
            # article_json['scrape_date'] = datetime.fromtimestamp(os.stat(path).st_birthtime).strftime('%Y-%m-%d')
            print(article_json)

            records += [article_json]
            print('\n\n')

    columns = ['slug', 'published', 'title', 'subtitle', 'content', 'author']
    df = (
        pd.DataFrame(records)[columns]
        .rename(columns={
            'slug': 'uri',
            'published': 'publish_date',
            'author': 'author_name'
        })
        .drop_duplicates('uri')
    )
    df.uri = df.uri.map(lambda x: 'https://stabmag.com/news/{}'.format(x))
    print(df.head())
    print(df.dtypes)

    records = []
    with open('./data/stabmag_articles.json') as f:
        for line in f:
            # This is a horrible hack to work around the newlines in the article text. Can't figure out any other solution
            records += [json.loads(line.replace("\n", "\\n")[:-2])]

    df2 = (
        pd.DataFrame(records)[['uri', 'publish_date', 'title', 'subtitle', 'thumb']]
          .drop_duplicates('uri')
    )
    print(df2)
    print(df2.dtypes)

    df3 = df2.merge(df, how='outer', on=['uri', 'publish_date', 'title', 'subtitle']).sort_values('publish_date')
    df3['publisher'] = 'stabmag'
    df3.publish_date = pd.to_datetime(df3.publish_date)

    columns = ['uri', 'publisher', 'publish_date', 'title', 'subtitle', 'author_name', 'thumb', 'content']
    print(df3[columns].head(3))
    print(df3.shape)
    print(df3.dtypes)

    return df3



def load_theinertia():
    print("start...")
    records = []
    inertia_articles = os.listdir('./data/article_json/theinertia')
    for article in inertia_articles:
        if article.endswith('.json'):
            path = './data/article_json/theinertia/{}'.format(article)
            print(article)

            with open(path) as f:
                line = f.readline()
                print(line)

            article_json = json.loads(line.replace("\n", "\\n"))
            article_json['scrape_date'] = datetime.fromtimestamp(os.stat(path).st_birthtime).strftime('%Y-%m-%d')
            print(article_json)

            records += [article_json]
            print('\n\n')

    columns = ['slug', 'published', 'scrape_date', 'category', 'title', 'article_type', 'article_photo',
               'article_caption', 'article_video', 'article_insta', 'content', 'author', 'author_type', 'author_url',
               'fblikes', 'twlikes']
    df = (
        pd.DataFrame(records)[columns]
            .rename(columns={
                'slug': 'uri',
                'published': 'publish_date',
                'author': 'author_name'
            })
            .drop_duplicates('uri')
    )
    df.uri = df.uri.map(lambda x: 'https://www.theinertia.com/{}'.format(x))
    df.loc[df.fblikes == 'like', 'fblikes'] = -1
    print(df.head())
    print(df.dtypes)

    # The thumbs are all in a csv **looks up towards the heavens in annoyance** so load and merge those in...
    df2 = pd.read_csv('data/theinertia_articles.csv').rename(columns={'url': 'uri'})
    df2.uri = df2.uri.map(lambda x: "https://www.theinertia.com/{}".format(x))
    # For some reason in late 2019 the scraper stopped working on the thumbnails, drop them, we'll rescrape
    df2 = df2[df2.thumb.str.lower() != 'none']
    df2.thumb = df2.thumb.map(lambda x: x[2:])
    print(df2.head())
    print(df2.dtypes)

    df3 = df2[['uri', 'thumb']].merge(df, how='inner').sort_values('publish_date', ascending=False)
    df3['publisher'] = 'theinertia'
    # Fix the inconsistent datetime format (why does that exist? Probably need to fix that)
    df3.loc[df3.publish_date.str.contains('UTC'), 'publish_date'] = df3.publish_date.map(lambda x: x.replace('UTC', ' ')[:-1])
    df3.loc[df3.publish_date.str.contains('GMT-0700'), 'publish_date'] = df3.publish_date.map(lambda x: x.replace('GMT-0700', ' ')[:-6])
    df3.publish_date = pd.to_datetime(df3.publish_date)
    df3.sort_values('publish_date', ascending=False, inplace=True)

    print(df3.shape)
    print(df3.dtypes)
    print(df3[['uri', 'thumb', 'publish_date', 'author_name']].head())

    return df3


if __name__ == '__main__':
    # df_stabmag = load_stabmag()
    # print(df_stabmag.dtypes)

    df_inertia = load_theinertia()
    print(df_inertia.dtypes)

    cols = ['uri', 'publisher', 'publish_date', 'scrape_date', 'category', 'title', 'subtitle', 'thumb', 'content',
            'article_type', 'article_photo', 'article_caption', 'article_video', 'article_insta',
            'author_name', 'author_type', 'author_url',
            'fblikes', 'twlikes']

    ########################
    # When importing both
    # df = (
    #     pd.concat([df_stabmag, df_inertia], sort=False)[cols]
    #         .sort_values('publish_date')
    #         .reset_index(drop=True)
    # )

    ########################
    # When importing just The Intertia
    df = df_inertia.sort_values('publish_date', ascending=False).reset_index(drop=True)

    df.publish_date = pd.to_datetime(df.publish_date)
    df.scrape_date = pd.to_datetime(df.scrape_date)
    df.fblikes = df.fblikes.map(lambda x: -1 if pd.isnull(x) else int(x))
    df.twlikes = df.twlikes.map(lambda x: -1 if (pd.isnull(x) or x == 'null') else int(x))

    df.drop_duplicates(inplace=True)

    print(df.head())
    print(df.shape)
    print(df.dtypes)

    df.to_sql(name=table, con=engine, if_exists='append', index=False)
