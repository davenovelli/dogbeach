##
# This script is used to automatically tag all new articles using an existing tfidf model
##
import pandas as pd
import pickle
import pymysql
import re
from tqdm import tqdm

import os
abspath = os.path.abspath(__file__)
dname = os.path.dirname(abspath)
os.chdir(dname)

# Default mode (all, new, or update)
MODE = 'new'

# Load data from file instead of re-generating?
LOAD_DATA = False

# Should we regenerate the model, or just use the existing one?
REGENERATE_MODEL = False

# Model Generation paramters
MIN_ARTICLE_LENGTH = 1000
MAX_RECENT_ARTICLES = 31169
MAX_DF = .7
MIN_DF = .001
MAX_FEATURES = 21000
PICKLE_FILENAME = './count_vectorizer.pickle'

# A list of all Apostrophe-containing words that should be maintained during tokenization
APOSTROPHE_TERMS = pd.read_csv('./apostrophe_terms_to_save.txt').term.tolist()

# When generating tags in "update" mode, how many weeks back should we go to potentially change the tags?
UPDATE_HISTORY_WEEKS = 8


def parse_args():
    """ Specify the command line arguments to accept """
    import argparse
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('--mode', action='store', dest='mode', default=MODE,
                        help='Define the autotagging mode: all, new, update (default: new)')
    
    args = parser.parse_args()
    print(args)

    return args


def process_apostrophe_terms(df):
    """ There are several dozen terms (primarily Irish and Hawaiian names) that contain apostrophes that we don't
    want to be truncated during tokenization. For those specific terms, remove the apostrophes ahead of time
    """
    for term in APOSTROPHE_TERMS:
        new_term = term.replace("'", '')
        print(f"---------------------------------------------\nreplacing {term} with { new_term }")
        df['text'] = df.text.str.replace(term, new_term)
        print(df[df.text.str.contains(term)].head())
        print(df[df.text.str.contains(new_term)].head())
    print("\ndone.\n")
    return df


def pre_process(text):
    """ Cleanup and convert the text """
    if text:
        # strip
        text = text.strip()

        # lowercase
        text = text.lower()

        # Manual replacements to avoid wiping out certain keywords
        text = text.replace('j.o.b', 'jamie obrien')
    else:
        text = ''
    
    return text


def get_articles():
    """ Either, by querying or by reading a file that was previously saved """
    HOST = 'yew-review-test.c2usv7nad3n6.us-west-2.rds.amazonaws.com'
    USER = 'admin'
    PASSWORD = 'Y3wR3view9!'
    DATABASE = 'yewreview'

    print("Getting articles from the database")

    allArticlesQuery = """
 SELECT article_id, publisher, url, feedDate, tags, title, subtitle, text_content
   FROM articles a
  ORDER BY feedDate DESC
"""
    print(allArticlesQuery)
    
    try:
        connection = pymysql.connect(HOST, USER, PASSWORD, DATABASE)
        if LOAD_DATA:
            df = pd.read_csv('./latest_articles_to_model_df.csv')
        else:
            df = (
                pd.read_sql(allArticlesQuery, connection)
                .assign(text_content=lambda df: df.text_content.fillna(''))
                .assign(text=lambda df: df.title.fillna('') + ' ' + df.subtitle.fillna('') + ' ' + df.text_content)
                .assign(text=lambda df: df.text.apply(pre_process))
                .assign(textlen=lambda df: df.text.str.len())
                .query('textlen > @MIN_ARTICLE_LENGTH')
                .drop('textlen', axis=1)
                .sort_values('article_id', ascending=False)
            )

            # preserve approved terms containing apostrophes
            df = process_apostrophe_terms(df)
            
            # remove special characters and digits
            df = df.assign(text=lambda df: df.text.str.replace(r"(\\d|\\W)+", " "))

            df.to_csv('./latest_articles_to_model_df.csv', index=False)
        
        if MODE == 'new':
            taggedArticlesQuery = "SELECT DISTINCT articleId FROM ArticleTag WHERE `source` = 'tfidf' and `deletedAt` IS NULL"
            df = (
                pd.read_sql(taggedArticlesQuery, connection).astype(int).sort_values('articleId', ascending=False)
                .merge(df, how='right', left_on='articleId', right_on='article_id')
                .query('articleId.isna()')
                .drop('articleId', axis=1)
            )
        
        print(f"There are {df.shape[0]} total articles to work with")
    finally:
        connection.close()

    print(df)
    print("Done getting articles.")
    
    return df


## Stopwords ###################################
def get_stop_words():
    """ Load stop words from NLTK and include any manually assigned words to skip """
    import nltk
    nltk.download('stopwords')
    from nltk.corpus import stopwords

    additional_stopwords = pd.read_csv('./stopwords.txt').stopword.tolist()
    
    return frozenset(stopwords.words('english') + additional_stopwords)


def generate_model(df):
    """ Generate the model (CountVectorizer + TfidfTransformer), then save as a pickle """
    from sklearn.feature_extraction.text import CountVectorizer, TfidfTransformer

    # load a set of stop words
    stopword_set = get_stop_words()

    ## Generate the model ###################################
    # Only use articles with at least MIN_ARTICLE_LENGTH characters, and only use the most recent MAX_RECENT_ARTICLES of those
    train_df = (
        df
        .query('text.str.len() > @MIN_ARTICLE_LENGTH')
        .sort_values('feedDate')
        .iloc[:MAX_RECENT_ARTICLES]
    )

    cv = CountVectorizer(
        stop_words=stopword_set, 
        ngram_range=(1, 3), 
        max_df=MAX_DF, 
        min_df=MIN_DF, 
        max_features=MAX_FEATURES
    )
    word_count_vector = cv.fit_transform(train_df.text.tolist())

    tfidf_transformer = TfidfTransformer(smooth_idf=True, use_idf=True)
    tfidf_transformer.fit(word_count_vector)

    ## Save the CountVectorizer as a pickle
    pickle.dump((cv, tfidf_transformer), open(PICKLE_FILENAME, "wb"))


def sort_coo(coo_matrix):
    """ Sorts a ranked coordinate matrix format returned by tfidf.transform()"""
    tuples = zip(coo_matrix.col, coo_matrix.data)
    return sorted(tuples, key=lambda x: (x[1], x[0]), reverse=True)


def extract_topn_from_vector(feature_names, sorted_items, topn=10):
    """get the feature names and tf-idf score of top n items"""
    
    #use only topn items from vector
    sorted_items = sorted_items[:topn]

    score_vals = []
    feature_vals = []
    
    # word index and corresponding tf-idf score
    for idx, score in sorted_items:        
        #keep track of feature name and its corresponding score
        score_vals.append(round(score, 3))
        feature_vals.append(feature_names[idx])

    #create a tuples of feature,score
    #results = zip(feature_vals,score_vals)
    results = {}
    for idx in range(len(feature_vals)):
        results[feature_vals[idx]] = score_vals[idx]

    return results


def process_results(df_idf, cv, tfidf_transformer):
    """
    """
    # Figure out how many articles to use
    articles = df_idf.copy()
    if MODE == 'update':
        from datetime import datetime, timedelta

        # Grab articles from the past N weeks
        start_date = datetime.today() - timedelta(weeks=UPDATE_HISTORY_WEEKS)

        articles = articles[articles.feedDate >= start_date.strftime("%Y-%m-%d")]

    # you only needs to do this once, this is a mapping of index to phrase
    feature_names = cv.get_feature_names()
    # print(feature_names)
    
    frames = []
    for i in range(articles.shape[0]):
        # get the article_id
        article_id = articles.article_id.iloc[-i]
        
        # get the url
        url = articles.url.iloc[-i]

        # get the document that we want to extract keywords from
        doc = articles.text.iloc[-i]

        # print(article_id, url, doc)

        #generate tf-idf for the given document
        tf_idf_vector = tfidf_transformer.transform(cv.transform([doc]))
        # print(f"\ntf_idf_vector\n{tf_idf_vector}")
        
        #sort the tf-idf vectors by descending order of scores
        sorted_items = sort_coo(tf_idf_vector.tocoo())
        # print(f"\nsorted_items\n{sorted_items}")

        #extract only the top n; n here is 20
        keywords = extract_topn_from_vector(feature_names, sorted_items, 20)
        # print(keywords)
        
        keywords_df = (
            pd.DataFrame(keywords.items(), columns=['keyword', 'tfidf_weight'])
            .assign(article_id=article_id)
            .assign(url=url)
            .assign(rank=lambda df: df.groupby(['url']).cumcount() + 1)
        )
        # print(f"keywords_df\n{keywords_df}")
        
        frames += [keywords_df]
    
    df = pd.concat(frames, ignore_index=True)
    # print(df)
    
    return df


def send_tags(tags):
    """ """
    import json
    import requests

    # What is the API endpoint
    CREATE_ENDPOINT = "http://localhost:8081/tag"

    for tag in tqdm(tags):
        header = { "Content-Type": "application/json" }
        json_data = json.dumps(tag, default=str)
        print(json_data)
        
        r = requests.post(CREATE_ENDPOINT, headers=header, data=json_data)
        try:
            r.raise_for_status()
        except Exception as ex:
            print(f"There was a {type(ex)} error while creating tags for article {tag['articleId']}:...\n{r.json()}")


def persist_tags(tags_df):
    """ Save tags to database through the REST API """
    # API is designed to handle one article at a time. We've got a lot. possibly want to batch in large batches to save round trip
    print(f"\nFound {len(tags_df.article_id.unique())} articles to autotag")

    tags_df = tags_df.drop(['url', 'rank'], axis=1)
    # print(tags_df)

    results = []
    for articleId in tqdm(tags_df.article_id.unique().tolist()):
        result = {
            'articleId': articleId,
            'userId': 5,
            'tags': []
        }
        for tagrow in tags_df.query('article_id == @articleId').itertuples():
            result['tags'] += [{'name': tagrow.keyword, 'weight': tagrow.tfidf_weight}]
        
        results += [result]

        if len(results) == 100:
            # Send the results to the API one at a time
            print("\nCreating tags with API...")
            send_tags(results)
            results = []
    
    # And once more to clear the stragglers
    if len(results) > 0:
        print("\nCreating tags with API...")
        send_tags(results)
        results = []


def generate_tags(df):
    """ Using the trained tagging model, tag whatever articles need to be tagged """
    # Load the pickled CountVectorizer and TFIDF Transformer
    cv, tfidf_transformer = pickle.load(open(PICKLE_FILENAME, "rb"))

    article_tags = process_results(df, cv, tfidf_transformer)
    article_tags.to_csv(f'./article_tags_{MODE}.csv', index=False)
    # article_tags = pd.read_csv('./all_article_tags.csv')    
    print(article_tags)

    print("Done generating tags.")

    # Send the articles to the API
    persist_tags(article_tags)


if __name__ == '__main__':    
    args = parse_args()
    if args.mode:
        MODE = args.mode
    print(f"running in {MODE} mode...\n")
    
    # Go grab whatever articles are needed for the mode we're in
    df = get_articles()
    
    # If the model should be regenerated before scoring, then do that now
    if MODE in ['all', 'update'] and REGENERATE_MODEL:
        print("Regenerating the Tfidf model...")
        generate_model(df)
    
    # Score all the articles
    print("Generating tags from article text...")
    generate_tags(df)

    print("Autotagging completed")
