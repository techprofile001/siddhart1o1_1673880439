import pymysql
import tweepy
import re
from textblob import TextBlob
import string
import json
consumer_key = "kTqyyBLueMdmsD0TATV3GvPDL"
consumer_secret = "llGkhLGQ0nvlfK9PpBpC73BQEcSw7ss0G7Fsej4ljpS5HHs3Pk"
access_key = "882074476371877888-07eddl9Vmxpuf6WzhBEj5S3FnvXPbmE"
access_secret = "IvNxZ6UE8wPlNHdtgwBCJhKUWlkJz3kNZG3KVS8RhwBKa"
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_key, access_secret)
api = tweepy.API(auth, wait_on_rate_limit=True)


STOPWORDS = {'a', 'about', 'above', 'after', 'again', 'ain', 'all', 'am', 'an',
             'and', 'any', 'are', 'as', 'at', 'be', 'because', 'been', 'before',
             'being', 'below', 'between', 'both', 'by', 'can', 'd', 'did', 'do',
             'does', 'doing', 'down', 'during', 'each', 'few', 'for', 'from',
             'further', 'had', 'has', 'have', 'having', 'he', 'her', 'here',
             'hers', 'herself', 'him', 'himself', 'his', 'how', 'i', 'if', 'in',
             'into', 'is', 'it', 'its', 'itself', 'just', 'll', 'm', 'ma',
             'me', 'more', 'most', 'my', 'myself', 'now', 'o', 'of', 'on', 'once',
             'only', 'or', 'other', 'our', 'ours', 'ourselves', 'out', 'own', 're', 's', 'same', 'she', "shes", 'should', "shouldve", 'so', 'some', 'such',
             't', 'than', 'that', "thatll", 'the', 'their', 'theirs', 'them',
             'themselves', 'then', 'there', 'these', 'they', 'this', 'those',
             'through', 'to', 'too', 'under', 'until', 'up', 've', 'very', 'was',
             'we', 'were', 'what', 'when', 'where', 'which', 'while', 'who', 'whom',
             'why', 'will', 'with', 'won', 'y', 'you', "youd", "youll", "youre",
             "youve", 'your', 'yours', 'yourself', 'yourselves'}


def processTweet(tweet):
    tweet = re.sub(r'\&\w*;', '', tweet)
    tweet = re.sub('@[^\s]+', '', tweet)
    tweet = re.sub(r'\$\w*', '', tweet)
    tweet = tweet.lower()
    tweet = re.sub(r'https?:\/\/.*\/\w*', '', tweet)
    tweet = re.sub(r'#\w*', '', tweet)
    tweet = re.sub(r'[' + string.punctuation.replace('@', '') + ']+', ' ', tweet)
    tweet = re.sub(r'\b\w{1,2}\b', '', tweet)
    tweet = re.sub(r'\s\s+', ' ', tweet)
    tweet = tweet.lstrip(' ')
    tweet = ''.join(c for c in tweet if c <= '\uFFFF')
    tweet = ' '.join(re.sub("[^a-z0-9]", ' ', tweet).split())
    return tweet
# -----------------------------------------------------------------------------------------------


def ProcessFrequency(word_freq, tweet, query, sentiment):
    tweet = " ".join([word for word in str(
        tweet).split() if word not in STOPWORDS])
    tweet = tweet.split()
    query = query.lower()
    for word in tweet:
        if(word != query):
            if word in word_freq:
                word_freq[word][sentiment] += 1
            else:
                word_freq[word] = {"positive": 0, "negative": 0}
                word_freq[word][sentiment] = 1


def get_sentiment(tweet):
    processed_tweet = processTweet(tweet)
    analysis = TextBlob(processed_tweet)
    if analysis.sentiment.polarity > 0:
        return 'positive', analysis.sentiment.polarity, processed_tweet, analysis.sentiment.subjectivity
    elif analysis.sentiment.polarity == 0:
        return 'neutral', analysis.sentiment.polarity, processed_tweet, analysis.sentiment.subjectivity
    else:
        return 'negative', analysis.sentiment.polarity, processed_tweet, analysis.sentiment.subjectivity


def get_tweets(query, repeated_words, connection):

    hashtag = "#"+query+" -filter:retweets"
    # -----------------------------------------------------------------------------------------------
    with connection.cursor() as cursor:
        for item in tweepy.Cursor(api.search_tweets, q=hashtag, tweet_mode='extended', lang="en").items():
            tweet = item.full_text
            sentiment, polarity, processed_tweet, subjectivity = get_sentiment(tweet)
            SQL = """INSERT INTO `twitter_analytics`.`tweets_data`
                        (
                        `brand`,
                        `username`,
                        `profile_pic`,
                        `tweet_original`,
                        `tweet_processed`,
                        `tweet_date`,
                        `retweet_number`,
                        `likes`,
                        `followers`,
                        `subjectivity`,
                        `polarity`,
                        `sentiment`)
                        VALUES
                        (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
                        """
            cursor.execute(SQL, (query, item.user.screen_name, item.user.profile_image_url,
                                 tweet, processed_tweet, item.created_at, item.retweet_count, item.favorite_count, item.user.followers_count, subjectivity, polarity, sentiment))
            connection.commit()
            if(sentiment == "positive" or sentiment == "negative"):
                ProcessFrequency(
                    repeated_words, processed_tweet, query, sentiment)


def save_word_frequency(repeated_words, connection, query):
    query = query.lower()
    print(repeated_words)
    with connection.cursor() as cursor:
        for word in repeated_words:
            if(repeated_words[word]['positive'] != 0):
                SQL = f"""INSERT INTO `twitter_analytics`.`word_frequency`
                            (
                            `word`,
                            `count`,
                            `query`,
                            `sentiment`)
                            VALUES
                            ("{word}",{repeated_words[word]['positive']},"{query}","positive") on
                            DUPLICATE KEY UPDATE count = count + {repeated_words[word]['positive']};
                            """
                cursor.execute(SQL)
                connection.commit()
            if(repeated_words[word]['negative'] != 0):
                SQL = f"""INSERT INTO `twitter_analytics`.`word_frequency`
                            (
                            `word`,
                            `count`,
                            `query`,
                            `sentiment`)
                            VALUES
                            ("{word}",{repeated_words[word]['negative']},"{query}","negative") on
                            DUPLICATE KEY UPDATE count = count + {repeated_words[word]['negative']};
                            """
                cursor.execute(SQL)
                connection.commit()


def lambda_handler(event, context):
    connection = pymysql.connect(
        host="rds-mysql-tutorial.cw5il3f2mv55.ap-south-1.rds.amazonaws.com",
        user="admin",
        password="9908rajesh",
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor)
    hashtag = "natwest"
    repeated_words = {}
    tweets = get_tweets(hashtag, repeated_words, connection)
    save_word_frequency(repeated_words, connection, hashtag)
    return {
        'statusCode': 200,
        'body': json.dumps('success')
    }


lambda_handler("", "")
