# This code is designed to run within a Google Cloud Function or from the command line.
# Google Cloud Function:
#   It expects a request with a message that contains a query and optionally a limit on the number of tweets to gather.
#   Example requests:
#     {"message":{"query":"Trump","limit":10}}
#     -- This will search for 10 twitters with the term "Trump".
#     {"message":{"query":["COVID","masks","opening schools"],"projectId":"bigdata-online-69660","topic":"tweet-query"}}
#     -- This will search for the default number of twitters mentioning "COVID", "masks", and "opening schools".
#     {"message":{"query":"Trump","limit":10,"bucket":"twitter-stream-outputs"}}
#     -- This will do the same search as the first example but will write the output to an existing bucket named twitter-stream-outputs.
#     {"message":{"query":["COVID vaccine"],"limit":10,"projectId":"datapipe-1","topic":"twitter_tweets","userTopic":"twitter_user_data","bucket":"twitter_tweets","userBucket":"twitter_user_data"}}
#     -- This will both publish in a pub/sub and store in GCS.
#
# Command Line:
#   Give a query and optionally supply a limit.
#   Example calls:
#     -query COVID masks "opening schools" -limit 25
#     This example will search for 25 twitters mentioning COVID, 25 mentioning masks, and 25 mentioning the phrase "opening schools".
#
# Output will be log and data objects as JSON.
# Data Object:
# Data objects include a key named "query" which contains the term searched for and "data" which contains the Twitter.
#   {"query": "COVID", "data": {"created_at": "Sun Aug 09 00:33:21 +0000 2020", "id": 1292257624524693504, "id_str": "1292257624524693504", "text": "@Ximmmmss Tista\u00f1o", "display_text_range": [10, 17], "source": "<a href=\"http://twitter.com/download/android\" rel=\"nofollow\">Twitter for Android</a>", "truncated": false, "in_reply_to_status_id": 1292257322283212800, "in_reply_to_status_id_str": "1292257322283212800", "in_reply_to_user_id": 1097272272031760385, "in_reply_to_user_id_str": "1097272272031760385", "in_reply_to_screen_name": "Ximmmmss", "user": {"id": 1001287290117799936, "id_str": "1001287290117799936", "name": "k1000aaAa", "screen_name": "camilameneses__", "location": null, "url": "https://www.instagram.com/_camilameneses_/?hl=es-la", "description": "k onda raza\ud83c\udf08", "translator_type": "none", "protected": false, "verified": false, "followers_count": 250, "friends_count": 188, "listed_count": 0, "favourites_count": 12128, "statuses_count": 4004, "created_at": "Tue May 29 02:21:07 +0000 2018", "utc_offset": null, "time_zone": null, "geo_enabled": true, "lang": null, "contributors_enabled": false, "is_translator": false, "profile_background_color": "F5F8FA", "profile_background_image_url": "", "profile_background_image_url_https": "", "profile_background_tile": false, "profile_link_color": "1DA1F2", "profile_sidebar_border_color": "C0DEED", "profile_sidebar_fill_color": "DDEEF6", "profile_text_color": "333333", "profile_use_background_image": true, "profile_image_url": "http://pbs.twimg.com/profile_images/1290214852116582400/oaea3y1r_normal.jpg", "profile_image_url_https": "https://pbs.twimg.com/profile_images/1290214852116582400/oaea3y1r_normal.jpg", "profile_banner_url": "https://pbs.twimg.com/profile_banners/1001287290117799936/1596446412", "default_profile": true, "default_profile_image": false, "following": null, "follow_request_sent": null, "notifications": null}, "geo": null, "coordinates": null, "place": null, "contributors": null, "is_quote_status": false, "quote_count": 0, "reply_count": 0, "retweet_count": 0, "favorite_count": 0, "entities": {"hashtags": [], "urls": [], "user_mentions": [{"screen_name": "Ximmmmss", "name": "bomb\u00f3n miope", "id": 1097272272031760385, "id_str": "1097272272031760385", "indices": [0, 9]}], "symbols": []}, "favorited": false, "retweeted": false, "filter_level": "low", "lang": "es", "timestamp_ms": "1596933201329"}}
#
# Log Object:
# Log objects have a key named "log" which contains the log statement.
#   {"log": "Waiting on rate limit to replenish."}
import traceback

import tweepy
from google.cloud.exceptions import Forbidden
from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
import time
import re
import datetime
import json
import argparse
from google.cloud.pubsub_v1 import PublisherClient
from google.cloud import storage

consumer_key = 'YOUR-CONSUMER-KEY'
consumer_secret = 'YOUR-SECRET-KEY'
access_token = 'YOUR-ACCESS-TOKEN-KEY'
access_secret = 'YOUR-ACCESS-SECRET-KEY'

class ExampleRequest(object):
  args = None
  
  # (args.projectId,args.query,limit=args.limit,topic=args.topic,debug=args.log)
  def __init__(self, projectId, query, limit=None, topic=None, userTopic=None, bucket=None, userBucket=None, debug=None):
    if type(query) == str:
      if not query.startswith('"'): query = '"' + query + '"'
    else:
      query = json.dumps(query)
    message = {'query': query, 'limit': limit if limit is not None else '', 'projectId': projectId,
               'topic': topic if topic is not None else '',
               'bucket': bucket if bucket is not None else '',
               'userTopic': userTopic if userTopic is not None else '',
               'userBucket': userBucket if userBucket is not None else ''}
    self.args = {'message': json.dumps(message)}
  
  def get_json(self,force=False):
    return json.loads(self.args['message'])

class MyListener(StreamListener):
  """Custom StreamListener for streaming data."""

  _tweetFields = ["created_at", "id", "id_str", "text", "in_reply_to_status_id", "in_reply_to_status_id_str",
                  "in_reply_to_user_id", "in_reply_to_user_id_str", "in_reply_to_screen_name", "geo", "coordinates",
                  "place", "contributors", "quoted_status_id", "quoted_status_id_str", "is_quote_status", "quote_count",
                  "reply_count", "retweet_count", "favorite_count", "favorited", "retweeted", "lang", "timestamp_ms"]
  _tweetReferences = {"user": "id",
                      "retweeted_status": "id",
                      "hashtags": "text",
                      "user_mentions": "id",
                      "symbols": "text",
                      "extended_tweet": "full_text"}
  _userFields=["id","id_str","name","screen_name","location","description","followers_count","friends_count","listed_count","favourites_count","statuses_count","created_at","following","follow_request_sent","notifications"]

  @classmethod
  def extractReference(cls, outerField, element):
    '''

    :param outerField: the field name of a nested field which holds information for entities the tweet is referencing.
    :param element: either a dict representing one referenced entity or a list of referenced entities.
    :return: returns a list of 0,1, or more entities referenced within element.
    '''
    entities = []
    if outerField in cls._tweetReferences:
      subfield = cls._tweetReferences[outerField]
      if type(element) == dict:
        if subfield in element: entities.append(element[subfield])
      elif type(element) == list:
        for subelement in element:
          entities.extend(cls.extractReference(outerField, subelement))
    return entities

  @classmethod
  def extractTweet(cls,tweet,query):
    '''
    Will process a single tweet and produce one or more records of tweet data. A single tweet may reference a tweet that it is retweeting, in which case this method returns both as tweet data records.
    :param tweet: the JSON from twitter representing one tweet.
    :param query: query that the tweet is a search result for.
    :return: returns one or more records of tweet data.
    '''
    if 'tweet' in tweet: return cls.extractTweet(tweet['tweet'],query)
    tweetRows = []
    tweetRow = {}
    for field, value in tweet.items():
      if value is not None:
        if field in cls._tweetFields:
          tweetRow[field] = value
        elif field in ['user','retweeted_status']:
          # These nested elements only contain one object.
          referencedEntities = cls.extractReference(field, value)
          if len(referencedEntities)>0:
            tweetRow[field]=referencedEntities[0]
        elif field in cls._tweetReferences:
          # Capture potentially multiple references.
          referencedEntities = cls.extractReference(field, value)
          if len(referencedEntities) > 0:
            tweetRow[field] = referencedEntities
        elif field == 'entities':
          # Unnest the entities object.
          for entityType, entity in value.items():
            # Capture each referenced entity.
            referencedEntities = cls.extractReference(entityType, entity)
            if len(referencedEntities) > 0:
              tweetRow[entityType] = referencedEntities
      
        if field == 'retweeted_status':
          try:
            nestedTweets = cls.extractTweet(tweet['retweeted_status'],query)
            tweetRows.extend(nestedTweets)
          except Exception as ex:
            print(json.dumps(
              {'log': 'SKIPPING Cannot parse nested tweet ' + str(tweet['retweeted_status']) + '. ERROR ' + str(ex)}))
    tweetRow['query']=query
    tweetRow['text']=json.dumps(tweet)
    tweetRows.append(tweetRow)
    return tweetRows

  @classmethod
  def _extractUser(cls,userData):
    userRow = {}
    for field,value in userData.items():
      if value is not None:
        if field in cls._userFields:
          userRow[field] = value
    userRow['text']=json.dumps(userData)
    return userRow

  @classmethod
  def extractUsers(cls,tweet):
    userRows = []
    if type(tweet)==dict:
      if 'tweet' in tweet: return cls.extractUser(tweet['tweet'])
      for field, value in tweet.items():
        if value is not None:
          if field=='user':
            userRows.append(cls._extractUser(value))
          elif type(value) in [dict,list]:
            userRows.extend(cls.extractUsers(value))
    elif type(tweet)==list:
      for element in tweet:
        userRows.extend(cls.extractUsers(element))
    return userRows

  def __init__(self, projectId, query, limit, topic=None, userTopic=None, bucket=None, userBucket=None):
    self.query = query
    self.limit = limit
    if topic is not None:
      if projectId is None: raise Exception('Must supply a project ID if you want to publish to topic "{topic}".'.format(topic=topic))
      self._topic = ('projects/' + projectId + '/topics/' + topic)
      print(json.dumps({'log': 'Output to Pub/Sub: ' + self._topic}))
    else:
      self._topic=None
    if userTopic is not None:
      if projectId is None: raise Exception('Must supply a project ID if you want to publish to topic "{topic}".'.format(topic=userTopic))
      self._userTopic = ('projects/' + projectId + '/topics/' + userTopic)
      print(json.dumps({'log': 'Output user data to Pub/Sub: ' + self._userTopic}))
    else:
      self._userTopic=None
    self._publisher = None
    self._userPublisher = None
    
    self._bucketClient = None
    self._userBucketClient = None
    self._bucket = bucket
    if bucket is not None:
      print(json.dumps({'log': 'Output to bucket: ' + self._bucket}))
    self._userBucket=userBucket
    if userBucket is not None:
      print(json.dumps({'log': 'Output user data to bucket: ' + self._userBucket}))

  def _writeToBucket(self, bucketClient, bucket, records):
    key = self._createObjectKey()
    try:
      if bucketClient is None: bucketClient=storage.Client().bucket(bucket)
      for record in records:
        recordKey = key + (('_' + str(record['id'])) if 'id' in record else '')
        bucketClient.blob(recordKey).upload_from_string(json.dumps(record))
    except Forbidden as fe:
      try:
        print(json.dumps({'log':
          'Failed to write to GCS bucket {bucket} because access to object {objectName} is forbidden. Error code={code}, response content={response}'.format(
            bucket=self._bucket,
            objectName=key,
            code=str(fe.code),
            response=fe.response.content
          )}))
      except:
        print(json.dumps({
          'log': 'Failed to write to GCS bucket {bucket} because access to object {objectName} is forbidden.'.format(
            bucket=self._bucket,
            objectName=key
          )}))
    except Exception as ex:
      print(json.dumps({'log': 'Failed to write to GCS bucket {bucket}, object {objectName}. Error='.format(
        bucket=self._bucket,
        objectName=key
      ) + str(ex)}))
      traceback.print_exc()
    return bucketClient

  def _createObjectKey(self):
    key = ''
    if type(self.query) == str:
      key += self.query
    else:
      key += '_'.join(self.query)
    key += '_' + str(datetime.datetime.now()) + '.json'
    return re.sub(r'[^A-Za-z0-9_.-]', '_', key)
  
  def on_data(self, data):
    print(json.dumps({'log':'Found tweet...'}))
    try:
      tweets=json.loads(data)
      tweetRecords=self.extractTweet(tweets,self.query)
      userRecords=self.extractUsers(tweets)

      if self._bucket is not None:
        self._bucketClient=self._writeToBucket(self._bucketClient,self._bucket,tweetRecords)
      if self._userBucket is not None:
        self._userBucketClient=self._writeToBucket(self._userBucketClient,self._userBucket,userRecords)
        
      if self._topic is not None:
        if self._publisher is None: self._publisher = PublisherClient()
        for record in tweetRecords:
          try:
            self._publisher.publish(self._topic, data=json.dumps(record).encode("utf-8"), **record)
          except:
            self._publisher.publish(self._topic, data=json.dumps(record).encode("utf-8"), query=self.query)

      if self._userTopic is not None:
        if self._userPublisher is None: self._userPublisher = PublisherClient()
        for record in userRecords:
          try:
            self._userPublisher.publish(self._userTopic, data=json.dumps(record).encode("utf-8"), **record)
          except:
            self._userPublisher.publish(self._userTopic, data=json.dumps(record).encode("utf-8"))

      self.limit -= 1
      if self.limit <= 0: return False
      return True
    except Exception as e:
      traceback.print_exc()
      print(json.dumps({'log': 'Error on_data:' + str(e)}))
      time.sleep(5)
    return True
  
  def on_error(self, status):
    if status == 420:
      print(json.dumps({'log': 'Exceeding rate limit. Exiting stream.'}))
      return False
    print(json.dumps({'log': 'Received error when querying for {query}: {code}'.format(query=self.query, code=status)}))
    time.sleep(10)
    return True

def format_filename(fname):
  """Convert file name into a safe string.
  Arguments:
      fname -- the file name to convert
  Return:
      String -- converted file name
  """
  return re.sub(r'[^A-Za-z0-9_.-]', r'_', fname) + '_' + datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S.%f')

def _getMessageJSON(request):
  request_json = request.get_json(force=True)
  message = None
  if request.args is not None:
    print(json.dumps({'log': 'request is ' + str(request) + ' with args ' + str(request.args)}))
    if 'message' in request.args:
      message = request.args.get('message')
    elif 'query' in request.args:
      message = request.args
  if message is None and request_json is not None:
    print(json.dumps({'log': 'request_json is ' + str(request_json)}))
    if 'message' in request_json:
      message = request_json['message']
    elif 'query' in request_json:
      message = request_json
  
  if message is None:
    print('message is empty. request=' + str(request) + ' request_json=' + str(request_json))
    message = '{"query":"The Spicy Amigos"}'
  
  if type(message) == str:
    try:
      messageJSON = json.loads(message)
    except:
      messageJSON = {"query": [message]}
  else:
    messageJSON = message
  return messageJSON

def main(request):
  """Responds to any HTTP request.
  Args:
      request (flask.Request): HTTP request object.
  Returns:
      The response text or any set of values that can be turned into a
      Response object using
      `make_response <http://flask.pocoo.org/docs/1.0/api/#flask.Flask.make_response>`.
  """
  messageJSON = _getMessageJSON(request)
  
  debug = messageJSON.get('debug', 10)
  if debug == 0: debug = None
  
  projectId = messageJSON.get('projectId', '')
  if projectId == '': projectId = None
  
  topic = messageJSON.get('topic', '')
  if topic == '': topic = None
  userTopic = messageJSON.get('userTopic', '')
  if userTopic == '': userTopic = None

  limit = messageJSON.get('limit', 10)
  if limit == '': limit = 10
  
  query = messageJSON.get('query', ['The Fast Dog'])
  
  bucket = messageJSON.get('bucket', None)
  if bucket=='': bucket=None
  userBucket = messageJSON.get('userBucket', None)
  if userBucket=='': userBucket=None
  
  # if debug is not None:
  print(json.dumps({'log': 'Parsed message is ' + json.dumps(messageJSON)}))
  
  if type(query) == str: query = [query]
  
  auth = OAuthHandler(consumer_key, consumer_secret)
  auth.set_access_token(access_token, access_secret)
  
  for term in query:
    if debug is not None: print(json.dumps({'log': 'Waiting on rate limit to replenish.'}))
    tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
    
    if debug is not None: print(json.dumps({'log': 'Querying for {term}'.format(term=term)}))
    twitter_stream = Stream(auth, MyListener(projectId, term, limit, topic=topic, userTopic=userTopic, bucket=bucket, userBucket=userBucket))
    twitter_stream.filter(track=term)
    if debug is not None: print(json.dumps({'log': 'Done querying for {query}.'.format(query=term)}))

if __name__ == '__main__':
  # To call from the command line, provide two arguments: query, limit.
  parser = argparse.ArgumentParser()
  
  # add arguments to the parser
  parser.add_argument('-query', nargs='+', help='One or more terms to search form (enclose phrases in double quotes.)')
  parser.add_argument('-limit', nargs='?', help='Limit the number of calls to twitter per query term, default is 10.',
                      default=None)
  parser.add_argument('-bucket',
                      help='Provide a bucket name to store the twitter data to if you want to persist the data in GCS. The bucket must already exist.',
                      default=None)
  parser.add_argument('-userBucket',
                      help='Provide a bucket name to store the user data to if you want to persist the data in GCS. The bucket must already exist.',
                      default=None)
  parser.add_argument('-projectId',
                      help='Specify the Google cloud project ID (required when publishing to pub/sub.).',
                      default=None)
  parser.add_argument('-topic', help='Specify a pub/sub topic to publish to if you want the twitter records to go to pub/sub.',
                      default=None)
  parser.add_argument('-userTopic', help='Specify a pub/sub topic to publish to if you want the user records to go to pub/sub.',
                      default=None)
  parser.add_argument('-log', help='Print out log statements.', default=None)
  
  # parse the arguments
  args = parser.parse_args()
  
  exampleRequest = ExampleRequest(args.projectId, args.query, limit=args.limit, topic=args.topic, userTopic=args.userTopic, bucket=args.bucket, userBucket=args.userBucket, debug=args.log)
  main(exampleRequest)