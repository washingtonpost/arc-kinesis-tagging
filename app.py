import boto3
import json
import gzip
import time
import os 
import threading
import sys

# import other project files
from utility import aws, tag

from dotenv import load_dotenv
load_dotenv()


def start_consuming_records(kinesis_client, stream_name, shard_id):
    # sets up the stream consumer and gets a starting iterator
    shard_iterator = kinesis_client.get_shard_iterator(StreamName=stream_name, ShardId=shard_id, ShardIteratorType='LATEST')
    initial_shard_iterator = shard_iterator['ShardIterator']


    record_response = kinesis_client.get_records(ShardIterator=initial_shard_iterator, Limit=20)
    while 'NextShardIterator' in record_response:
        record_response = kinesis_client.get_records(ShardIterator=record_response['NextShardIterator'], Limit=20)

        records = record_response['Records']
        if len(records) == 0:    # waits to find records. depending on traffic on your stream, it may take many cycles to find a record on the stream.
            print('waiting...')
            time.sleep(3)
            continue
        else: 
            for rec in records:
                d = rec['Data']

                try:
                    unbuf = gzip.decompress(d)
                except: 
                    print('unable to decompress buffer: ', d)

                try:
                    record = json.loads(unbuf.decode("utf-8"))
                except:
                    print('record is not a valid JSON. likely S3 link:', record)
                    # for very large documents over 1MB in size, Arc sends an S3 url rather than a fully-baked story object
                    # in that case, record.url will equal the S3 url for you to download the object from. 
                    record = {'url': unbuf.decode("utf-8")}


                # --- ENTER CUSTOM BUSINESS LOGIC HERE ----
                # in this example, looking for events that have been published
                # we do this by looking for:
                # operation: insert-story
                # published: True
                # created: True <-- can be added to find first publishes

                if record.get('operation') == 'insert-story' and record.get('published') == True and record.get('id')=='CINZOH7FJVDZ5ERKVXZNL5N22Q':
                    obj = {
                        'shard_origination': shard_id, 
                        '_id': record.get('id'),
                        'operation': record.get('operation'),
                        'created':record.get('created'),
                        'type':record.get('type'),
                        'published':record.get('published'),
                        'referent_update':record.get('trigger').get('referent_update') if record.get('trigger') else None,
                        'headline':record.get('body').get('headlines').get('basic'),
                        'revision':record.get('body').get('revision')
                    }

                    print(obj)

                    try:
                        tag.autotag(obj)
                    except:
                        print('Auto tagging failed. Stream continues.')
                        pass                

        # wait for 3 seconds, and then go back for more records
        time.sleep(3)


# run the things
stream_name = os.environ.get('stream_name')
print(stream_name) # quick validation that your stream name is correct & your .env has loaded correctly

sts_response = aws.assume_role()
kinesis_client = aws.create_kinesis_client(sts_response)
stream_shards = aws.describe_stream(kinesis_client, stream_name)

# your kinesis stream has two shards. in order to get ALL of the data, we need to listen to both. 
# to do this, we create two parallel threads running at once, both of which are listening to the stream above. 
try:
    shard_1 = threading.Thread( target=start_consuming_records, args=(kinesis_client, stream_name, stream_shards[0].get('ShardId')) )
    shard_2 = threading.Thread( target=start_consuming_records, args=(kinesis_client, stream_name, stream_shards[1].get('ShardId')) )

    shard_1.start()
    shard_2.start()
except:
    raise Exception('Unable to start threads.')
