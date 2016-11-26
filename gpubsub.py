import os
import logging
from gcloud import pubsub
from oauth2client.service_account import ServiceAccountCredentials

__author__ = 'SilverHand'
_logger = logging.getLogger(__name__)

credential_key = os.path.join(os.path.dirname(__file__), 'etc',
                              'vnp-pubsub-lab-5c68513d3537.json')
credentials = ServiceAccountCredentials.from_json_keyfile_name(
    credential_key)

client = pubsub.Client('vnp-pubsub-lab', credentials=credentials)

binlog_topic = client.topic('devfest2016')
subscription = binlog_topic.subscription('pulling')
