import logging
from gcloud import pubsub

__author__ = 'SilverHand'
_logger = logging.getLogger(__name__)

client = pubsub.Client('vnp-pubsub-lab')

binlog_topic = client.topic('devfest2016')
subscription = binlog_topic.subscription('pulling')
