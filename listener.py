import json
import logging
import gpubsub

__author__ = 'SilverHand'
_logger = logging.getLogger(__name__)


def pulling():
    """
    Pulling from Pub/Sub topic
    :return:
    """
    while True:
        msgs_info = gpubsub.subscription.pull()
        """:type: list[(str, gcloud.pubsub.message.Message)]"""

        for ack_id, msg_obj in msgs_info:
            msg = json.loads(msg_obj.data.decode())
            print('=' * 15, 'Message %s.on_rows_updated '
                  % msg['table'], '=' * 15)
            print('Metadata: %s' % msg['meta'])
            print('Updated value: %s' % msg['row']['updated_values'])

            print('Acknowledging..#%s' % ack_id)
            gpubsub.subscription.acknowledge([ack_id, ])


if __name__ == '__main__':
    pulling()
