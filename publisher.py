import json
import logging
import gpubsub
import mysqlbinlog2blinker
from mysqlbinlog2blinker import signals

__author__ = 'Silverhand'
_logger = logging.getLogger(__name__)


# @signals.on_rows_updated
# def print_on_rows_updated(table, rows, meta):
#     row = rows[0]
#     print('=' * 15, 'Message %s.on_rows_updated ' % table, '=' * 15)
#     print('Metadata: %s' % meta)
#     print('Updated value: %s' % row['updated_values'])



@signals.on_rows_updated
def publish_on_rows_updated(table, rows, meta):
    msg_payload = json.dumps({
        'table': table,
        'row': rows[0],
        'meta': meta,
    })
    print('Sending message to Pub/Sub topic "%s"' % gpubsub.binlog_topic.name)
    gpubsub.binlog_topic.publish(msg_payload.encode())


def listening():
    """
    Listening mysql binlog publish to pubsub
    :return:
    """
    mysqlbinlog2blinker.start_replication(
        {
            "host": "localhost",
            "port": 3306,
            "user": "root",
            "passwd": "",
        },
    )


if __name__ == '__main__':
    listening()
