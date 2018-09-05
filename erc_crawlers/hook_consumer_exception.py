import pika
import requests
import json
import datetime
from util import MyLogger, config, insert_sql

headers = {'Content-type': 'application/json'}

# Connection
connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

# Queue
queue = config.get('hook_excpetion', 'queue')
channel.queue_declare(queue=queue,durable=True)
logs_directory = config.get('hook_excpetion', 'logs')
category = config.get('hook_excpetion', 'category')


def callback(ch, method, properties, body):
    try:

        obj_logger = MyLogger(logs_directory, category)

        obj_logger.msg_logger('#'*100)
        obj_logger.msg_logger('In Exception Queue : %s'%(queue))
        obj_logger.msg_logger('Getting Data : %s'%(datetime.datetime.now()))

        # Data
        data = json.loads(body)
        notification_url = data['notification_url']
        data.pop('notification_url')
        notification_params = data

        obj_logger.msg_logger('>>>>>>>>>> Sending Notification : %s || %s' % (notification_url, notification_params))
        requests.post(notification_url, data=json.dumps(notification_params), headers=headers)
        obj_logger.msg_logger('>>>>>>>>>> Notification Success : %s || %s' % (notification_url, notification_params))
        insert_sql(logger=obj_logger, table_name='notification_logs', data={
            'tx_hash': notification_params['tx_hash'],
            'notification_url ': notification_url,
            'params': str(notification_params),
            'timestamp': datetime.datetime.now(),
            'Status': 'Success'
        })
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        obj_logger.error_logger('>>>>>>>>>> Notification Failure : %s || %s || %s' % (e, notification_url, notification_params))
    finally:
        obj_logger.msg_logger("#" * 100)


print(' [*] Waiting for messages. To exit pess CTRL+C')

channel.basic_consume(callback,queue=queue)
channel.start_consuming()
