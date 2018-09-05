import pika
import requests
import json
import datetime
from util import MyLogger,config,send_notification,insert_sql

headers = {'Content-type': 'application/json'}

# Connection
connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

# Queue
queue = config.get('hook_main', 'queue')
channel.queue_declare(queue=queue,durable=True)
logs_directory = config.get('hook_main', 'logs')
category = config.get('hook_main', 'category')


def callback(ch, method, properties, body):
    try:

        obj_logger = MyLogger(logs_directory, category)
        obj_logger.msg_logger('#'*100)
        obj_logger.msg_logger('Getting Data : %s'%(datetime.datetime.now()))

        # Data
        data = json.loads(body)
        notification_url = data['notification_url']
        data.pop('notification_url')
        notification_params = data

        obj_logger.msg_logger('>>>>>>>>>> Sending Notification : %s || %s' % (notification_url, notification_params))
        requests.post(notification_url, data=json.dumps(notification_params), headers=headers)
        obj_logger.msg_logger('>>>>>>>>>> Notification Success : %s || %s' % (notification_url, notification_params))
        insert_sql(logger=obj_logger,table_name='notification_logs', data={
            'tx_hash' : notification_params['tx_hash'],
            'notification_url ': notification_url,
            'params': str(notification_params),
            'timestamp' : datetime.datetime.now(),
            'Status': 'Success'
        })
    except Exception as e:

        obj_logger.error_logger('>>>>>>>>>> Notification Failure : %s || %s || %s' % (e, notification_url, notification_params))
        exception_queue = config.get('hook_excpetion', 'queue')
        obj_logger.msg_logger('>>>>>>>>>> Pushing to Exception Queue : %s'%(exception_queue))
        send_notification(obj_logger, notification_url, notification_params, queue=exception_queue)

    finally:
        obj_logger.msg_logger("#" * 100)
        # We are ACK in both the case of success or failure because if there is no error then its ok
        # But if there is an error then we are sending it to Exception Queue . So in both the case we can delete this from main queue
        ch.basic_ack(delivery_tag=method.delivery_tag)


channel.basic_consume(callback,queue=queue)
channel.start_consuming()
