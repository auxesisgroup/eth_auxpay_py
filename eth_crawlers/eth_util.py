import pymysql
import configparser
import requests
import json
import pika
import logging
import sys
import os
import datetime

# Config Source
conf_file = r'/var/eth_conf/conf.ini'
config = configparser.RawConfigParser()
config.read(conf_file)


class MyLogger():
    """
    Class For Handling Logging
    """

    def __init__(self, directory, category):

        self.category = category

        str_date = str(datetime.date.today()).replace('-', '_')
        file_path = os.path.join(directory, str_date + '.txt')

        logging.basicConfig(
            filename=file_path,
            filemode='a',
            format='%(asctime)s,%(msecs)d | %(name)s | %(levelname)s | %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S',
            level=logging.INFO
        )
        logging.getLogger("requests").setLevel(logging.WARNING)
        logging.getLogger("pika").setLevel(logging.CRITICAL)
        logging.getLogger("pymysql").setLevel(logging.CRITICAL)
        self.logger = logging.getLogger()
        self.logger.addHandler(logging.StreamHandler(sys.stdout))

    def msg_logger(self,msg):
        self.logger.info(msg)


    def error_logger(self,error):
        self.logger.error(error)

        insert_sql(self,'error_logs', {
            'category': self.category,
            'file_name': os.path.basename(__file__),
            'error': error,
            'timestamp': datetime.datetime.now()
        })


# MYSQL - Starts
def get_db_connect(logger):
    """
    MySQL Connection
    :return: DB object
    """
    try:
        db = pymysql.connect(
            host = config.get('sql', 'host'),
            user = config.get('sql', 'user'),
            passwd = config.get('sql', 'passwd'),
            db = config.get('sql', 'db')
        )
        return db
    except Exception as e:
        logger.error_logger('Error get_db_connect : %s ' % (str(e)))


def query_from_filter(filters,type='AND'):
    params = ''
    for key, value in filters.items():
        params += "%s = '%s' %s " % (key, value, type)
    return params[:-(len(type)+2)]


def query_from_filter_join(filters,type='AND'):
    params = ''
    for key, value in filters.items():
        params += "%s = %s %s " % (key, value, type)
    return params[:-(len(type)+2)]


def insert_sql(logger, table_name, data):
    try:
        db = get_db_connect(logger)
        ret_status = False
        cursor = db.cursor()
        query = 'insert into %s(%s) values(%s)' % (table_name, ','.join([key for key in data]),','.join(['%s' for _ in data]))
        values = tuple([value for key,value in data.items()])
        cursor.execute(query,(values))
        db.commit()
        ret_status = True
        logger.msg_logger('>>>>>>>> MYSQL Insert Success : %s || %s' % (query, str(data)))
    except Exception as e:
        logger.error_logger('insert_sql : %s || %s || %s'%(str(e),query,str(data)))
    finally:
        if db : db.close()
        return ret_status


def find_sql(logger, table_name,filters,columns=''):
    try:
        data = None
        db = get_db_connect(logger)
        cursor = db.cursor(pymysql.cursors.DictCursor)

        if columns:
            columns = ','.join(columns)
        else:
            columns = '*'

        params = query_from_filter(filters)
        query = 'SELECT %s FROM %s WHERE %s'%(columns,table_name,params)
        cursor.execute(query)
        data = cursor.fetchall()
        logger.msg_logger('>>>>>>>> MYSQL Find Success : %s' % (query))
    except Exception as e:
        logger.error_logger('find_sql : %s || %s'%(str(e),query))
    finally:
        if db: db.close()
        return data


def find_sql_join(logger, table_names,filters, on_conditions, type='INNER',columns=''):
    try:
        data = None
        db = get_db_connect(logger)
        cursor = db.cursor(pymysql.cursors.DictCursor)

        if columns:
            columns = ','.join(columns)
        else:
            columns = '*'

        on = query_from_filter_join(on_conditions)
        params = query_from_filter(filters)

        join = ''
        for table in table_names:
            join += '%s %s %s JOIN '%(table,table,type)
        join = join[:-(len(type)+7)] # Removing Join String


        query = 'SELECT %s FROM %s ON %s WHERE %s'%(columns,join,on,params)
        cursor.execute(query)
        data = cursor.fetchall()
        logger.msg_logger('>>>>>>>> MYSQL Find Success : %s' % (query))
    except Exception as e:
        logger.error_logger('find_sql_join : %s || %s'%(str(e),query))
    finally:
        if db: db.close()
        return data


def update_sql(logger, table_name,filters,updated_values):
    try:
        db = get_db_connect(logger)
        ret_status = False
        cursor = db.cursor()
        update_params = query_from_filter(updated_values,type=',')
        filter_params = query_from_filter(filters)
        query = 'UPDATE %s SET %s WHERE %s' % (table_name, update_params, filter_params)
        cursor.execute(query)
        db.commit()
        logger.msg_logger('>>>>>>>> MYSQL update Success : %s' % (query))
    except Exception as e:
        logger.error_logger('update_sql : %s || %s'%(str(e),query))
    finally:
        if db : db.close()
        return ret_status


def increment_sql(logger, table_name,filters,column):
    try:
        db = get_db_connect(logger)
        ret_status = False
        cursor = db.cursor()
        params = query_from_filter(filters)
        query = 'UPDATE %s SET %s = %s + 1 WHERE %s'%(table_name,column,column,params)
        cursor.execute(query)
        db.commit()
        ret_status = True
        logger.msg_logger('>>>>>>>> MYSQL Increment Success : %s' % (query))
    except Exception as e:
        logger.error_logger('increment_sql : %s || %s'%(str(e),query))
    finally:
        if db : db.close()
        return ret_status

# MYSQL - Ends

def rpc_request(logger, method,params,url=''):
    """
    Custom RPC Method
    """
    try :
        if not url:
            url = config.get('node', 'url')

        headers = {'Content-type': 'application/json'}
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method":method,
            "params": params
        }
        response = requests.post(url, data=json.dumps(payload), headers=headers)
        return json.loads(response.text)
    except Exception as e:
        logger.error_logger('rpc_request : %s' % (e))


def send_notification(logger, notif_url,notif_params, queue):
    try:
        if not notif_url:
            logger.error_logger('>>>>>>>>>> Notification URL is empty : %s'%(notif_params))
            # TODO - Raise Exception
            return False

        notif_params['notification_url'] = notif_url
        logger.msg_logger('>>>>>>>>>> Sending Notification : %s || %s' % (notif_url, notif_params))
        # Rabbit MQ
        connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        channel = connection.channel()
        channel.queue_declare(queue=queue, durable=True)
        channel.basic_publish(exchange='', routing_key=queue, body=json.dumps(notif_params),properties=pika.BasicProperties(delivery_mode=2))
        connection.close()
        # response = requests.post(notif_url, data=notif_params)
        logger.msg_logger('>>>>>>>>>> Hook Queue : %s ' % (queue))
    except Exception as e:
        logger.error_logger('send_notification : %s'%(e))