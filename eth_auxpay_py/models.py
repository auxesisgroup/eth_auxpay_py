# -*- coding: utf-8 -*-
from __future__ import unicode_literals
import pymysql
import configparser

# Create your models here.

def get_db_connect(logger):
    """
    MySQL Connection
    :return: DB object
    """
    try:
        conf_file = r'/var/eth_conf/conf.ini'
        parser = configparser.RawConfigParser()
        parser.read(conf_file)
        db = pymysql.connect(
            host = parser.get('sql', 'host'),
            user = parser.get('sql', 'user'),
            passwd = parser.get('sql', 'passwd'),
            db = parser.get('sql', 'db')
        )
        return db
    except Exception as e:
        logger.msg_logger("Error get_db_connect : " + str(e))
        raise Exception(str(e))


def insert_sql(logger, table_name, data):
    try:
        ret_status = False
        db = get_db_connect(logger)
        cursor = db.cursor()
        query = 'insert into %s(%s) values(%s)' % (table_name, ','.join([key for key in data]),','.join(['%s' for _ in data]))
        values = tuple([value for key,value in data.items()])
        cursor.execute(query,(values))
        db.commit()
        logger.msg_logger('>>>>>>>> MYSQL Insert Success : %s || %s' % (query, str(data)))
        ret_status = True
    except Exception as e:
        logger.msg_logger('Error insert_sql : %s | %s'%(str(e),query))
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

        params = ''
        for key,value in filters.items():
            params += "%s = '%s' AND "%(key,value)
        params = params[:-5] # Removing AND

        query = 'SELECT %s FROM %s WHERE %s'%(columns,table_name,params)
        cursor.execute(query)
        data = cursor.fetchall()
    except Exception as e:
        logger.msg_logger('find_sql : %s | %s'%(str(e),query))
    finally:
        if db: db.close()
        return data


