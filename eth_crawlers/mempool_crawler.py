import datetime
import redis
from apscheduler.schedulers.blocking import BlockingScheduler
from util import insert_sql, rpc_request, send_notification, find_sql_join, MyLogger, config

# Redis Connection
pool = redis.ConnectionPool(
    host = config.get('redis', 'host'),
    port = int(config.get('redis', 'port')),
    db = int(config.get('redis', 'db'))
)
redis_conn = redis.Redis(connection_pool=pool)

# Blockchain Node
logs_directory = config.get('eth_mempool', 'logs')
category = config.get('eth_mempool', 'category')
hook_queue = config.get('eth_hook_main', 'queue')

def mempool_crawler():
    """
    Mempool Process
    :return:
    """

    obj_logger = MyLogger(logs_directory,category)
    obj_logger.msg_logger('#'*100)
    obj_logger.msg_logger('Getting Mempool Data')

    # Get Mempool Data
    mempool_transaction_data = rpc_request(obj_logger, 'eth_getBlockByNumber', ['pending', True]).get('result',{}).get('transactions',[])

    obj_logger.msg_logger('Crawling Mempool Starts')

    for tx in mempool_transaction_data:

        tx_hash = tx['hash']
        to_address = tx['to']

        # Redis Check
        if (not redis_conn.sismember('eth_eth_zct_set',tx_hash)) and (redis_conn.sismember('eth_eth_aw_set',to_address)):
            obj_logger.msg_logger('>>>>>>>> Transaction Found in Mempool : %s'%(tx_hash))

            from_address = tx['from']
            value = int(tx['value'],16)
            bid_id = -2
            confirmations = 0
            block_number = -1
            flag = 'eth_incoming'
            sys_timestamp = datetime.datetime.now()

            # Insert in DB
            result = insert_sql(
                logger=obj_logger,
                table_name= 'eth_transactions',
                data={
                'from_address': from_address,
                'to_address': to_address,
                'tx_hash': tx_hash,
                'bid_id': bid_id,
                'confirmations': confirmations,
                'block_number': block_number,
                'value': value,
                'flag': flag,
                'sys_timestamp': sys_timestamp,
                }
            )

            if result:
                notif_url = find_sql_join(logger=obj_logger,
                    table_names=['user_master', 'address_master'],
                    filters={'address_master.address': to_address},
                    on_conditions={'user_master.user_name': 'address_master.user_name'},
                    columns=['user_master.notification_url']
                )[0]['notification_url']

                notif_params = {
                    'from_address': from_address,
                    'to_address': to_address,
                    'tx_hash': tx_hash,
                    'bid_id': bid_id,
                    'confirmations': confirmations,
                    'block_number': block_number,
                    'value': value,
                    'flag': flag
                }
                send_notification(obj_logger,notif_url,notif_params,queue=hook_queue)
                obj_logger.msg_logger('>>>>>>>> Adding to eth_eth_zct_set : %s' % (tx_hash))
                redis_conn.sadd('eth_eth_zct_set', tx_hash.encode('utf-8')) # To cross check in Block Crawler and not to send multiple notification

    obj_logger.msg_logger('Crawling Mempool Ends')
    obj_logger.msg_logger('#' * 100)


def main():
    """
    Scheduling
    :return:
    """
    try:
        sched = BlockingScheduler(timezone='Asia/Kolkata')
        sched.add_job(mempool_crawler, 'interval', id='eth_mempool_crawler', seconds=3)
        sched.start()
    except Exception as e:
        obj_logger = MyLogger(logs_directory,category)
        obj_logger.error_logger('Main : %s'%(e))


if __name__ == '__main__':
    main()
