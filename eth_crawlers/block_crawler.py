import datetime
import redis
from apscheduler.schedulers.blocking import BlockingScheduler
from util import insert_sql,increment_sql,rpc_request,update_sql,send_notification,find_sql_join,config,MyLogger

# Redis Connection
pool = redis.ConnectionPool(
    host = config.get('redis', 'host'),
    port = int(config.get('redis', 'port')),
    db = int(config.get('redis', 'db'))
)
redis_conn = redis.Redis(connection_pool=pool)

# Blockchain Node
url = config.get('node', 'url')
confirmation_threshold = int(config.get('eth', 'confirmations'))

logs_directory = config.get('eth_block', 'logs')
category = config.get('eth_block', 'category')
hook_queue = config.get('eth_hook_main', 'queue')

def block_crawler():
    """
    Block Crawling process
    :return:
    """

    obj_logger = MyLogger(logs_directory, category)
    obj_logger.msg_logger('Getting Block Numbers.....')

    # Get Current Block from RPC
    current_block = int(rpc_request(obj_logger,'eth_blockNumber',[]).get('result',0),16)
    crawled_blocks = int(redis_conn.get('eth_eth_blocks_crawled') or 0)

    obj_logger.msg_logger('Crawled Block Number : %s'%(crawled_blocks))
    obj_logger.msg_logger('Current Block Number : %s' % (current_block))
    obj_logger.msg_logger('Pending : %s' % (current_block - crawled_blocks))

    if current_block > crawled_blocks:

        for block_number in range(crawled_blocks + 1,current_block + 1):

            obj_logger.msg_logger('#' * 100)
            obj_logger.msg_logger('Crawling Block : %s || Current Block : %s'%(block_number,current_block))
            obj_logger.msg_logger('Pending : %s' % (current_block - block_number))
            obj_logger.msg_logger('Start :%s'%(datetime.datetime.now()))

            # Increment Confirmations for tx_id whose 1 confirmation is already sent
            for tx_hash in redis_conn.smembers('eth_eth_pct_set'):
                tx_hash = tx_hash.decode('utf-8')
                data = find_sql_join(logger=obj_logger,
                    table_names=['user_master','address_master','eth_transactions'],
                    filters = {'eth_transactions.tx_hash':tx_hash},
                    on_conditions = {'user_master.user_name':'address_master.user_name','address_master.address':'eth_transactions.to_address'},
                )

                if not data:
                    obj_logger.error_logger('>>>>>>>>>>> Data not found in SQL for tx_hash : %s'%(tx_hash))
                    continue

                confirmations =  data[0]['confirmations']
                notif_url = data[0]['notification_url']

                if confirmations < confirmation_threshold:
                    increment_sql(obj_logger, 'eth_transactions',{'tx_hash':tx_hash},'confirmations')
                    notif_params = {
                        'from_address': data[0]['from_address'],
                        'to_address': data[0]['to_address'],
                        'tx_hash': tx_hash,
                        'bid_id': -1,
                        'confirmations': confirmations + 1,
                        'block_number': data[0]['block_number'],
                        'value': data[0]['value'],
                        'flag': 'eth_incoming'
                    }
                    obj_logger.msg_logger('>>>>>>>> Sending Confirmation : %s || %s' % (confirmations + 1, tx_hash))
                    send_notification(obj_logger, notif_url, notif_params,queue=hook_queue)
                else:
                    obj_logger.msg_logger('>>>>>>>> %s Confirmation Sent : %s' %(confirmation_threshold,  tx_hash))
                    obj_logger.msg_logger('>>>>>>>> Removing from eth_eth_pct_set : %s' %(tx_hash))
                    redis_conn.srem('eth_eth_pct_set', tx_hash)

            # Crawling Blocks
            block_info = rpc_request(obj_logger,'eth_getBlockByNumber',[hex(int(block_number)), True])
            if block_info:
                block_transactions = block_info.get('result', {}).get('transactions', [])
            else:
                block_transactions = []
                obj_logger.error_logger('Data not found for block number : %s' %str(block_number))

            for tx in block_transactions:

                to_address = tx['to']
                if (redis_conn.sismember('eth_eth_aw_set',to_address)):

                    tx_hash = tx['hash']
                    obj_logger.msg_logger('>>>>>>>> Transaction Found in Block : %s : %s' % (block_number,tx_hash))

                    confirmations = 1
                    block_number = int(tx['blockNumber'], 16)

                    # Check if 1 Confirmation is sent from mempool crawler - Should be found in eth_eth_pct_set
                    if not redis_conn.sismember('eth_eth_pct_set',tx_hash):

                        from_address = tx['from']
                        value = int(tx['value'], 16)
                        bid_id = -1
                        flag = 'eth_incoming'
                        sys_timestamp = datetime.datetime.now()


                        # Check if 0 Confirmation is sent from mempool crawler - Should be found in eth_eth_zct_set
                        if redis_conn.sismember('eth_eth_zct_set',tx_hash):
                            update_sql(obj_logger, 'eth_transactions',{'tx_hash':tx_hash},updated_values={'confirmations':confirmations,'block_number':block_number})
                        else: # Missed in Mempool - Send 1 Confirmation and add in eth_eth_pct_set
                            obj_logger.msg_logger('>>>>>>>> Transaction Missed from mempool. Sending %s confirmation : %s' % (confirmations, str(tx_hash)))
                            data = {
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
                            insert_sql(obj_logger,'eth_transactions', data)

                        notif_url = find_sql_join(logger=obj_logger,
                            table_names = ['user_master','address_master'],
                            filters = {'address_master.address':to_address},
                            on_conditions= {'user_master.user_name':'address_master.user_name'},
                            columns = ['user_master.notification_url']
                        )[0]['notification_url']

                        notif_params = {
                            'from_address': from_address,
                            'to_address': to_address,
                            'tx_hash': tx_hash,
                            'bid_id': -1,
                            'confirmations': confirmations,
                            'block_number': block_number,
                            'value': value,
                            'flag': flag
                        }
                        obj_logger.msg_logger('>>>>>>>> Sending Confirmation : %s || %s' % (confirmations, tx_hash))
                        send_notification(obj_logger, notif_url, notif_params,queue=hook_queue)
                        obj_logger.msg_logger('>>>>>>>> Adding to eth_eth_pct_set : %s' % (tx_hash))
                        redis_conn.sadd('eth_eth_pct_set', tx_hash.encode('utf-8'))

            # Increment Redis Blocks Crawled
            redis_conn.set('eth_eth_blocks_crawled',block_number)
            obj_logger.msg_logger('Ends :%s' % (datetime.datetime.now()))
            obj_logger.msg_logger('#' * 100)
    else:
        obj_logger.msg_logger('#'*100)


def main():
    """
    Scheduling
    :return:
    """
    try:
        sched = BlockingScheduler(timezone='Asia/Kolkata')
        sched.add_job(block_crawler, 'interval', id='eth_block_crawler', seconds=3)
        sched.start()
    except Exception as e:
        obj_logger = MyLogger(logs_directory, category)
        obj_logger.error_logger('Main : %s' % (e))


if __name__ == '__main__':
    main()
