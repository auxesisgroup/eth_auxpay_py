import json
import datetime
import redis
from apscheduler.schedulers.blocking import BlockingScheduler
import web3
from eth_util import insert_sql,increment_sql,rpc_request,update_sql,send_notification,find_sql_join,config,MyLogger

# Redis Connection
pool = redis.ConnectionPool(
    host = config.get('redis', 'host'),
    port = int(config.get('redis', 'port')),
    db = int(config.get('redis', 'db'))
)
redis_conn = redis.Redis(connection_pool=pool)

# Blockchain Node
url = config.get('node', 'url')
abi_file = config.get('erc20', 'abi')
confirmation_threshold = int(config.get('erc20', 'confirmations'))

logs_directory = config.get('block', 'logs')
category = config.get('block', 'category')

def block_crawler():

    obj_logger = MyLogger(logs_directory, category)

    obj_logger.msg_logger('Getting Block Numbers.....')
    current_block = int(rpc_request(obj_logger,'eth_blockNumber',[]).get('result',0),16)
    crawled_blocks = int(redis_conn.get('eth_erc_blocks_crawled') or 0)
    obj_logger.msg_logger('Crawled Block Number : %s'%(crawled_blocks))
    obj_logger.msg_logger('Current Block Number : %s' % (current_block))
    obj_logger.msg_logger('Pending : %s' % (current_block - crawled_blocks))

    if current_block > crawled_blocks:

        for block_number in range(crawled_blocks + 1,current_block + 1):
            obj_logger.msg_logger('#' * 100)
            obj_logger.msg_logger('Crawling Block : %s || Current Block : %s'%(block_number,current_block))
            obj_logger.msg_logger('Pending : %s' % (current_block - block_number))
            obj_logger.msg_logger('Start :%s'%(datetime.datetime.now()))

            # Increment Confirmations
            for tx_hash in redis_conn.smembers('eth_erc_pct_set'):
                tx_hash = tx_hash.decode('utf-8')
                data = find_sql_join(logger=obj_logger,
                    table_names=['user_master','erc_address_master','erc_transactions'],
                    filters = {'erc_transactions.tx_hash':tx_hash},
                    on_conditions = {'user_master.user_name':'erc_address_master.user_name','erc_address_master.address':'erc_transactions.to_address'},
                )

                if not data:
                    obj_logger.error_logger('>>>>>>>>>>> Data not found in SQL for tx_hash : %s'%(tx_hash))
                    continue

                confirmations =  data[0]['confirmations']
                notif_url = data[0]['notification_url']

                if confirmations < confirmation_threshold:
                    increment_sql(obj_logger, 'erc_transactions',{'tx_hash':tx_hash},'confirmations')
                    notif_params = {
                        'from_address': data[0]['from_address'],
                        'to_address': data[0]['to_address'],
                        'contract_address': data[0]['erc_transactions.contract_address'],
                        'tx_hash': tx_hash,
                        'bid_id': -1,
                        'confirmations': confirmations + 1,
                        'block_number': data[0]['block_number'],
                        'value': data[0]['value'],
                        'flag': 'erc20'
                    }
                    obj_logger.msg_logger('>>>>>>>> Sending Confirmation : %s || %s' % (confirmations + 1, tx_hash))
                    send_notification(obj_logger, notif_url, notif_params,queue=config.get('hook_main', 'queue'))
                else:
                    obj_logger.msg_logger('>>>>>>>> %s Confirmation Sent : %s' %(confirmation_threshold,  tx_hash))
                    obj_logger.msg_logger('>>>>>>>> Removing from eth_erc_pct_set : %s' %(tx_hash))
                    redis_conn.srem('eth_erc_pct_set', tx_hash)

            # Crawling Blocks
            block_info = rpc_request(obj_logger,'eth_getBlockByNumber',[hex(int(block_number)), True])
            if block_info:
                block_transactions = block_info.get('result', {}).get('transactions', [])
            else:
                block_transactions = []
                obj_logger.error_logger('Data not found for block number : %s' %str(block_number))

            for tx in block_transactions:

                contract_address = tx['to']
                if (redis_conn.sismember('eth_erc_aw_set',contract_address)):

                    tx_hash = tx['hash']
                    obj_logger.msg_logger('>>>>>>>> Transaction Found in Block : %s : %s' % (block_number,tx_hash))

                    confirmations = 1
                    block_number = int(tx['blockNumber'], 16)

                    # Check if 1 Confirmation is sent from mempool crawler - Should be found in eth_erc_pct_set
                    if not redis_conn.sismember('eth_erc_pct_set',tx_hash):

                        from_address = tx['from']
                        bid_id = -1
                        flag = 'erc20'
                        sys_timestamp = datetime.datetime.now()

                        # Decoding Inputs
                        input = tx['input']
                        with open(abi_file, 'r') as abi_definition:
                            abi = json.load(abi_definition)
                        contract_obj = web3.Web3().eth.contract(address=web3.Web3().toChecksumAddress(contract_address), abi=abi)
                        params = contract_obj.decode_function_input(input)
                        to_address = params[1].get('_to')
                        value = params[1].get('_value')

                        # Check if 0 Confirmation is sent from mempool crawler - Should be found in eth_erc_zct_set
                        if redis_conn.sismember('eth_erc_zct_set',tx_hash):
                            update_sql(obj_logger, 'erc_transactions',{'tx_hash':tx_hash},updated_values={'confirmations':confirmations,'block_number':block_number})
                        else: # Missed in Mempool - Send 1 Confirmation and add in eth_erc_pct_set
                            obj_logger.msg_logger('>>>>>>>> Transaction Missed from mempool. Sending %s confirmation : %s' % (confirmations, str(tx_hash)))
                            data = {
                                'from_address': from_address,
                                'to_address': to_address,
                                'contract_address': contract_address,
                                'tx_hash': tx_hash,
                                'bid_id': bid_id,
                                'confirmations': confirmations,
                                'block_number': block_number,
                                'value': value,
                                'flag': flag,
                                'sys_timestamp': sys_timestamp,
                            }
                            insert_sql(obj_logger,'erc_transactions', data)

                        notif_url = find_sql_join(logger=obj_logger,
                            table_names = ['user_master','erc_address_master'],
                            filters = {'erc_address_master.address':to_address},
                            on_conditions= {'user_master.user_name':'erc_address_master.user_name'},
                            columns = ['user_master.notification_url']
                        )[0]['notification_url']

                        notif_params = {
                            'from_address': from_address,
                            'to_address': to_address,
                            'contract_address': contract_address,
                            'tx_hash': tx_hash,
                            'bid_id': -1,
                            'confirmations': confirmations,
                            'block_number': block_number,
                            'value': value,
                            'flag': 'erc20'
                        }
                        obj_logger.msg_logger('>>>>>>>> Sending Confirmation : %s || %s' % (confirmations, tx_hash))
                        send_notification(obj_logger, notif_url, notif_params,queue=config.get('hook_main', 'queue'))
                        obj_logger.msg_logger('>>>>>>>> Adding to eth_erc_pct_set : %s' % (tx_hash))
                        redis_conn.sadd('eth_erc_pct_set', tx_hash.encode('utf-8'))

            # Increment Redis Blocks Crawled
            redis_conn.set('eth_erc_blocks_crawled',block_number)
            obj_logger.msg_logger('Ends :%s' % (datetime.datetime.now()))
            obj_logger.msg_logger('#' * 100)
    else:
        obj_logger.msg_logger('#'*100)


def main():
    try:
        sched = BlockingScheduler(timezone='Asia/Kolkata')
        sched.add_job(block_crawler, 'interval', id='my_job_id', seconds=3)
        sched.start()
    except Exception as e:
        obj_logger = MyLogger(logs_directory, category)
        obj_logger.error_logger('Main : %s' % (e))


if __name__ == '__main__':
    main()
