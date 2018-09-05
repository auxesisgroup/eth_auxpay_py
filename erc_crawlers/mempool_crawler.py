import json
import datetime
import redis
from apscheduler.schedulers.blocking import BlockingScheduler
import web3
from util import insert_sql, rpc_request, send_notification, find_sql_join, MyLogger, config

# Redis Connection
pool = redis.ConnectionPool(
    host = config.get('redis', 'host'),
    port = int(config.get('redis', 'port')),
    db = int(config.get('redis', 'db'))
)
redis_conn = redis.Redis(connection_pool=pool)

# Blockchain Node
abi_file = config.get('erc20', 'abi')
logs_directory = config.get('mempool', 'logs')
category = config.get('mempool', 'category')


def mempool_crawler():

    obj_logger = MyLogger(logs_directory,category)

    obj_logger.msg_logger('#'*100)
    obj_logger.msg_logger('Getting Mempool Data')
    mempool_transaction_data = rpc_request(obj_logger, 'eth_getBlockByNumber', ['pending', True]).get('result',{}).get('transactions',[])

    obj_logger.msg_logger('Crawling Mempool Starts')

    for tx in mempool_transaction_data:

        tx_hash = tx['hash']
        contract_address = tx['to'] # To address in ERC 20 Transaction is Contract Address

        # Redis Check
        if (not redis_conn.sismember('eth_erc_zct_set',tx_hash)) and (redis_conn.sismember('eth_erc_aw_set',contract_address)):
            obj_logger.msg_logger('>>>>>>>> Transaction Found in Mempool : %s'%(tx_hash))

            from_address = tx['from']
            bid_id = -1
            confirmations = 0
            block_number = -1
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

            # Insert in DB
            result = insert_sql(
                logger=obj_logger,
                table_name= 'erc_transactions',
                data={
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
            )

            if result:
                notif_url = find_sql_join(logger=obj_logger,
                    table_names=['user_master', 'erc_address_master'],
                    filters={'erc_address_master.address': to_address},
                    on_conditions={'user_master.user_name': 'erc_address_master.user_name'},
                    columns=['user_master.notification_url']
                )[0]['notification_url']

                notif_params = {
                    'from_address': from_address,
                    'to_address': to_address,
                    'contract_address': contract_address,
                    'tx_hash': tx_hash,
                    'bid_id': bid_id,
                    'confirmations': confirmations,
                    'block_number': block_number,
                    'value': value,
                    'flag': flag
                }
                send_notification(obj_logger,notif_url,notif_params,queue=config.get('hook_main', 'queue'))
                obj_logger.msg_logger('>>>>>>>> Adding to eth_erc_zct_set : %s' % (tx_hash))
                redis_conn.sadd('eth_erc_zct_set', tx_hash.encode('utf-8')) # To cross check in Block Crawler and not to send multiple notification

    obj_logger.msg_logger('Crawling Mempool Ends')
    obj_logger.msg_logger('#' * 100)


def main():
    try:
        sched = BlockingScheduler(timezone='Asia/Kolkata')
        sched.add_job(mempool_crawler, 'interval', id='my_job_id', seconds=3)
        sched.start()
    except Exception as e:
        obj_logger = MyLogger(logs_directory,category)
        obj_logger.error_logger('Main : %s'%(e))


if __name__ == '__main__':
    main()
