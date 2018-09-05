import os
import datetime
import json
import web3
import requests
from . import exception_str,models,custom_exception
import configparser
import logging
import sys
from django.http import JsonResponse

# Config
conf_file = r'/var/eth_conf/conf.ini'
config = configparser.RawConfigParser()
config.read(conf_file)

# Node URL
url = config.get('node', 'url')
abi_file = config.get('erc20', 'abi')
gas_limit = int(config.get('erc20', 'gas_limit'))

# Log
category = config.get('end_points', 'category')
logs_directory = config.get('end_points', 'logs')

# for RPC Request
headers = {'Content-type': 'application/json'}
payload = {"jsonrpc": "2.0", "id": 1}

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
        self.logger.info('#'*100)
        self.logger.info(msg)
        self.logger.info('#' * 100)


    def error_logger(self,error):
        self.logger.error('#' * 100)
        self.logger.error(error)
        self.logger.error('#' * 100)

        models.insert_sql(self,'error_logs', {
            'category': self.category,
            'file_name': os.path.basename(__file__),
            'error': error,
            'timestamp': datetime.datetime.now()
        })


def get_client_ip(request):
    """
    To Get Client IP
    """
    x_forwarded_for = request.META.get('HTTP_X_FORWARDED_FOR')
    if x_forwarded_for:
        ip = x_forwarded_for.split(',')[0]
    else:
        ip = request.META.get('REMOTE_ADDR')
    return ip


def who_is_hitting(func):
    """
    Decorator : To check who is hitting the end points
    """
    def user_details(*args,**kwargs):
        try:
            # Before
            request = args[0]
            time = datetime.datetime.now()
            ip = get_client_ip(request)
            url = request.build_absolute_uri()
            type = request.method
            header = ''
            body = ''
            if request.method == 'POST':
                for key,value in request.POST.items():
                    body += key + " = " + value + ", "
            elif request.method == 'GET':
                for key,value in request.GET.items():
                    body += key + " = " + value + ", "

            # Main
            response = func(*args,**kwargs)
            response_data = str(response.content)

            # Insert in DB
            obj_logger = MyLogger(logs_directory, category)
            models.insert_sql(obj_logger, 'server_logs',{
                'requestIP':str(ip),
                'url':str(url),
                'type':type,
                'headers': header,
                'body': body[:-2] if body else '',
                'response':response_data,
                'timestamp':time
            })
            return response
        except Exception as e:
            obj_logger.error_logger('who_is_hitting : %s'%(e))
            return JsonResponse({'error ': exception_str.UserExceptionStr.bad_request, 'status': 200})
    return user_details


def valid_user(func):

    def validate_user(*args,**kwargs):
        try:
            request = args[0]
            user_name = request.POST.get('user_name')
            token = request.POST.get('token')

            if not (user_name and token):
                return JsonResponse({'error ': exception_str.UserExceptionStr.specify_required_fields, 'status': 200})

            obj_logger = MyLogger(logs_directory, category)

            if models.find_sql(logger=obj_logger, table_name='user_master', filters= {'user_name':user_name,'token': token}):
                return func(*args,**kwargs)
            else:
                return JsonResponse({'error ': exception_str.UserExceptionStr.invalid_user, 'status': 200})

        except Exception as e:
            obj_logger.error_logger('validate_user : %s'%(e))
            return JsonResponse({'error ': exception_str.UserExceptionStr.bad_request, 'status': 200})


    return validate_user


def check_if_present(*args):
    """
    For Server Side Checks
    """
    if not all(arg for arg in args):
        raise custom_exception.UserException(exception_str.UserExceptionStr.specify_required_fields)


# ERC 20 - starts
def rpc_request(url,method,params):
    """
    Custom RPC Method
    """
    try :
        payload['method'] = method
        payload['params'] = params
        response = requests.post(url, data=json.dumps(payload), headers=headers)
        return json.loads(response.text)
    except Exception as e:
        obj_logger = MyLogger(logs_directory, category)
        obj_logger.error_logger('rpc_request : ' + str(e))
        raise custom_exception.UserException(exception_str.UserExceptionStr.bad_request)


def blockchain_connection(url):
    """
    Connect To Blockchain Node
    """
    try:
        provider = web3.Web3().HTTPProvider(url)
        return web3.Web3(providers=[provider])
    except Exception as e:
        obj_logger = MyLogger(logs_directory, category)
        obj_logger.error_logger('Error blockchain_connection : ' + str(e))
        raise custom_exception.UserException(exception_str.UserExceptionStr.bad_request)


def create_contract_object(con,abi_file,contract_address):
    """
    Create Contract Object
    """
    try:
        with open(abi_file, 'r') as abi_definition:
            abi = json.load(abi_definition)
        return con.eth.contract(address=contract_address, abi=abi)
    except Exception as e:
        obj_logger = MyLogger(logs_directory, category)
        obj_logger.error_logger('Error create_contract_object : ' + str(e))
        raise custom_exception.UserException(exception_str.UserExceptionStr.bad_request)


def get_token_balance(user_name, user_address,contract_address):
    """
    To Get user token Balance
    """
    try:
        # Create Contract Object
        con = blockchain_connection(url)
        obj_contract = create_contract_object(con,abi_file,contract_address)

        # Check if the address correspond to the user
        obj_logger = MyLogger(logs_directory, category)
        if not models.find_sql(logger=obj_logger, table_name='erc_address_master',filters={'user_name': user_name, 'address': user_address}):
            raise custom_exception.UserException(exception_str.UserExceptionStr.not_user_address)

        # RPC
        method = 'eth_call'
        data = obj_contract.encodeABI('balanceOf', args=[user_address])
        params = [
            {
                'to': contract_address,
                'data': data
            }
            ,"latest"
        ]
        response = rpc_request(url, method, params)
        result = response['result']
        return int(result,16)

    except custom_exception.UserException:
        raise
    except web3.exceptions.ValidationError as e:
        obj_logger = MyLogger(logs_directory, category)
        obj_logger.error_logger('Error get_balance : ' + str(e))
        raise custom_exception.UserException(exception_str.UserExceptionStr.input_params_wrong)
    except Exception as e:
        obj_logger = MyLogger(logs_directory, category)
        obj_logger.error_logger('Error get_balance : ' + str(e))
        raise custom_exception.UserException(exception_str.UserExceptionStr.bad_request)


def get_fee(unit='ether'):
    """
    To Get Token Transfer Fee
    """
    try:
        # Create connection
        con = blockchain_connection(url)
        # RPC
        gas_price = con.eth.gasPrice
        fee = web3.Web3().fromWei(gas_limit*gas_price,unit=unit)
        return fee

    except Exception as e:
        obj_logger = MyLogger(logs_directory, category)
        obj_logger.error_logger('Error get_fee : ' + str(e))
        raise custom_exception.UserException(exception_str.UserExceptionStr.bad_request)


def get_ether_balance(address, unit='ether'):
    """
    To get balance
    :param address:
    :return: balance in wei
    """
    try:
        # RPC
        method = 'eth_getBalance'
        params = [address, 'latest']
        response = rpc_request(url, method, params)
        return web3.Web3().fromWei(int(response['result'], 16), unit=unit)

    except Exception as e:
        obj_logger = MyLogger(logs_directory, category)
        obj_logger.error_logger('Error get_ether_balance : ' + str(e))
        raise custom_exception.UserException(exception_str.UserExceptionStr.bad_request)


def sign_transaction(from_address, to_address, value, contract_address, private_key):
    """
    For Signing Transaction
    """
    try:
        con = blockchain_connection(url)
        obj_contract = create_contract_object(con, abi_file, contract_address)

        data = obj_contract.encodeABI('transfer', args=[to_address, value])

        # Sign TODO - Confirm Bid ID
        transaction = {
            'from': from_address,
            'to': contract_address,
            'data': data,
            'gas': 210001,
            'gasPrice': con.eth.gasPrice,
            'nonce': con.eth.getTransactionCount(from_address),
        }

        signed = con.eth.account.signTransaction(transaction, private_key)
        return signed.rawTransaction.hex()

    except Exception as e:
        obj_logger = MyLogger(logs_directory, category)
        obj_logger.error_logger('Error sign_transaction : ' + str(e))
        raise custom_exception.UserException(exception_str.UserExceptionStr.bad_request)


def transfer_token(user_name, from_address, to_address, value ,contract_address):
    """
    To Transfer Token
    """
    try:

        # Get User Token Balance
        token_balance = get_token_balance(user_name, from_address, contract_address)

        # Check If Token Balance is less than value
        if value > token_balance:
            raise custom_exception.UserException(exception_str.UserExceptionStr.insufficient_tokens)

        # Get User Ether Balance in Wei
        eth_balance_wei = get_ether_balance(from_address)

        # Transaction Fee in
        tx_fee = get_fee()

        # Check if the transaction fee > eth_balance
        if tx_fee > eth_balance_wei:
            raise custom_exception.UserException(exception_str.UserExceptionStr.insufficient_funds_ether)

        # TODO - Encryption/Decryption
        obj_logger = MyLogger(logs_directory, category)
        private_key = models.find_sql(logger=obj_logger, table_name='address_master',filters={'address':from_address})[0]['private_key']

        # Create Transaction Sign
        sign = sign_transaction(from_address=from_address, to_address=to_address, value=value, contract_address=contract_address, private_key=private_key)

        # Create Raw Transaction
        method = 'eth_sendRawTransaction'
        params = [sign]
        response = rpc_request(url, method, params)
        tx_hash = response.get('result','')

        if not tx_hash:
            raise custom_exception.UserException(exception_str.UserExceptionStr.some_error_occurred)

        return tx_hash

    except custom_exception.UserException:
        raise
    except web3.exceptions.ValidationError as e:
        obj_logger = MyLogger(logs_directory, category)
        obj_logger.error_logger('Error transfer_token : ' + str(e))
        raise custom_exception.UserException(exception_str.UserExceptionStr.input_params_wrong)
    except Exception as e:
        obj_logger = MyLogger(logs_directory, category)
        obj_logger.error_logger('Error transfer_token : ' + str(e))
        raise custom_exception.UserException(exception_str.UserExceptionStr.bad_request)
# ERC 20 - ends