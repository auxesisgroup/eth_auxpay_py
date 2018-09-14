import json
import web3
import models
import exception_str
import custom_exception
import common_util

# Node URL
abi_file = common_util.config.get('erc20', 'abi')
gas_limit = int(common_util.config.get('erc20', 'gas_limit'))

# Log
log = 'erc_end_points'
logs_directory, category= common_util.get_config(log)

# Common Methods for eth and erc
obj_common = common_util.CommonUtil(log=log)

def create_contract_object(abi_file,contract_address):
    """
    Create Contract Object
    """
    try:
        with open(abi_file, 'r') as abi_definition:
            abi = json.load(abi_definition)
        return web3.Web3().eth.contract(address=contract_address, abi=abi)
    except Exception as e:
        obj_logger = common_util.MyLogger(logs_directory, category)
        obj_logger.error_logger('Error create_contract_object : ' + str(e))
        raise custom_exception.UserException(exception_str.UserExceptionStr.bad_request)


def get_token_balance(user_name, user_address,contract_address):
    """
    To Get user token Balance
    """
    try:

        # Check if the address correspond to the user
        obj_logger = common_util.MyLogger(logs_directory, category)
        if not models.find_sql(logger=obj_logger, table_name='erc_address_master',filters={'user_name': user_name, 'address': user_address}):
            raise custom_exception.UserException(exception_str.UserExceptionStr.not_user_address)

        # Create Contract Object
        obj_contract = create_contract_object(abi_file,contract_address)

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
        response = obj_common.rpc_request(common_util.url, method, params)
        result = response['result']
        return int(result,16)

    except custom_exception.UserException:
        raise
    except web3.exceptions.ValidationError as e:
        obj_logger = common_util.MyLogger(logs_directory, category)
        obj_logger.error_logger('Error get_balance : ' + str(e))
        raise custom_exception.UserException(exception_str.UserExceptionStr.input_params_wrong)
    except Exception as e:
        obj_logger = common_util.MyLogger(logs_directory, category)
        obj_logger.error_logger('Error get_balance : ' + str(e))
        raise custom_exception.UserException(exception_str.UserExceptionStr.bad_request)


def get_fee():
    """
    To Get Token Transfer Fee
    """
    try:
        # Create connection
        con = obj_common.blockchain_connection(common_util.url)
        # RPC
        gas_price = con.eth.gasPrice
        fee = gas_limit*gas_price
        return fee

    except Exception as e:
        obj_logger = common_util.MyLogger(logs_directory, category)
        obj_logger.error_logger('Error get_fee : ' + str(e))
        raise custom_exception.UserException(exception_str.UserExceptionStr.bad_request)


def sign_transaction(from_address, to_address, value, contract_address, private_key):
    """
    For Signing Transaction
    """
    try:
        con = obj_common.blockchain_connection(common_util.url)
        obj_contract = create_contract_object(abi_file, contract_address)

        data = obj_contract.encodeABI('transfer', args=[to_address, value])

        # Sign TODO - Confirm Bid ID
        transaction = {
            'from': from_address,
            'to': contract_address,
            'data': data,
            'gas': gas_limit,
            'gasPrice': con.eth.gasPrice,
            'nonce': con.eth.getTransactionCount(web3.Web3().toChecksumAddress(from_address)),
        }

        signed = web3.Account.signTransaction(transaction, private_key)
        return signed.rawTransaction.hex()

    except Exception as e:
        obj_logger = common_util.MyLogger(logs_directory, category)
        obj_logger.error_logger('Error sign_transaction : ' + str(e))
        raise custom_exception.UserException(exception_str.UserExceptionStr.bad_request)


def transfer_token(user_name, token, from_address, to_address, value ,contract_address):
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
        eth_balance_wei = obj_common.get_ether_balance(from_address)

        # Transaction Fee in
        tx_fee_wei = get_fee()

        # Check if the transaction fee > eth_balance
        if tx_fee_wei > eth_balance_wei:
            raise custom_exception.UserException(exception_str.UserExceptionStr.insufficient_funds_ether)

        # Decrypt Private Key
        obj_logger = common_util.MyLogger(logs_directory, category)
        enc_private_key = models.find_sql(logger=obj_logger, table_name='address_master',filters={'user_name': user_name, 'address': from_address})[0]['private_key']
        if not enc_private_key:
            raise custom_exception.UserException(exception_str.UserExceptionStr.not_user_address)
        private_key = common_util.AESCipher(token, log).decrypt(enc_private_key)

        # Create Transaction Sign
        sign = sign_transaction(from_address=from_address, to_address=to_address, value=value, contract_address=contract_address, private_key=private_key)

        # Create Raw Transaction
        method = 'eth_sendRawTransaction'
        params = [sign]
        response = obj_common.rpc_request(common_util.url, method, params)
        tx_hash = response.get('result','')

        if not tx_hash:
            raise custom_exception.UserException(exception_str.UserExceptionStr.some_error_occurred)

        return tx_hash

    except custom_exception.UserException:
        raise
    except web3.exceptions.ValidationError as e:
        obj_logger = common_util.MyLogger(logs_directory, category)
        obj_logger.error_logger('Error transfer_token : ' + str(e))
        raise custom_exception.UserException(exception_str.UserExceptionStr.input_params_wrong)
    except Exception as e:
        obj_logger = common_util.MyLogger(logs_directory, category)
        obj_logger.error_logger('Error transfer_token : ' + str(e))
        raise custom_exception.UserException(exception_str.UserExceptionStr.bad_request)
# ERC 20 - ends