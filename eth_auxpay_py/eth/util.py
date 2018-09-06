import os
import math
import web3
import models
import exception_str
import custom_exception
import common_util

# Gas Limit
gas_limit = int(common_util.config.get('eth', 'gas_limit'))

# Log
log = 'eth_end_points'
logs_directory, category = common_util.get_config(log)

# Common Methods for eth and erc
obj_common = common_util.CommonUtil(log=log)


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


def get_ether_balance(user_name, user_address):
    """
    To Get user token Balance
    """
    try:
        # Check if the address correspond to the user
        obj_logger = common_util.MyLogger(logs_directory, category)
        if not models.find_sql(logger=obj_logger, table_name='address_master', filters={'user_name': user_name, 'address': user_address.lower()}):
            raise custom_exception.UserException(exception_str.UserExceptionStr.not_user_address)

        return obj_common.get_ether_balance(user_address)

    except custom_exception.UserException:
        raise
    except Exception as e:
        obj_logger = common_util.MyLogger(logs_directory, category)
        obj_logger.error_logger('Error get_balance : ' + str(e))
        raise custom_exception.UserException(exception_str.UserExceptionStr.bad_request)


def generate_address(user_name, token):
    try:
        # Logs
        obj_logger = common_util.MyLogger(logs_directory, category)

        # Generate Crypto Secure Number
        crypto_secure = ''.join([str(int(math.pow(number, 2))) for number in os.urandom(20)])

        # Generate Private Key
        private_key = web3.Web3().sha3(text=crypto_secure).hex()

        # Genrate public Address
        address = web3.Account.privateKeyToAccount(private_key).address.lower()

        # Encrypt PK
        enc_pk = common_util.AESCipher(token,log).encrypt(private_key)

        # Insert in DB
        models.insert_sql(
            logger=obj_logger,
            table_name='address_master',
            data={
                'user_name': user_name,
                'address': address,
                'private_key': enc_pk,
            })

        # Check if encryption algorithm pass
        enc_pk = models.find_sql(obj_logger,'address_master',{'user_name':user_name,'address':address})[0]['private_key']
        dec_pk = common_util.AESCipher(token,log).decrypt(enc_pk)
        if private_key != dec_pk:
            raise custom_exception.UserException(exception_str.UserExceptionStr.some_error_occurred)

        return address

    except custom_exception.UserException:
        raise
    except Exception as e:
        obj_logger.error_logger('Error get_balance : ' + str(e))
        raise custom_exception.UserException(exception_str.UserExceptionStr.bad_request)


def sign_transaction(from_address, to_address, value, private_key):
    """
    For Signing Transaction
    """
    try:

        con = obj_common.blockchain_connection(common_util.url)

        # Sign TODO - Confirm Bid ID
        transaction = {
            'to': to_address,
            'value': web3.Web3().toHex(value),
            'gas': gas_limit,
            'gasPrice': con.eth.gasPrice,
            'nonce': con.eth.getTransactionCount(from_address),
            'chainId' : 3,
        }

        signed = web3.Account.signTransaction(transaction, private_key)
        return signed.rawTransaction.hex()

    except Exception as e:
        obj_logger = common_util.MyLogger(logs_directory, category)
        obj_logger.error_logger('Error sign_transaction : ' + str(e))
        raise custom_exception.UserException(exception_str.UserExceptionStr.bad_request)


def forward_etherum(user_name, token, from_address, to_address, value):
    """
    To Transfer Ethereum
    """
    try:

        # Get User Ether Balance in Wei
        eth_balance_wei = obj_common.get_ether_balance(from_address)

        # Transaction Fee in
        tx_fee_wei = get_fee()

        # Check if the transaction fee > eth_balance
        if tx_fee_wei > eth_balance_wei:
            raise custom_exception.UserException(exception_str.UserExceptionStr.insufficient_funds_ether)

        # Decrypt Private Key
        obj_logger = common_util.MyLogger(logs_directory, category)
        enc_private_key = models.find_sql(logger=obj_logger, table_name='address_master', filters={'user_name': user_name, 'address':from_address})[0]['private_key']
        if not enc_private_key:
            raise custom_exception.UserException(exception_str.UserExceptionStr.not_user_address)
        private_key = common_util.AESCipher(token, log).decrypt(enc_private_key)

        # Create Transaction Sign
        sign = sign_transaction(from_address=from_address, to_address=to_address, value=value, private_key=private_key)

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