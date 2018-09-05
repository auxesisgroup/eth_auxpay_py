import os
import math
import json
import web3
import models
import exception_str
import custom_exception
import common_util
import hashlib

# Encryption
l1_start = int(common_util.config.get('encryption', 'l1_start'))
l1_end = int(common_util.config.get('encryption', 'l1_end'))
l2_start = int(common_util.config.get('encryption', 'l2_start'))
l2_end = int(common_util.config.get('encryption', 'l2_end'))

# Gas Limit
gas_limit = int(common_util.config.get('eth', 'gas_limit'))

# Log
log = 'eth_end_points'
logs_directory, category = common_util.get_config(log)

# Common Methods for eth and erc
obj_common = common_util.CommonUtil(log=log)

def generate_key(token):
    """
    This method is used for creating key for aes cipher
    :param input: token number
    :return: sha256 of the input
    """
    try:
        token_key = hashlib.sha256(token.encode()).hexdigest()
        l1_token_key = token_key[:l1_start] + token_key[l1_end:]
        l2_token_key = hashlib.sha256(l1_token_key.encode()).hexdigest()
        l2_token_key = l2_token_key[l2_start:l2_end]
        return l2_token_key
    except Exception as e:
        obj_logger = common_util.MyLogger(logs_directory, category)
        obj_logger.error_logger("Error generate_key : " + str(e))
        raise custom_exception.UserException(exception_str.UserExceptionStr.some_error_occurred)


def encrpyt_private_key(token,secret):
    """
    Encryption of key
    :param password:
    :return:
    """
    try:
        key = generate_key(token)
        enc_sk = common_util.AESCipher(key,log).encrypt(secret)
        return enc_sk
    except Exception as e:
        obj_logger = common_util.MyLogger(logs_directory, category)
        obj_logger.error_logger("Error encrypt_password : " + str(e))
        raise custom_exception.UserException(exception_str.UserExceptionStr.some_error_occurred)


def decrpyt_private_key(token, enc_sk):
    """
    Decryption of key
    :param password:
    :param enc_pass:
    :return:
    """
    try:
        key = generate_key(token)
        dec_sk = common_util.AESCipher(key,log).decrypt(enc_sk)
        return dec_sk
    except Exception as e:
        obj_logger = common_util.MyLogger(logs_directory, category)
        obj_logger.error_logger("Error decrypt_password : " + str(e))
        raise custom_exception.UserException(exception_str.UserExceptionStr.some_error_occurred)


def get_fee(unit='ether'):
    """
    To Get Token Transfer Fee
    """
    try:
        # Create connection
        con = obj_common.blockchain_connection(common_util.url)
        # RPC
        gas_price = con.eth.gasPrice
        fee = web3.Web3().fromWei(gas_limit*gas_price,unit=unit)
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
        enc_pk = encrpyt_private_key(token,private_key)

        # Insert in DB
        models.insert_sql(
            logger=obj_logger,
            table_name='address_master',
            data={
                'user_name': user_name,
                'address': address,
                'private_key': enc_pk,
            })

        enc_pk = models.find_sql(obj_logger,'address_master',{'user_name':user_name,'address':address})[0]['private_key']
        dec_pk = decrpyt_private_key(token, enc_pk)

        print('@'*100)
        print('crypto_secure : ',crypto_secure)
        print('private_key : ', private_key)
        print('enc_pk : ',enc_pk)
        print('dec_pk : ', dec_pk)
        print('@' * 100)

        # Check if encryption algorithm pass
        if private_key != dec_pk:
            raise custom_exception.UserException(exception_str.UserExceptionStr.some_error_occurred)

        return address

    except custom_exception.UserException:
        raise
    except Exception as e:
        obj_logger.error_logger('Error get_balance : ' + str(e))
        raise custom_exception.UserException(exception_str.UserExceptionStr.bad_request)