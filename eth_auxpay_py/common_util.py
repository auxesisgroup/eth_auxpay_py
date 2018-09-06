import os
import datetime
import json
import web3
import requests
import models
import exception_str
import custom_exception
import configparser
import logging
import sys
import hashlib
import base64
from Crypto.Cipher import AES
from Crypto import Random
from django.http import JsonResponse

# Config
conf_file = '../conf.ini'
config = configparser.RawConfigParser()
config.read(conf_file)

# Node URL
url = config.get('node', 'url')

# Encryption
l1_start = int(config.get('encryption', 'l1_start'))
l1_end = int(config.get('encryption', 'l1_end'))
l2_start = int(config.get('encryption', 'l2_start'))
l2_end = int(config.get('encryption', 'l2_end'))

# for RPC Request
headers = {'Content-type': 'application/json'}
payload = {"jsonrpc": "2.0", "id": 1}


def get_config(log):
    return config.get(log, 'logs'),config.get(log, 'category')


def check_if_present(*args):
    """
    For Server Side Checks
    """
    if not all(arg for arg in args):
        raise custom_exception.UserException(exception_str.UserExceptionStr.specify_required_fields)


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

        models.insert_sql(self, 'error_logs', {
            'category': self.category,
            'file_name': os.path.basename(__file__),
            'error': error,
            'timestamp': datetime.datetime.now()
        })


class CommonUtil():

    def __init__(self, log):
        self.logs_directory , self.category = get_config(log)

    def get_client_ip(self, request):
        """
        To Get Client IP
        """
        x_forwarded_for = request.META.get('HTTP_X_FORWARDED_FOR')
        if x_forwarded_for:
            ip = x_forwarded_for.split(',')[0]
        else:
            ip = request.META.get('REMOTE_ADDR')
        return ip

    def who_is_hitting(self, func):
        """
        Decorator : To check who is hitting the end points
        """
        def user_details(*args,**kwargs):
            try:
                # Before
                request = args[0]
                time = datetime.datetime.now()
                ip = self. get_client_ip(request)
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
                obj_logger = MyLogger(self.logs_directory , self.category)
                models.insert_sql(obj_logger, 'server_logs', {
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
                return JsonResponse({'error ': exception_str.UserExceptionStr.bad_request, 'status': 400})
        return user_details

    def valid_user(self, func):
        """
        Calidate user Tokens
        :param func:
        :return:
        """

        def validate_user(*args,**kwargs):
            try:
                request = args[0]
                user_name = request.POST.get('user_name')
                token = request.POST.get('token')

                if not (user_name and token):
                    return JsonResponse({'error ': exception_str.UserExceptionStr.specify_required_fields, 'status': 400})

                obj_logger = MyLogger(self.logs_directory, self.category)

                if models.find_sql(logger=obj_logger, table_name='user_master', filters= {'user_name':user_name, 'token': token}):
                    return func(*args,**kwargs)
                else:
                    return JsonResponse({'error ': exception_str.UserExceptionStr.invalid_user, 'status': 400})

            except Exception as e:
                obj_logger.error_logger('validate_user : %s'%(e))
                return JsonResponse({'error ': exception_str.UserExceptionStr.bad_request, 'status': 400})


        return validate_user

    def rpc_request(self, url,method,params):
        """
        Custom RPC Method
        """
        try :
            payload['method'] = method
            payload['params'] = params
            response = requests.post(url, data=json.dumps(payload), headers=headers)
            return json.loads(response.text)
        except Exception as e:
            obj_logger = MyLogger(self.logs_directory , self.category)
            obj_logger.error_logger('rpc_request : ' + str(e))
            raise custom_exception.UserException(exception_str.UserExceptionStr.bad_request)

    def blockchain_connection(self, url):
        """
        Connect To Blockchain Node
        """
        try:
            provider = web3.Web3().HTTPProvider(url)
            return web3.Web3(providers=[provider])
        except Exception as e:
            obj_logger = MyLogger(self.logs_directory, self.category)
            obj_logger.error_logger('Error blockchain_connection : ' + str(e))
            raise custom_exception.UserException(exception_str.UserExceptionStr.bad_request)

    def get_ether_balance(self, address):
        """
        To get balance
        :param address:
        :return: balance in wei
        """
        try:
            # RPC
            method = 'eth_getBalance'
            params = [address, 'latest']
            response = self.rpc_request(url, method, params)
            return int(response['result'], 16)

        except Exception as e:
            obj_logger = MyLogger(self.logs_directory , self.category)
            obj_logger.error_logger('Error get_ether_balance : ' + str(e))
            raise custom_exception.UserException(exception_str.UserExceptionStr.bad_request)


class AESCipher():
    """
    AES Cipher Encryption
    Source : https://stackoverflow.com/questions/12524994/encrypt-decrypt-using-pycrypto-aes-256
    """

    def __init__(self, key, log):
        try:
            # Logs
            self.logs_directory, self.category = get_config(log)
            self.obj_logger = MyLogger(self.logs_directory, self.category)

            self.bs = 32
            self.key = self.generate_key(key)
            self.key = hashlib.sha256(self.key.encode()).digest()

        except Exception as e:
            if self.obj_logger : self.obj_logger.error_logger("Error AESCipher __init__ : " + str(e))
            raise custom_exception.UserException(exception_str.UserExceptionStr.some_error_occurred)

    def encrypt(self, raw):
        try:
            raw = self._pad(raw)
            raw = raw.encode('utf-8')
            iv = Random.new().read(AES.block_size)
            cipher = AES.new(self.key, AES.MODE_CBC, iv)
            return base64.b64encode(iv + cipher.encrypt(raw))
        except Exception as e:
            self.obj_logger.error_logger("Error AESCipher encrypt : " + str(e))
            raise custom_exception.UserException(exception_str.UserExceptionStr.some_error_occurred)

    def decrypt(self, enc):
        try:
            enc = base64.b64decode(enc)
            iv = enc[:AES.block_size]
            cipher = AES.new(self.key, AES.MODE_CBC, iv)
            return self._unpad(cipher.decrypt(enc[AES.block_size:])).decode('utf-8')
        except Exception as e:
            self.obj_logger.error_logger("Error AESCipher decrypt : " + str(e))
            raise custom_exception.UserException(exception_str.UserExceptionStr.some_error_occurred)

    def _pad(self, s):
        try:
            return s + (self.bs - len(s) % self.bs) * chr(self.bs - len(s) % self.bs)
        except Exception as e:
            self.obj_logger.error_logger("Error AESCipher _pad : " + str(e))
            raise custom_exception.UserException(exception_str.UserExceptionStr.some_error_occurred)

    def _unpad(self, s):
        try:
            return s[:-ord(s[len(s)-1:])]
        except Exception as e:
            self.obj_logger.error_logger("Error AESCipher _unpad : " + str(e))
            raise custom_exception.UserException(exception_str.UserExceptionStr.some_error_occurred)

    def generate_key(self, key):
        """
        This method is used for creating key for aes cipher
        :param input: token number
        :return: sha256 of the input
        """
        try:
            token_key = hashlib.sha256(key.encode()).hexdigest()
            l1_token_key = token_key[:l1_start] + token_key[l1_end:]
            l2_token_key = hashlib.sha256(l1_token_key.encode()).hexdigest()
            l2_token_key = l2_token_key[l2_start:l2_end]
            return l2_token_key
        except Exception as e:
            self.obj_logger.error_logger("Error generate_key : " + str(e))
            raise custom_exception.UserException(exception_str.UserExceptionStr.some_error_occurred)
