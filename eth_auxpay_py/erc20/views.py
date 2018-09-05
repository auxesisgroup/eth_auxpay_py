# -*- coding: utf-8 -*-
from __future__ import unicode_literals
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
import exception_str
import custom_exception
import common_util
from . import util


obj_common = common_util.CommonUtil(log=util.log)

@csrf_exempt
@obj_common.who_is_hitting
@obj_common.valid_user
def get_balance(request):
    """
    End point for Getting Balance
    :param request: user_name, token, address, contract_address
    :return:
    """
    if request.method == 'POST':
        try:
            user_name = request.POST.get('user_name')
            token = request.POST.get('token')
            user_address = request.POST.get('address')
            contract_address = request.POST.get('contract_address')

            # Server Side Checks
            common_util.check_if_present(user_name, token, user_address, contract_address)

            # Get Balance
            balance = util.get_token_balance(user_name, user_address, str(contract_address))
            return JsonResponse({'balance' : str(balance),'status':200})

        except custom_exception.UserException as e:
            return JsonResponse({'error ': str(e), 'status': 400})
        except Exception as e:
            obj_logger = common_util.MyLogger(util.logs_directory, util.category)
            obj_logger.error_logger('get_balance : %s'%(str(e)))
            return JsonResponse({'error ': exception_str.UserExceptionStr.bad_request, 'status': 400})


@csrf_exempt
@obj_common.who_is_hitting
@obj_common.valid_user
def get_fee(request):
    """
    End Point for Getting Token Transfer fee
    :param request: user_name, token
    :return:
    """
    if request.method == 'POST':
        try:

            user_name = request.POST.get('user_name')
            token = request.POST.get('token')

            # Server Side Checks
            common_util.check_if_present(user_name, token)

            # Get Balance
            fee = util.get_fee()
            return JsonResponse({'fee' : str(fee),'status':200})

        except custom_exception.UserException as e:
            return JsonResponse({'error ': str(e), 'status': 400})

        except Exception as e:
            obj_logger = common_util.MyLogger(util.logs_directory, util.category)
            obj_logger.error_logger('get_balance : %s' % (str(e)))
            return JsonResponse({'error ': exception_str.UserExceptionStr.bad_request, 'status': 400})


@csrf_exempt
@obj_common.who_is_hitting
@obj_common.valid_user
def forward_token(request):
    """
    End point for forwarding Token
    :param request: user_name, token, from_address, to_address, value, contract_address
    :return:
    """
    if request.method == 'POST':
        try:
            user_name = request.POST.get('user_name')
            token = request.POST.get('token')
            from_address = request.POST.get('from_address')
            to_address = request.POST.get('to_address')
            value = request.POST.get('value')
            contract_address = request.POST.get('contract_address')

            # Server Side Checks
            common_util.check_if_present(user_name, token, from_address, to_address, value, contract_address)

            # Transfer Token
            tx_hash = util.transfer_token(user_name = user_name, from_address=from_address, to_address=to_address, value=int(value), contract_address=contract_address)

            return JsonResponse({'tx_status':'Initiated','tx_hash' : str(tx_hash), 'status':200})

        except custom_exception.UserException as e:
            return JsonResponse({'error ': str(e), 'status': 400})

        except Exception as e:
            obj_logger = common_util.MyLogger(util.logs_directory, util.category)
            obj_logger.error_logger('get_balance : %s' % (str(e)))
            return JsonResponse({'error ': exception_str.UserExceptionStr.bad_request, 'status': 400})