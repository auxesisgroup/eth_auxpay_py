# -*- coding: utf-8 -*-
from __future__ import unicode_literals
from django.views.decorators.csrf import csrf_exempt
from django.http import JsonResponse
import exception_str
import custom_exception
import common_util
from . import util
import web3

obj_common = common_util.CommonUtil(log=util.log)


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
            fee = web3.Web3.fromWei(util.get_fee(),unit='ether')
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

            # Server Side Checks
            common_util.check_if_present(user_name, token, user_address)

            # Get Balance
            balance = util.get_ether_balance(user_name, user_address)
            balance = web3.Web3.fromWei(balance, unit='ether')
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
def generate_address(request):
    """
    End point for Getting Balance
    :param request: user_name, token, address, contract_address
    :return:
    """
    if request.method == 'POST':
        try:
            user_name = request.POST.get('user_name')
            token = request.POST.get('token')

            # Server Side Checks
            common_util.check_if_present(user_name, token)

            # Generate address
            address = util.generate_address(user_name, token)

            return JsonResponse({'address' : str(address),'status':200})

        except custom_exception.UserException as e:
            return JsonResponse({'error ': str(e), 'status': 400})
        except Exception as e:
            obj_logger = common_util.MyLogger(util.logs_directory, util.category)
            obj_logger.error_logger('get_balance : %s'%(str(e)))
            return JsonResponse({'error ': exception_str.UserExceptionStr.bad_request, 'status': 400})


@csrf_exempt
@obj_common.who_is_hitting
@obj_common.valid_user
def forward_ethereum(request):
    """
    End point for Forward Ethereum
    :param request: user_name, token, address, contract_address
    :return:
    """
    if request.method == 'POST':
        try:
            user_name = request.POST.get('user_name')
            token = request.POST.get('token')
            from_address = request.POST.get('from_address')
            to_address = request.POST.get('to_address')
            value = request.POST.get('value')

            # Server Side Checks
            common_util.check_if_present(user_name, token, from_address, to_address, value)

            # Generate address
            address = util.forward_etherum(user_name, token, from_address, to_address, value)

            return JsonResponse({'address' : str(address),'status':200})

        except custom_exception.UserException as e:
            return JsonResponse({'error ': str(e), 'status': 400})
        except Exception as e:
            obj_logger = common_util.MyLogger(util.logs_directory, util.category)
            obj_logger.error_logger('get_balance : %s'%(str(e)))
            return JsonResponse({'error ': exception_str.UserExceptionStr.bad_request, 'status': 400})