"""eth_auxpay_py URL Configuration

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/1.11/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  url(r'^$', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  url(r'^$', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.conf.urls import url, include
    2. Add a URL to urlpatterns:  url(r'^blog/', include('blog.urls'))
"""
from django.conf.urls import url
from . import views

app_name = 'eth'

urlpatterns = [
    url(r'get_fee/$', view=views.get_fee, name='get_fee'),
    url(r'get_balance/$', view=views.get_balance, name='get_balance'),
    url(r'generate_address/$', view=views.generate_address, name='generate_address'),
    url(r'forward_ethereum/$', view=views.forward_ethereum, name='forward_ethereum'),
]
