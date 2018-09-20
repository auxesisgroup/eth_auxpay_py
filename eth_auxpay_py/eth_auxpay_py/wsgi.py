"""
WSGI config for eth_auxpay_py project.

It exposes the WSGI callable as a module-level variable named ``application``.

For more information on this file, see
https://docs.djangoproject.com/en/1.11/howto/deployment/wsgi/
"""

import os
import sys

sys.path.append('/var/www')
sys.path.append('/var/www/eth_auxpay_py')
sys.path.append('/usr/local/lib/python3.6')
sys.path.append('/usr/local/lib/python3.6/dist-packages/mod_wsgi/server')
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "eth_auxpay_py.settings")

from django.core.wsgi import get_wsgi_application
application = get_wsgi_application()