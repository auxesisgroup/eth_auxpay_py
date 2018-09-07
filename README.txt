Copy Conf.ini file in /var/eth_conf/
Create Directory : /var/log/eth_logs
Give it 777 access

Create Directory for logs : 
	/var/log/eth_logs/erc_end_points/
	/var/log/eth_logs/erc_mempool/
	/var/log/eth_logs/erc_block/
	/var/log/eth_logs/hook_main/
	/var/log/eth_logs/hook_exception/

-------------------------------------------------------
*optional - sudo apt-get install python3-pip apache2 libapache2-mod-wsgi-py3
Apache2 config

sudo apt install apache2-dev
sudo python3.6 -m pip install mod_wsgi
------------------------------------------------------------------------
wsgi.py

import os
import sys

sys.path.append('/var/www')
sys.path.append('/var/www/eth_auxpay_py')
sys.path.append('/usr/local/lib/python3.6/dist-packages')

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "eth_auxpay_py.settings")

from django.core.wsgi import get_wsgi_application
application = get_wsgi_application()


