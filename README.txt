Copy Conf.ini file in /var/eth_conf/
Create Directory : /var/log/eth_logs
Give it 777 access

Create Directory for logs : 
	/var/log/eth_logs/erc_end_points/
	/var/log/eth_logs/erc_mempool/
	/var/log/eth_logs/erc_block/
	/var/log/eth_logs/hook_main/
	/var/log/eth_logs/hook_exception/


Apache2 config

/etc/apache2/sites-enabled/000-default.conf

<VirtualHost *:80>

        ServerName eth_auxpay.org
        ServerAlias www.eth_auxpay.org
        ServerAdmin Jitender.Bhutani@auxesisgroup.com

        WSGIDaemonProcess api
        WSGIScriptAlias /api /var/www/eth_auxpay_py/eth_auxpay_py/wsgi.py process-group=api application-group=%{GLOBAL}

        ErrorLog /var/www/logs/error.logs
        CustomLog /var/www/logs/custom.log combined

</VirtualHost>


