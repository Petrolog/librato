__author__ = 'Cesar'

#-------------------------------------------------------------------------------
# Name:        main
# Purpose:
#
# Author:      Cesar
#
# Created:     02/12/2014
# Copyright:   (c) Petrolog 2014
# Licence:     <your licence>
#-------------------------------------------------------------------------------

import threading
import logging
import omnimeter

# logging.basicConfig(format='%(asctime)s - [%(levelname)s]: %(message)s',
#                     filename='/home/ec2-user/logs/librato.log',
#                     level=logging.INFO)

logging.basicConfig(format='%(asctime)s - [%(levelname)s]: %(message)s',
                    filename='/Users/Cesar/logs/librato.log',
                    level=logging.INFO)


apiClientDaemon = threading.Thread(target=omnimeter.apiClientDaemon)
apiClientDaemon.daemon = True
apiClientDaemon.start()


while True:
    a = 0  # Do nothing
