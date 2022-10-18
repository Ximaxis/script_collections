import datetime
import logging
from TGbot import send_msg_developer
from urllib3.exceptions import NewConnectionError
from services import start_database, main

logging.basicConfig(filename='log.log',
                    filemode='a',
                    format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                    datefmt='%H:%M:%S',
                    level=logging.INFO)

logger = logging.getLogger('CollectionMainScript')


if __name__ == '__main__':
    logger.warning(f"Start {datetime.datetime.now()}")
    start_database()
    try:
        main()
    except NewConnectionError as e:
        send_msg_developer(f"Connection ERROR: {e}")
    except Exception as e:
        logger.error(f"Fatal ERROR: {e}")
        send_msg_developer(f"Fatal ERROR: {datetime.datetime.now()}")
