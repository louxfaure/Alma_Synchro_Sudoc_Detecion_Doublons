import os, logging
from logging.config import dictConfig

FORMAT = "%(asctime)s {app} [%(thread)d] %(levelname)-5s %(name)s - %(message)s. [file=%(filename)s:%(lineno)d]"
DATE_FORMAT = None

def setup_logging(name, level="INFO", fmt=FORMAT, log_dir='/tmp'):
    formatted = fmt.format(app=name)
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    logging_config = {
        "version": 1,
        'disable_existing_loggers': True,
        "formatters": {
            'standard': {
                'format': formatted
            },
            'brief': {
                'format': '%(asctime)s :: %(levelname)s :: %(message)s'
            }
        },
        "handlers": {
            'default': {
                'class': 'logging.StreamHandler',
                'formatter': 'standard',
                'level': level,
                'stream': 'ext://sys.stdout'
            },
            'file': {
                'class': 'logging.handlers.TimedRotatingFileHandler',
                'level': level,
                'formatter': 'standard',
                'filename': '{}/{}.log'.format(log_dir,name),
                'when' : "d",
                'interval' : 1,
                'backupCount': 5,
            },
            # Pour corps de message du mail de rapport
            'mail': {
                'class': 'logging.FileHandler',
                'level': level,
                'filename': '/tmp/{}.txt'.format(name),
                'formatter': 'brief',
                'mode': 'w',
            }
        },
        "loggers": {
            name : {
                'handlers': ['default', 'file','mail'],
                'level': level
            }
        }
    }

    dictConfig(logging_config)