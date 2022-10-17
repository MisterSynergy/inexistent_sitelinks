import logging
import logging.config

logging.config.fileConfig('logging.conf')

from delsitelinks.tasks import main_tidy_sitelinks

main_tidy_sitelinks()