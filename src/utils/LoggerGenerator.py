import logging


class LoggerGenerator:
    logger_count = 0
    global_log_level = logging.WARNING

    @staticmethod
    def get_logger(logger_name, create_file=False, log_level=None):
        if log_level is None:
            log_level = LoggerGenerator.global_log_level

        # create logger for prd_ci
        log = logging.getLogger(f"lg_{LoggerGenerator.logger_count}_{logger_name}")
        log.setLevel(level=log_level)
        LoggerGenerator.logger_count += 1

        # create formatter and add it to the handlers
        log_format = '[%(asctime)s %(name)s %(levelname)s] %(message)s [%(pathname)s:%(lineno)d in %(funcName)s]'
        formatter = logging.Formatter(log_format)

        if create_file:
            # create file handler for logger.
            fh = logging.FileHandler('SPOT.log')
            fh.setLevel(level=log_level)
            fh.setFormatter(formatter)
            log.addHandler(fh)

        # create console handler for logger.
        ch = logging.StreamHandler()
        ch.setLevel(level=log_level)
        ch.setFormatter(formatter)
        log.addHandler(ch)

        return log

