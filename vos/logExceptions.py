import logging
import traceback


def logExceptions():
    """
    A decorator which catches and logs exceptions.
    """

    def decorator(func):
        def wrapper(*args, **kwds):
            try:
                return func(*args, **kwds)
            except Exception as e:
                logger = logging.getLogger('exceptions')
                logger.error("Exception throw: %s %s" % (type(e), str(e)))
                logger.error(traceback.format_exc())
                raise
        return wrapper
    return decorator
