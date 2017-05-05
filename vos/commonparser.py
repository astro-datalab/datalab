
import logging
import optparse
import os
import sys
from __version__ import version



class CommonParser(optparse.OptionParser):
    """Class to hold and parse common command-line arguments for vos clients"""

    def __init__(self, *args, **kwargs):
        # call the parent constructor
        optparse.OptionParser.__init__(self, *args, **kwargs)

        # inherit the VOS client version
        self.version = version
        self.log_level = logging.ERROR

        # now add on the common parameters
        self.add_option("--certfile",
                        help="location of your CADC security certificate file",
                        default=os.path.join(os.getenv("HOME", "."),
                                             ".ssl/cadcproxy.pem"))
        self.add_option("--token",
                        help="token string (alternative to certfile)",
                        default=None)
        self.add_option("--version", action="store_true",
                        default=False,
                        help="Print the version (%s)" % version)
        self.add_option("-d", "--debug", action="store_true", default=False,
                        help="Print debug level log messages")
        self.add_option("--vos-debug", action="store_true", help="Turn on VOS module debugging")
        self.add_option("-v", "--verbose", action="store_true", default=False,
                        help="Print verbose level log messages")
        self.add_option("-w", "--warning", action="store_true", default=False,
                        help="Print warning level log messages")

    def process_informational_options(self):
        """Display version, set logging verbosity"""
        (opt, args) = self.parse_args()

        if opt.version:
            self.print_version()
            sys.exit(0)

        # Logger verbosity
        if opt.debug:
            self.log_level = logging.DEBUG
        elif opt.verbose:
            self.log_level = logging.INFO
        elif opt.warning:
            self.log_level = logging.WARNING
        else:
            self.log_level = logging.ERROR

        log_format = "%(levelname)s %(module)s %(message)s"
        if self.log_level < logging.INFO:
            log_format = ("%(levelname)s %(asctime)s %(thread)d vos-"+str(version)+" %(module)s.%(funcName)s.%(lineno)d %(message)s")
        logging.basicConfig(format=log_format, level=self.log_level)

        if opt.vos_debug:
            logger = logging.getLogger('vos')
            logger.setLevel(logging.DEBUG)

        if sys.version_info[1] > 6:
            logger = logging.getLogger()
            logger.addHandler(logging.NullHandler())




