"""
Interactions with the CANFAR processing service.

set the environment variables 'CANFAR_PROC_SERVER' and 'CANFAR_PROC_ENDPOINT' to change which service this
module is looking at.
"""
import logging

import vos
from astropy.io import votable
import cStringIO
import os
import curses
from datetime import datetime
import warnings

DELAY = int(os.getenv('CANTOP_DELAY', 8))
RATE = int(os.getenv('CANTOP_RATE', 1))
PROGRESS = '*'

# The address of the server running the proc service.
CANFAR_PROC_SERVER = os.getenv('CANFAR_PROC_SERVER', 'https://www.canfar.phys.uvic.ca')
# The endpoint of the proc service on that server.
CANFAR_PROC_ENDPOINT = os.getenv('CANFAR_PROC_ENDPOINT', '/proc/pub')


class Cantop(object):
    """
    A class to manage interaction with the CANFAR job listing service.
    """
    def __init__(self, server=None, endpoint=None, **kwargs):
        self.keep_columns = ['Job_ID',
                             'User',
                             'Started_on',
                             'Status',
                             'VM_Type',
                             'Command']
        self.filter = {'User': None,
                       'Status': None}

        if server is None:
            server = CANFAR_PROC_SERVER
        if endpoint is None:
            endpoint = CANFAR_PROC_ENDPOINT

        self.server = server
        self.endpoint = endpoint
        self.client = vos.Client(**kwargs)

    def window_init(self):
        self.main_window = curses.initscr()
        curses.noecho()
        curses.cbreak()

    def set_filter(self, ch):
        """Set a filter value, either on the User Name attached to the job or on the Status of the job."""

        columns = {ord('u'): 'User',
                   ord('s'): 'Status'}

        if ch in columns:
            self.filter[columns[ch]] = self.__get_value(columns[ch])

        if ch == ord('a'):
            for key in self.filter:
                self.filter[key] = None

    @property
    def get_proc_table(self):
        """
        retrieve the table of processes from the proc service.
        @return: Table
        """
        url = self.server+self.endpoint
        f = cStringIO.StringIO(self.client.open(uri=None, url=url).read())
        f.seek(0)
        logging.debug(f.read())
        f.seek(0)

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            return votable.parse(f, invalid='mask').get_first_table().to_table()

    def get_status(self):
        """
        return a string representation of the current processing table, after applying filters.
        @return: str
        """

        self.table = self.get_proc_table

        resp = "%s \n" % ( str(datetime.now())[0:19] )

        for key in self.filter:
            if self.filter[key] is None:
                continue
            self.table = self.table[self.table[key] == self.filter[key]]
            resp += "%s: %s\t" % (key, self.filter[key])
        self.table.keep_columns(self.keep_columns)
        self.table = self.table[tuple(self.keep_columns)]
        self.table.sort('Job_ID')
        resp += "\n"
        resp += str(self.table)
        resp += "\n"

        return resp

    def __get_value(self, prompt):
        """
        get's the keystroke that the user entered, to set the filter.

        @param prompt:
        @return: str
        """

        curses.echo()
        self.main_window.addstr(1, 0, prompt + " ")
        value = self.main_window.getstr()
        curses.noecho()
        if len(value) == 0:
            value = None
        return value

    def redraw(self):
        """
        Redraw the cantop window.
        @return: None
        """
        self.main_window.erase()
        self.main_window.addstr(self.get_status())
        self.main_window.refresh()


if __name__ == '__main__':
    cantop = Cantop()
    try:
        while True:
            cantop.redraw()
            curses.halfdelay(RATE * 10)
            elapsed = 0
            while elapsed < DELAY:
                cmd = cantop.main_window.getch()
                if cmd > 0:
                    cantop.set_filter(cmd)
                    break
                cantop.main_window.addch(1, 25, str(DELAY - elapsed))
                cantop.main_window.refresh()
                elapsed += RATE
            if cmd == ord('q'):
                break
    finally:
        curses.nocbreak()
        curses.echo()
        curses.endwin()