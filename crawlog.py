"""
    Logging helpers for use in the Crawly project.
    
    Jordan Zdimirovic. https://github.com/jordstar20001/crawly
"""

from datetime import datetime as dt
from os import fsync
from os import path

class Crawlogger():
    def __init__(self, show_in_console = True, fpath = None, insta_flush = True, name = "DEBUG"):
        """
            Create a crawlogger instance.
        """
        self.__show_in_console = show_in_console
        if fpath:
            if not path.exists(fpath):
                open(fpath, 'x').close()
            self.__fobj = open(fpath, "a")
        
        else: self.__fobj = None
            
        self.name = name
        self.__insta_flush = insta_flush

    def log(self, msg: object):
        """
            Log a message using the options provided
        """
        val = f"{dt.now().strftime('%d/%m/%Y %H:%M:%S')} | {self.name} -> {msg}"
        if self.__show_in_console: print(val)
        if self.__fobj:
            self.__fobj.write(val + "\n")
            if self.__insta_flush:
                self.__fobj.flush()
                fsync(self.__fobj)