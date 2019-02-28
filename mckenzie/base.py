import os
from random import randint


class DatabaseView:
    def __init__(self, db):
        self.db = db


class Manager:
    def __init__(self, mck):
        self.mck = mck

        self.conf = mck.conf
        self.db = mck.conf.db

        pid = os.getpid()
        rand = randint(0, 0xff)
        # 00000001 RRRRRRRR PPPPPPPP PPPPPPPP
        self.ident = (0x01 << 24) | (rand << 16) | (pid & 0xffff)
