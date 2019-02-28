class DatabaseView:
    def __init__(self, db):
        self.db = db


class Manager:
    def __init__(self, mck):
        self.mck = mck

        self.conf = mck.conf
        self.db = mck.conf.db
