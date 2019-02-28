class Manager:
    def __init__(self, mck):
        self.mck = mck

        self.conf = mck.conf
        self.db = mck.conf.db
