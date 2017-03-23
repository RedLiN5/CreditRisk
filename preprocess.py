from pymongo import MongoClient
import pandas as pd

class DataPreprocess(object):

    def __init__(self, nrow=None):
        self.nrow = nrow

    def _load_db(self):
        client = MongoClient('localhost', 27017)
        db = client.user_info
        if self.nrow:
            cursor = db.find().limit(self.nrow)
        else:
            cursor = db.find().limit(1000)
        data = pd.DataFrame(list(cursor))
        data = data.drop('_id', axis=1)
        client.close()
        return data



