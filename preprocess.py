from pymongo import MongoClient
import pandas as pd

class DataPreprocess(object):

    def __init__(self):
        pass

    def _load_db(self):
        client = MongoClient('localhost', 27017)
        db = client['CreditRisk']
        cursor = db.user_bank.find()
        user_bank = pd.DataFrame(list(cursor))
        user_bank.drop("_id", axis=1, inplace=True)
        cursor = db.user_bill.find()
        user_bill = pd.DataFrame(list(cursor))
        user_bill.drop("_id", axis=1, inplace=True)
        cursor = db.overdue.find()
        overdue = pd.DataFrame(list(cursor))
        overdue.drop("_id", axis=1, inplace=True)
        client.close()
        return user_bank, user_bill, overdue

