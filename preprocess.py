from pymongo import MongoClient
import pandas as pd


def bill_status(x, y):
    def status(z):
        if z==0:
            return 0
        elif z>=0.5 or z<0:
            return 1
        else:
            return -1
    ind = y.index[y>0]
    x_val = x.iloc[ind]
    y_val = y.iloc[ind]
    temp = x_val/y_val
    data = list(map(status, temp))
    series = pd.Series(data=data, index=ind)
    return series

class DataPreprocess(object):

    def __init__(self):
        pass

    def _load_db(self):
        client = MongoClient('localhost', 27017)
        db = client['CreditRisk']
        cursor = db.user_bank.find().limit(1000)
        user_bank = pd.DataFrame(list(cursor))
        user_bank.drop("_id", axis=1, inplace=True)
        cursor = db.user_bill.find().limit(1000)
        user_bill = pd.DataFrame(list(cursor))
        user_bill.drop("_id", axis=1, inplace=True)
        cursor = db.overdue.find().limit(1000)
        overdue = pd.DataFrame(list(cursor))
        overdue.drop("_id", axis=1, inplace=True)
        client.close()
        return user_bank, user_bill, overdue

    def _preprocess(self):
        user_bank, user_bill, overdue = self._load_db()
        user_bill['LastBillStatus'] = bill_status(user_bill['LastBillPaid'],
                                                  user_bill['LastBillAmount'])
        user_bill['LastBillStatus'].fillna(0, inplace=True)
        user_bill.drop(['LastBillPaid', 'LastBillAmount'],
                       axis=1, inplace=True)




