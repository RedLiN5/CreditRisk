from pymongo import MongoClient
import pandas as pd
import numpy as np


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

def interest_status(x, y):
    def status(z):
        if z>=0.3:
            return -1
        else:
            return 0
    ind = y.index[y>=0]
    x_val = x.iloc[ind]
    y_val = y.iloc[ind]
    temp = x_val/y_val
    data = list(map(status, temp))
    series = pd.Series(data=data, index=ind)
    return series

def current_balance_status(x):
    if x >= 0.6:
        return 1
    elif x >= 0:
        return 0
    else:
        return -1

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
        user_bill['ID'] = list(map(int, user_bill['ID']))
        user_bank['ID'] = list(map(int, user_bank['ID']))
        overdue['ID'] = list(map(int, overdue['ID']))
        user_bill['LastBillStatus'] = bill_status(user_bill['LastBillPaid'],
                                                  user_bill['LastBillAmount'])
        user_bill['LastBillStatus'].fillna(0, inplace=True)
        user_bill.drop(['LastBillPaid', 'LastBillAmount'],
                       axis=1, inplace=True)
        user_bill['Adjustment'] = np.sign(user_bill['AdjustAmount'])
        user_bill.drop('AdjustAmount', axis=1, inplace=True)
        user_bill.drop('CurrentMinPayment', axis=1, inplace=True)
        user_bill['InterestStatus'] = interest_status(user_bill['Interest'],
                                                      user_bill['Limit'])
        user_bill.drop('Interest', axis=1, inplace=True)
        user_bill.drop('BankID', axis=1, inplace=True)
        # TODO Filter  Ratio of "CurrentBalance" to "Limit", and sum up "Limit" by ID
        user_bill['AvaiBalanceStatus'] = list(map(lambda x: 1 if x >= 0 else -1,
                                                  user_bill['AvaiBalance']))
        user_bill.drop('AvaiBalance', axis=1, inplace=True)
        ratio_CurrentBill_Limit = user_bill['CurrentBillAmount']/user_bill['Limit']
        user_bill['CurrentBillRatio'] = list(map(lambda x: 1 if x<=0.5 else 0,
                                                 ratio_CurrentBill_Limit))
        user_bill.drop('CurrentBillAmount', axis=1, inplace=True)
        ratio_CurrentBalan_Limit = user_bill['CurrentBalance']/user_bill['Limit']
        user_bill['CurrentBlanceRatio'] = list(map(current_balance_status,
                                                   ratio_CurrentBalan_Limit))
        user_bill.drop('CurrentBalance', axis=1, inplace=True)
        user_bill.drop(['BillTime','CashLimit'], axis=1, inplace=True)

        salaryincome_index = user_bank['Income'] == 1
        user_bank.ix[salaryincome_index, 'SalaryIncome'] = user_bank.ix[salaryincome_index, 'TransactionAmount']
        income_index = user_bank['TransactionType'] == 0
        otherincome_index = [x for x in income_index if x not in salaryincome_index]
        user_bank.ix[otherincome_index, 'OtherTransaction'] = user_bank.ix[otherincome_index, 'TransactionAmount']
        consum_loc = user_bank['TransactionType'] == 1
        user_bank.ix[consum_loc, 'Consumption'] = user_bank.ix[consum_loc, 'TransactionAmount']
        user_bank[['SalaryIncome', 'OtherTransaction', 'Consumption']] = user_bank[
            ['SalaryIncome', 'OtherTransaction', 'Consumption']].fillna(0)
        user_bank.drop(['TransactionAmount', 'TransactionTime', 'TransactionType', 'Income'], axis=1, inplace=True)



