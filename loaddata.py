import pandas as pd
from pymongo import MongoClient
import os
from functools import reduce
import json
import time

class DataProcess(object):

    def __init__(self, dir_path=None):
        if dir_path == None:
            self.dir_path = os.path.dirname(os.path.realpath(__file__))
        else:
            self.dir_path = dir_path

    def _load_user_info(self):
        user_info = pd.read_table(self.dir_path+'/train/user_info_train.txt',
                                  sep=',', header=None, index_col=None)
        user_info.columns = ['ID', 'Gender', 'Occupation', 'Education',
                             'Marriage', 'Residence']
        return user_info

    def _load_bank_detail(self):
        bank_detail = pd.read_table(self.dir_path+'/train/bank_detail_train.txt',
                                    sep=',', header=None, index_col=None)
        bank_detail.columns = ['ID', 'TransactionTime', 'TransactionType', 'TransactionAmount',
                               'Income']
        return bank_detail

    def _load_browse_history(self):
        browse_history = pd.read_table(self.dir_path+'/train/browse_history_train.txt',
                                       sep=',', header=None, index_col=None)
        browse_history.columns = ['ID', 'Timestamp', 'Activity', 'ActivityNum']
        return browse_history

    def _load_bill_detail(self):
        bill_detail = pd.read_table(self.dir_path+'/train/bill_detail_train.txt',
                                    sep=',', header=None, index_col=None)
        bill_detail.columns = ['ID', 'BillTime', 'BankID', 'LastBillAmount',
                               'LastBillPaid', 'Limit', 'CurrentBalance', 'CurrentMinPayment',
                               'TransCount', 'CurrentBillAmount', 'AdjustAmount',
                               'Interest', 'AvaiBalance', 'CashBalance', 'PayStatus']
        return bill_detail

    def _load_loan_time(self):
        loan_time = pd.read_table(self.dir_path+'/train/loan_time_train.txt',
                                  sep=',', header=None, index_col=None)
        loan_time.columns = ['ID', 'LoanTime']
        return loan_time

    def _load_overdue(self):
        overdue = pd.read_table(self.dir_path+'/train/overdue_train.txt',
                                sep=',', header=None, index_col=None)
        overdue.columns = ['ID', 'Overdue']
        return overdue

    def _concate_data(self):
        user_info = self._load_user_info()
        bank_detail = self._load_bank_detail()
        bill_detail = self._load_bill_detail()
        loan_time = self._load_loan_time()
        overdue = self._load_overdue()
        user = user_info[:1000]
        ids = user['ID']
        bank_pos = bank_detail['ID'].isin(ids)
        bank = bank_detail.loc[bank_pos]
        bill_pos = bill_detail['ID'].isin(ids)
        bill = bill_detail.loc[bill_pos]
        loan_pos = loan_time['ID'].isin(ids)
        loan = loan_time.loc[loan_pos]
        overd_pos = overdue['ID'].isin(ids)
        overd = overdue.loc[overd_pos]
        dfs = [user, loan, bank, bill, overd]
        data = reduce(lambda left, right: pd.merge(left, right,
                                                   on='ID', how='inner'),
                      dfs)
        return data  # (15500110, 26)

    def _save2mongodb(self):
        data = self._concate_data()
        print(data.shape)
        client = MongoClient('localhost', 27017)
        db = client['CreditRisk']
        nrow = data.shape[0]
        base = 1000000
        n = nrow//base
        restrow = nrow%base
        for i in range(n):
            if i == 0:
                data_temp = data.loc[:(i+1)*base]
                records = json.loads(data_temp.T.to_json()).values()
                db.user_info.insert(records)
                del data_temp
            else:
                data_temp = data.loc[(i*base+1):(i+1)*base]
                records = json.loads(data_temp.T.to_json()).values()
                db.user_info.insert(records)
                del data_temp
        data_temp = data.iloc[-restrow:]
        records = json.loads(data_temp.T.to_json()).values()
        db.user_info.insert(records)
        client.close()

if __name__ == '__main__':
    dp = DataProcess()
    start = time.time()
    dp._save2mongodb()
    print('Time:', time.time() - start)

