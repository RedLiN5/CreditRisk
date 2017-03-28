import pandas as pd
from pymongo import MongoClient
import os
from functools import reduce
import json
import time
import inspect

def retrieve_name(var):
    callers_local_vars = inspect.currentframe().f_back.f_locals.items()
    return [var_name for var_name, var_val in callers_local_vars if var_val is var]

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
                               'Interest', 'AvaiBalance', 'CashLimit', 'PayStatus']
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
        overdue = self._load_overdue()
        user_bank = pd.merge(user_info, bank_detail,
                             on='ID', how='inner')
        user_bill = pd.merge(user_info, bill_detail,
                             on='ID', how='inner')
        return user_bank, user_bill, overdue  # (15500110, 26)

    def _save2mongodb(self):
        user_bank, user_bill, overdue = self._concate_data()
        dfs = [user_bank, user_bill, overdue]
        collection_names = ['user_bank', 'user_bill', 'overdue']
        print('user_bank:', user_bank.shape)
        print('user_bill:', user_bill.shape)
        print('user_overdue:', overdue.shape)
        client = MongoClient('localhost', 27017)
        db = client['CreditRisk']
        for i in range(len(dfs)):
            data = dfs[i]
            collection_name = collection_names[i]
            print(collection_name)
            nrow = data.shape[0]
            base = 1000000
            n = nrow//base
            restrow = nrow%base
            if n >0:
                for j in range(n):
                    if j == 0:
                        data_temp = data.loc[:(j+1)*base]
                    else:
                        data_temp = data.loc[(j*base+1):(j+1)*base]
                    records = json.loads(data_temp.T.to_json()).values()
                    eval('db.' + collection_name + '.insert(records)')
            data_temp = data.iloc[-restrow:]
            records = json.loads(data_temp.T.to_json()).values()
            eval('db.' + collection_name + '.insert(records)')
            del data_temp
        client.close()

if __name__ == '__main__':
    dp = DataProcess()
    start = time.time()
    dp._save2mongodb()
    print('Time:', time.time() - start)

