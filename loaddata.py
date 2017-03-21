import pandas as pd
from pymongo import MongoClient
import os


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
        bank_detail.columns = ['ID', 'Timestamp', 'Transaction', 'Amount',
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
        bill_detail.columns = ['ID', 'BillTimestamp', 'BankID', 'LastBillAmount',
                               'LastBillPaid', 'Limit', 'Balance', 'MinPayment',
                               'TransCount', 'PresentBill', 'AdjustAmount',
                               'Interest', 'Balance', 'CashBalance', 'PayStatus']
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
        browse_history = self._load_browse_history()
        bill_detail = self._load_bill_detail()
        loan_time = self._load_loan_time()
        overdue = self._load_overdue()
        data = pd.concat([user_info, bank_detail,browse_history,
                          bill_detail, loan_time, overdue],
                         axis=1)
        return data

    def _save2mongodb(self):
        data = self._concate_data()
        client = MongoClient('localhost', 27017)
        db = client['CreditRisk']
        collection = db.user_info
        collection.insert_many(data[:1000].to_dict('records'))

if __name__ == '__main__':
    dp = DataProcess()
    data = dp._concate_data()
    # print(data.shape)
    # dp._save2mongodb()

