import pandas as pd



class DataProcess(object):

    def _load_user_info(self):
        user_info = pd.read_table('train/user_info_train.txt',
                                  sep=',', header=None, index_col=None)
        user_info.columns = ['ID', 'Gender', 'Occupation', 'Education',
                             'Marriage', 'Residence']
        return user_info

    def _load_bank_detail(self):
        bank_detail = pd.read_table('train/bank_detail_train.txt',
                                    sep=',', header=None, index_col=None)
        bank_detail.columns = ['ID', 'Timestamp', 'Transaction', 'Amount',
                               'Income']
        return bank_detail

    def _load_browse_history(self):
        browse_history = pd.read_table('train/browse_history_train.txt',
                                       sep=',', header=None, index_col=None)
        browse_history.columns = ['ID', 'Timestamp', 'Activity', 'ActivityNum']
        return browse_history

    def _load_bill_detail(self):
        bill_detail = pd.read_table('train/bill_detail_train.txt',
                                    sep=',', header=None, index_col=None)
        bill_detail.columns = ['ID', 'BillTimestamp', 'BankID', 'LastBillAmount',
                               'LastBillPaid', 'Limit', 'Balance', 'MinPayment',
                               'TransCount', 'PresentBill', 'AdjustAmount',
                               'Interest', 'Balance', 'CashBalance', 'PayStatus']
        return bill_detail

    def _load_loan_time(self):
        loan_time = pd.read_table('train/loan_time_train.txt',
                                  sep=',', header=None, index_col=None)
        loan_time.columns = ['ID', 'LoanTime']
        return loan_time

    def _load_overdue(self):
        overdue = pd.read_table('train/overdue_train.txt',
                                sep=',', header=None, index_col=None)
        overdue.columns = ['ID', 'Overdue']
        return overdue

    def _concate_data(self):
        pass
        # TODO pd.concat

    