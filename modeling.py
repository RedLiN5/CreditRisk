from preprocess import DataPreprocess
from sklearn.model_selection import train_test_split
from sklearn.naive_bayes import BernoulliNB
from sklearn.ensemble import RandomForestClassifier
from xgboost.sklearn import XGBClassifier
from sklearn.ensemble import AdaBoostClassifier
import time
from sklearn import metrics


def ks(y_predicted, y_true):
    label=y_true
    #label = y_true.get_label()
    fpr,tpr,thres = metrics.roc_curve(label,y_predicted,pos_label=1)
    print('ks:',abs(fpr - tpr).max(), '\n')


class Model(object):

    def __init__(self):
        dp = DataPreprocess()
        df = dp.run()
        X = df.drop('Overdue', axis=1)
        y = df['Overdue']
        self.X_train, self.X_test, self.y_train, self.y_test = \
            train_test_split(X, y, test_size=0.2, random_state=1)

    def _bernoulli_NB(self):
        clf = BernoulliNB()
        clf.fit(self.X_train, self.y_train)
        score = clf.score(self.X_test, self.y_test)
        print('Accuracy rate of Naive Bayes: {0:.3f}'.format(score))
        y_pred = clf.predict_proba(self.X_test)
        ks(y_pred.T[0], self.y_test)

    def _random_forest(self):
        clf = RandomForestClassifier()
        clf.fit(self.X_train, self.y_train)
        score = clf.score(self.X_test, self.y_test)
        print('Accuracy rate of Random Forest: {0:.3f}'.format(score))
        y_pred = clf.predict_proba(self.X_test)
        ks(y_pred.T[0], self.y_test)

    def _XGBoost(self):
        clf = XGBClassifier()
        clf.fit(self.X_train, self.y_train)
        score = clf.score(self.X_test, self.y_test)
        print('Accuracy rate of XGBoost: {0:.3f}'.format(score))
        y_pred = clf.predict_proba(self.X_test)
        ks(y_pred.T[0], self.y_test)

    def _adaboost(self):
        clf = AdaBoostClassifier()
        clf.fit(self.X_train, self.y_train)
        score = clf.score(self.X_test, self.y_test)
        print('Accuracy rate of Adaptive Boosting: {0:.3f}'.format(score))
        y_pred = clf.predict_proba(self.X_test)
        ks(y_pred.T[0], self.y_test)

    def run_test(self):
        self._bernoulli_NB()
        self._random_forest()
        self._XGBoost()
        self._adaboost()

if __name__ == '__main__':
    start = time.time()
    model = Model()
    model.run_test()
    print('Time:', time.time() - start)
