from preprocess import DataPreprocess
from sklearn.model_selection import train_test_split
from sklearn.naive_bayes import BernoulliNB

class Model(object):

    def __init__(self):
        dp = DataPreprocess()
        df = dp.run()
        X = df.drop('Overdue', axis=1)
        y = df['Overdue']
        self.X_train, self.X_test, self.y_train, self.y_test = \
            train_test_split(X, y, test_size=0.3, random_state=1)

    # TODO Bernoulli_NB, XGB, RandomForest, Adaboost
    def _bernoulli_NB(self):
        clf = BernoulliNB()
        clf.fit(self.X_train, self.y_train)
        score = clf.score(self.X_test, self.y_test)
        print('Accuracy rate of Naive Bayes: {0:.3f}'.format(score))
    