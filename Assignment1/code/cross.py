import models
from sklearn.cross_validation import KFold, ShuffleSplit
from numpy import mean
from sklearn.metrics import *

import utils

#input: training data and corresponding labels
#output: accuracy, auc
def get_acc_auc_kfold(X,Y,k=5):
	#TODO:First get the train indices and test indices for each iteration

    auc_scores =[]
    acc_scores = []

    kf = KFold(n=X.get_shape()[0], n_folds=k, random_state=545510477, shuffle=True)
    print "len(kf)", len(kf)
    for train_index, test_index in kf:
        X_train, X_test = X[train_index], X[test_index]
        Y_train, Y_test = Y[train_index], Y[test_index]
        #Then train the classifier accordingly
        Y_pred = models.logistic_regression_pred(X_train, Y_train, X_test)
        #Report the mean accuracy and mean auc of all the iterations
        accur, auc1, precision, recall, F1score = models.classification_metrics(Y_pred, Y_test)
        acc_scores.append(accur)
        auc_scores.append(auc1)


	accuracy = sum(acc_scores)/len(acc_scores)
	auc = sum(auc_scores)/len(auc_scores)

	return accuracy, auc



#input: training data and corresponding labels
#output: accuracy, auc
def get_acc_auc_randomisedCV(X,Y,iterNo=5,test_percent=0.2):
	#TODO: First get the train indices and test indices for each iteration
	auc_scores =[]
	acc_scores = []
	rs = ShuffleSplit(n=X.get_shape()[0], n_iter=5, random_state=545510477)

	print "len(kf)", len(rs)
	for train_index, test_index in rs:

		X_train, X_test = X[train_index], X[test_index]
		Y_train, Y_test = Y[train_index], Y[test_index]

		#Then train the classifier accordingly
		Y_pred = models.logistic_regression_pred(X_train, Y_train, X_test)
		#Report the mean accuracy and mean auc of all the iterations
		accur, auc1, precision, recall, F1score = models.classification_metrics(Y_pred, Y_test)
		acc_scores.append(accur)
		auc_scores.append(auc1)

	accuracy = sum(acc_scores)/len(acc_scores)
	auc = sum(auc_scores)/len(auc_scores)


	return accuracy, auc


def main():
	X,Y = utils.get_data_from_svmlight("../deliverables/features_svmlight.train")
	print "Classifier: Logistic Regression__________"
	acc_k,auc_k = get_acc_auc_kfold(X,Y)
	print "Average Accuracy in KFold CV: "+str(acc_k)
	print "Average AUC in KFold CV: "+str(auc_k)
	acc_r,auc_r = get_acc_auc_randomisedCV(X,Y)
	print "Average Accuracy in Randomised CV: "+str(acc_r)
	print "Average AUC in Randomised CV: "+str(auc_r)

if __name__ == "__main__":
	main()

