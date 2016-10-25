import utils
import pandas as pd
import numpy as np
import collections
import models
from sklearn.cross_validation import KFold, ShuffleSplit
from sklearn.ensemble import AdaBoostClassifier,RandomForestClassifier
from sklearn.naive_bayes import GaussianNB
from numpy import mean
from sklearn.metrics import *
from sklearn.datasets import load_svmlight_file
from sklearn.linear_model import LogisticRegression
from sklearn.svm import LinearSVC
from sklearn.tree import DecisionTreeClassifier
from sklearn.metrics import *


#Note: You can reuse code that you wrote in etl.py and models.py and cross.py over here. It might help.

'''
You may generate your own features over here.
Note that for the test data, all events are already filtered such that they fall in the observation window of their respective patients. Thus, if you were to generate features similar to those you constructed in code/etl.py for the test data, all you have to do is aggregate events for each patient.
IMPORTANT: Store your test data features in a file called "test_features.txt" where each line has the
patient_id followed by a space and the corresponding feature in sparse format.
Eg of a line:
60 971:1.000000 988:1.000000 1648:1.000000 1717:1.000000 2798:0.364078 3005:0.367953 3049:0.013514
Here, 60 is the patient id and 971:1.000000 988:1.000000 1648:1.000000 1717:1.000000 2798:0.364078 3005:0.367953 3049:0.013514 is the feature for the patient with id 60.

Save the file as "test_features.txt" and save it inside the folder deliverables

input:
output: X_train,Y_train,X_test
'''
def my_features():
    #TODO: complete this
    #get train data

    X_train,Y_train = utils.get_data_from_svmlight("../deliverables/features_svmlight.train")

    #create test data

    t_events = pd.read_csv("../data/test/events.csv")
    t_feature_map = pd.read_csv("../data/test/event_feature_map.csv")
    aggregate_events(t_events,t_feature_map )
    X_test, Y_test = utils.get_data_from_svmlight("../deliverables/features_svmlight.test")


    print X_test
    print Y_test
    return X_train, Y_train, X_test

def aggregate_events(events, feature_map):

    temp_df = pd.merge(events, feature_map, on='event_id')

    #drop events with nan values
    temp_df = temp_df.dropna(subset=['value'])


    p1 = ['DIAG', 'DRUG']
    drug = temp_df[temp_df['event_id'].str.contains('|'.join(p1))]
    lab = temp_df[temp_df['event_id'].str.contains('LAB')]


    ds = drug.groupby(['idx', 'patient_id','event_id']).aggregate({'value' : np.sum}).reset_index()
    ls = lab.groupby(['idx', 'patient_id','event_id']).aggregate({'value' : len}).reset_index()


    aggregated_events= pd.concat([ds, ls])
    aggregated_events = aggregated_events.rename(columns={'idx':'feature_id'})
    #print aggregated_events.columns
    #get max value per feature
    aggregated_events['max'] = aggregated_events.groupby('feature_id')['value'].transform(max)
    #then divide each each value by max
    aggregated_events['normalized'] = aggregated_events.groupby(['feature_id'])['value'].apply(lambda x: (x)/(x.max()))
    #aggregated_events['normalized'] = aggregated_events['value'].div(aggregated_events['max'])
    print "aggregated events", aggregated_events[:10]
    aggregated_events = aggregated_events.rename(columns={'normalized':'feature_value'})

    patient_features = {}

    pf_df = pd.DataFrame()
    pf_df['patient_id'] = aggregated_events['patient_id']
    pf_df['feature_id'] = aggregated_events['feature_id']
    pf_df['feature_value'] = aggregated_events['feature_value']


    for x in range(len(pf_df)):
        currentid = pf_df.iloc[x,0]
        patient_features.setdefault(currentid, [])
        patient_features[currentid].append((pf_df.iloc[x,1] ,pf_df.iloc[x,2]))


    patient_features = collections.OrderedDict(sorted(patient_features.items()))

    deliverable1 = open("../deliverables/features_svmlight.test", 'w')
    deliverable2 = open("../deliverables/test_features.txt", 'w')

    svm_light = ""
    train = ""
    #print "mortality dictionary", mortality
    for key in sorted(patient_features.iterkeys()):
        label = 0
        value = patient_features.get(key)

        '''for k in sorted(mortality):
            #print "k row", k, int(key)
            if k == int(key):
                label = 1'''


        str = "%d" % label + ' ' + utils.bag_to_svmlight(sorted(value)) + "\n"
        str_t = "%d" % key + ' ' + "%.1f" % label + ' ' + utils.bag_to_svmlight(sorted(value)) + "\n"
        svm_light = svm_light + str
        train = train + str_t



    deliverable1.write(svm_light);
    deliverable2.write(train);
'''
You can use any model you wish.

input: X_train, Y_train, X_test
output: Y_pred
'''
def my_classifier_predictions(X_train,Y_train,X_test):
    #TODO: complete this

    '''cl = DecisionTreeClassifier(max_depth=5,random_state=545510477)
    cl.fit(X_train, Y_train)
    Y_pred = cl.predict(X_test)'''
    '''cl = AdaBoostClassifier(random_state=545510477)
    cl.fit(X_train, Y_train)
    Y_pred = cl.predict(X_test)'''
    cl = RandomForestClassifier(n_estimators=200, max_depth=5,random_state=545510477, max_features=5)
    cl.fit(X_train, Y_train)
    Y_pred = cl.predict(X_test)

    X_test, Y_test = utils.get_data_from_svmlight("../deliverables/features_svmlight.test")
    accur = accuracy_score(Y_test, Y_pred)


    print "--------------------"
    print "accuracy", accur


    return Y_pred


def main():
    X_train, Y_train, X_test = my_features()
    Y_pred = my_classifier_predictions(X_train,Y_train,X_test)
    print Y_pred
    utils.generate_submission("../deliverables/test_features.txt",Y_pred)
    #The above function will generate a csv file of (patient_id,predicted label) and will be saved as "my_predictions.csv" in the deliverables folder.

if __name__ == "__main__":
    main()

