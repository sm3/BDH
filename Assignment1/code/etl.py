import utils
import pandas as pd
from datetime import datetime, timedelta
import numpy as np
import os
import collections


def read_csv(filepath):
    
    '''
    TODO: This function needs to be completed.
    Read the events.csv, mortality_events.csv and event_feature_map.csv files into events, mortality and feature_map.
    
    Return events, mortality and feature_map
    '''

    #Columns in events.csv - patient_id,event_id,event_description,timestamp,value
    events = ''
    
    #Columns in mortality_event.csv - patient_id,timestamp,label
    mortality = ''

    #Columns in event_feature_map.csv - idx,event_id
    feature_map = ''

    events, mortality, feature_map = utils.read_csv(filepath)
    return events, mortality, feature_map


def calculate_index_date(events, mortality, deliverables_path):
    
    '''
    TODO: This function needs to be completed.

    Refer to instructions in Q3 a

    Suggested steps:
    1. Create list of patients alive ( mortality_events.csv only contains information about patients deceased)
    2. Split events into two groups based on whether the patient is alive or deceased
    3. Calculate index date for each patient
    
    IMPORTANT:
    Save indx_date to a csv file in the deliverables folder named as etl_index_dates.csv. 
    Use the global variable deliverables_path while specifying the filepath. 
    Each row is of the form patient_id, indx_date.
    The csv file should have a header 
    For example if you are using Pandas, you could write: 
        indx_date.to_csv(deliverables_path + 'etl_index_dates.csv', columns=['patient_id', 'indx_date'], index=False)

    Return indx_date
    '''

    deceased_data = pd.DataFrame()

    #Add deceased data first
    deceased_data['patient_id'] = mortality['patient_id']
    deceased_data['indx_date'] = mortality['timestamp'].apply(lambda x : (utils.date_convert(x)-timedelta(days=30)))

    deceased_data = deceased_data.reset_index()

    #print deceased_data[:10]

    # get alive data

    alive_data = events[events['patient_id'].isin(mortality.patient_id) == False]
    alive_indx = alive_data.groupby('patient_id').aggregate({'timestamp' : np.max}).reset_index()

    alive_indx = alive_indx.rename(columns = {'timestamp': 'indx_date'})
    alive_indx['indx_date'] = alive_indx['indx_date'] .apply(lambda x : (utils.date_convert(x)))

    #print alive_data[:10]
    #print alive_indx[:10]


    indx_date = pd.DataFrame()
    indx_date = pd.concat([deceased_data, alive_indx]).reset_index(drop=True)
    #indx_date.rename(columns = {'timstamp':'indx_date'}, inplace = True)
    index_date = indx_date.reset_index().drop(1)

    #print indx_date[:10]

    p_i = pd.DataFrame(data=indx_date, columns=['patient_id', 'indx_date'])
    #print "p_i", p_i

    p_i.to_csv(deliverables_path + 'etl_index_dates.csv', columns=['patient_id', 'indx_date'], index=False)

    #print "length of indx dates", len(p_i)
    #p_i = p_i.reset_index(drop=True)
    return p_i
    '''
    indx_date.to_csv(deliverables_path + 'etl_index_dates.csv', columns=['patient_id', 'indx_date'], index=True)
    return indx_date'''


def filter_events(events, indx_date, deliverables_path):
    
    '''
    TODO: This function needs to be completed.

    Refer to instructions in Q3 a

    Suggested steps:
    1. Join indx_date with events on patient_id
    2. Filter events occuring in the observation window(IndexDate-2000 to IndexDate)
    
    
    IMPORTANT:
    Save filtered_events to a csv file in the deliverables folder named as etl_filtered_events.csv. 
    Use the global variable deliverables_path while specifying the filepath. 
    Each row is of the form patient_id, event_id, value.
    The csv file should have a header 
    For example if you are using Pandas, you could write: 
        filtered_events.to_csv(deliverables_path + 'etl_filtered_events.csv', columns=['patient_id', 'event_id', 'value'], index=False)

    Return filtered_events
    '''

    filtered_events = pd.merge(events, indx_date, how='left', on='patient_id')

    print "unfiltered", len(filtered_events)


    #Filter events occuring in the observation window(IndexDate-2000 to IndexDate)

    filtered_events['timestamp'] = filtered_events['timestamp'].apply(lambda x: utils.date_convert(x))


    filtered_events = filtered_events[(filtered_events['timestamp'] >= filtered_events['indx_date']-timedelta(days=2000)) & (filtered_events['timestamp'] <= filtered_events['indx_date'])]
    #print filtered_events[:10]
    print "filtered", len(filtered_events)

    filtered_events.to_csv(deliverables_path + 'etl_filtered_events.csv', columns=['patient_id', 'event_id', 'value'], index=False)

    return filtered_events


def aggregate_events(filtered_events_df, mortality_df,feature_map_df, deliverables_path):
    
    '''
    TODO: This function needs to be completed.

    Refer to instructions in Q3 a

    Suggested steps:
    1. Replace event_id's with index available in event_feature_map.csv
    2. Remove events with n/a values
    3. Aggregate events using sum to calculate feature value 
    4. Normalize the values obtained above using min-max normalization
    
    
    IMPORTANT:
    Save aggregated_events to a csv file in the deliverables folder named as etl_aggregated_events.csv. 
    Use the global variable deliverables_path while specifying the filepath. 
    Each row is of the form patient_id, event_id, value.
    The csv file should have a header .
    For example if you are using Pandas, you could write: 
        aggregated_events.to_csv(deliverables_path + 'etl_aggregated_events.csv', columns=['patient_id', 'feature_id', 'feature_value'], index=False)

    Return filtered_events
    '''
    aggregated_events = ''

    temp_df = pd.merge(filtered_events_df, feature_map_df, on='event_id')

    #drop events with nan values
    temp_df = temp_df.dropna(subset=['value'])
    #temp_df = temp_df.groupby(['idx', 'patient_id', 'event_id']).aggregate({'value' : np.sum})


    #print "temp_df", temp_df[:10]


    p1 = ['DIAG', 'DRUG']
    drug = temp_df[temp_df['event_id'].str.contains('|'.join(p1))]
    lab = temp_df[temp_df['event_id'].str.contains('LAB')]


    ds = drug.groupby(['patient_id','event_id'])['value'].sum().reset_index()
    ls = lab.groupby(['patient_id','event_id'])['value'].count().reset_index()

    ds = drug.groupby(['idx', 'patient_id','event_id']).aggregate({'value' : np.sum}).reset_index()
    ls = lab.groupby(['idx', 'patient_id','event_id']).aggregate({'value' : len}).reset_index()
    #print "drug", ds[:10]
    #print "lab" , ls[:10]
    #ls = ls.reset_index(drop=True)

    #lab.to_csv("lab.csv")

    #ds = ds.set_index('patient_id')
    #ls = ls.set_index('patient_id')

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



    aggregated_events.to_csv(deliverables_path + 'etl_aggregated_events.csv', columns=['patient_id', 'feature_id', 'feature_value'], index=False)



    return aggregated_events

def create_features(events, mortality, feature_map):
    
    deliverables_path = '../deliverables/'

    #Calculate index date
    indx_date = calculate_index_date(events, mortality, deliverables_path)

    #Filter events in the observation window
    filtered_events = filter_events(events, indx_date,  deliverables_path)
    
    #Aggregate the event values for each patient 
    aggregated_events = aggregate_events(filtered_events, mortality, feature_map, deliverables_path)

    '''
    TODO: Complete the code below by creating two dictionaries - 
    1. patient_features :  Key - patient_id and value is array of tuples(feature_id, feature_value)
    2. mortality : Key - patient_id and value is mortality label
    '''
    patient_features = {}
    mortality_dict = {}

    pf_df = pd.DataFrame()
    pf_df['patient_id'] = aggregated_events['patient_id']
    pf_df['feature_id'] = aggregated_events['feature_id']
    pf_df['feature_value'] = aggregated_events['feature_value']


    for x in range(len(pf_df)):
        currentid = pf_df.iloc[x,0]
        patient_features.setdefault(currentid, [])
        patient_features[currentid].append((pf_df.iloc[x,1] ,pf_df.iloc[x,2]))



    #pf_df.set_index('patient_id')

    #m_df = pd.DataFrame()
    #m_df['patient_id'] = mortality['patient_id']
    #m_df['value'] = mortality['label']
    patient_features = collections.OrderedDict(sorted(patient_features.items()))
    #print "patient_features", len(patient_features)

    #mortality : Key - patient_id and value is mortality label



    mortality_dict = mortality.set_index('patient_id')['label'].to_dict()
    mortality_dict = collections.OrderedDict(sorted(mortality_dict.items()))
    #print "mortality dictionary", mortality_dict

    return patient_features, mortality_dict

def save_svmlight(patient_features, mortality, op_file, op_deliverable):
    
    '''
    TODO: This function needs to be completed
    Create two files:
    1. op_file - which saves the features in svmlight format. (See instructions in Q3d for detailed explanation)
    2. op_deliverable - which saves the features in following format:
       patient_id1 label feature_id:feature_value feature_id:feature_value feature_id:feature_value ...
       patient_id2 label feature_id:feature_value feature_id:feature_value feature_id:feature_value ...  
    
    Note: Please make sure the features are ordered in ascending order, and patients are stored in ascending order as well.     
    '''
    deliverable1 = open(op_file, 'w')
    deliverable2 = open(op_deliverable, 'w')

    svm_light = ""
    train = ""
    #print "mortality dictionary", mortality
    for key in sorted(patient_features.iterkeys()):
        label = 0
        value = patient_features.get(key)

        for k in sorted(mortality):
            #print "k row", k, int(key)
            if k == int(key):
                label = 1


        str = "%d" % label + ' ' + utils.bag_to_svmlight(sorted(value)) + "\n"
        str_t = "%d" % key + ' ' + "%.1f" % label + ' ' + utils.bag_to_svmlight(sorted(value)) + "\n"
        svm_light = svm_light + str
        train = train + str_t


    
    deliverable1.write(svm_light);
    deliverable2.write(train);

def main():
    train_path = '../data/train/'
    events, mortality, feature_map = read_csv(train_path)
    patient_features, mortality = create_features(events, mortality, feature_map)
    save_svmlight(patient_features, mortality, '../deliverables/features_svmlight.train', '../deliverables/features.train')

if __name__ == "__main__":
    main()




