import time

import os
import pandas as pd
import numpy as np
import utils
from datetime import timedelta


def read_csv(filepath):
    '''
    Read the events.csv and mortality_events.csv files. Variables returned from this function are passed as input to the metric functions.
    This function needs to be completed.
    '''

    def symbol_to_path(symbol, base_dir="data"):
        """Return CSV file path given ticker symbol."""
        return os.path.join(base_dir, "events.csv")
    events = ''

    mortality = ''
    #base_dir = "../data/train"
    base_dir = filepath
    events = pd.read_csv(os.path.join(base_dir,"events.csv"),  index_col="patient_id", parse_dates=True, na_values=['nan'])
    mortality = pd.read_csv(os.path.join(base_dir,"mortality_events.csv"),  index_col="patient_id", parse_dates=True, na_values=['nan'])
    #print events.index
    #print mortality.index


    return events, mortality

def event_count_metrics(events, mortality):
    '''
    Event count is defined as the number of events recorded for a given patient.
    This function needs to be completed.
    '''
    avg_dead_event_count = 0.0

    max_dead_event_count = 0.0

    min_dead_event_count = 0.0

    avg_alive_event_count = 0.0

    max_alive_event_count = 0.0

    min_alive_event_count = 0.0


    df = events.reset_index().merge(mortality, how='left', on='timestamp').set_index('patient_id')
    for i in mortality.index:
        df.loc[i,'label'] = 1



    df["label"] = df["label"].fillna(0)

    df = df.reset_index()
    grouped  = df.groupby(['patient_id','timestamp'])
    alive_data = grouped.apply(lambda g: g[g['label'] == 0])
    dead_data = grouped.apply(lambda g: g[g['label'] == 1])

    ag = alive_data.groupby("patient_id")
    dg = dead_data.groupby("patient_id")

    asum = ag['event_id'].count()
    dsum = dg['event_id'].count()

    avg_dead_event_count = dsum.mean()

    max_dead_event_count = dsum.max()

    min_dead_event_count = dsum.min()

    avg_alive_event_count = asum.mean()

    max_alive_event_count = asum.max()

    min_alive_event_count = asum.min()
    print "min_dead_event_count", min_dead_event_count
    print "max_dead_event_count", max_dead_event_count
    print "avg_dead_event_count", avg_dead_event_count
    print "min_alive_event_count", min_alive_event_count
    print "max_alive_event_count", max_alive_event_count
    print "avg_alive_event_count",avg_alive_event_count


    return min_dead_event_count, max_dead_event_count, avg_dead_event_count, min_alive_event_count, max_alive_event_count, avg_alive_event_count

def encounter_count_metrics(events, mortality):
    '''
    Encounter count is defined as the count of unique dates on which a given patient visited the ICU. 
    This function needs to be completed.
    '''

    df = events.reset_index().merge(mortality, how='left', on='timestamp').set_index('patient_id')

    #for all patients whose patient id is in the mortality sheet enter label as 1
    for i in mortality.index:
        df.loc[i,'label'] = 1



    df["label"] = df["label"].fillna(0)
    df = df.reset_index()

    grouped  = df.groupby(['patient_id','timestamp'])

    alive_data = grouped.apply(lambda g: g[g['label'] == 0])
    dead_data = grouped.apply(lambda g: g[g['label'] == 1])



    dd = dead_data.groupby('patient_id').aggregate({'event_id' : np.sum, 'timestamp' : lambda x: x.nunique()})
    ad = alive_data.groupby('patient_id').aggregate({'event_id' : np.sum, 'timestamp' : lambda x: x.nunique()})


    avg_dead_encounter_count = dd['timestamp'].mean()
    max_dead_encounter_count = dd['timestamp'].max()
    min_dead_encounter_count = dd['timestamp'].min()
    avg_alive_encounter_count = ad['timestamp'].mean()
    max_alive_encounter_count = ad['timestamp'].max()
    min_alive_encounter_count = ad['timestamp'].min()

    print "min_dead_encounter_count", min_dead_encounter_count
    print "max_dead_encounter_count", max_dead_encounter_count
    print "avg_dead_encounter_count", avg_dead_encounter_count
    print "min_alive_encounter_count", min_alive_encounter_count
    print "max_alive_encounter_count", max_alive_encounter_count
    print "avg_alive_encounter_count",avg_alive_encounter_count



    return min_dead_encounter_count, max_dead_encounter_count, avg_dead_encounter_count, min_alive_encounter_count, max_alive_encounter_count, avg_alive_encounter_count

def record_length_metrics(events, mortality):
    '''
    Record length is the duration between the first event and the last event for a given patient. 
    This function needs to be completed.
    '''
    df = events.reset_index().merge(mortality, how='left', on='timestamp').set_index('patient_id')

     #for all patients whose patient id is in the mortality sheet enter label as 1
    for i in mortality.index:
        df.loc[i,'label'] = 1

    #else fill with 0
    df["label"] = df["label"].fillna(0)

    df = df.reset_index()

    grouped  = df.groupby(['patient_id','timestamp'])

    #separate alive and deceased data
    alive_data = grouped.apply(lambda g: g[g['label'] == 0])
    dead_data = grouped.apply(lambda g: g[g['label'] == 1])

    alive_data.to_csv("alive_data.csv")
    dead_data.to_csv("dead_data.csv")

    for i in alive_data.iterrows():
        #print "alive_data.at[i[0],'timestamp']", alive_data.at[i[0],'timestamp']
        alive_data.at[i[0],'timestamp'] = utils.date_convert(alive_data.at[i[0],'timestamp'])

    for i in dead_data.iterrows():
        dead_data.at[i[0],'timestamp'] = utils.date_convert(dead_data.at[i[0],'timestamp'])




    alive_data['min'] = alive_data.groupby("patient_id")['timestamp'].transform(min)
    alive_data['max'] = alive_data.groupby("patient_id")['timestamp'].transform(max)
    alive_data['diff'] = abs(alive_data['max']- alive_data['min'])
    dead_data['min'] = dead_data.groupby("patient_id")['timestamp'].transform(min)
    dead_data['max'] = dead_data.groupby("patient_id")['timestamp'].transform(max)
    dead_data['diff'] = abs(dead_data['max']-dead_data['min'])


    alive_data.to_csv("alive_data.csv")
    dead_data.to_csv("dead_data.csv")


    max_dead_rec_len = (dead_data["diff"].max()/np.timedelta64(1,'D')).astype('int64')
    min_dead_rec_len =(dead_data["diff"].min()/np.timedelta64(1,'D')).astype('int64')
    avg_dead_rec_len = (max_dead_rec_len +min_dead_rec_len)/2.0
    max_alive_rec_len = (alive_data["diff"].max()/np.timedelta64(1,'D')).astype('int64')
    min_alive_rec_len = (alive_data["diff"].min()/np.timedelta64(1,'D')).astype('int64')
    avg_alive_rec_len = (max_alive_rec_len +min_alive_rec_len)/2.0


    print "min_dead_rec_len", min_dead_rec_len
    print "max_dead_rec_len", max_dead_rec_len
    print "avg_dead_rec_len", avg_dead_rec_len
    print "min_alive_rec_len", min_alive_rec_len
    print "max_alive_rec_len", max_alive_rec_len
    print "avg_alive_rec_len",avg_alive_rec_len

    return min_dead_rec_len, max_dead_rec_len, avg_dead_rec_len, min_alive_rec_len, max_alive_rec_len, avg_alive_rec_len

def main():
    '''
    DONOT MODIFY THIS FUNCTION. 
    Just update the train_path variable to point to your train data directory.
    '''
    #Modify the filepath to point to the CSV files in train_data
    train_path = '../data/train'
    events, mortality = read_csv(train_path)

    #Compute the event count metrics
    start_time = time.time()
    event_count = event_count_metrics(events, mortality)
    end_time = time.time()
    print("Time to compute event count metrics: " + str(end_time - start_time) + "s")
    print event_count

    #Compute the encounter count metrics
    start_time = time.time()
    encounter_count = encounter_count_metrics(events, mortality)
    end_time = time.time()
    print("Time to compute encounter count metrics: " + str(end_time - start_time) + "s")
    print encounter_count

    #Compute record length metrics
    start_time = time.time()
    record_length = record_length_metrics(events, mortality)
    end_time = time.time()
    print("Time to compute record length metrics: " + str(end_time - start_time) + "s")
    print record_length
    
if __name__ == "__main__":
    main()



