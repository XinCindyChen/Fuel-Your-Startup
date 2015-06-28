#!/usr/bin/env python

import pandas as pd
from sklearn import linear_model
from sklearn.ensemble import RandomForestClassifier
from sklearn.svm import SVC

import numpy as np
import random
import time
import datetime
import pymysql as mdb

con = mdb.connect('localhost', 'root', '', 'crunchbase', charset='utf8')
cur = con.cursor()


def main():
    data = pd.read_csv('feature/cleaned_train_set.csv', index_col=0)
    companies = data['company_permalink'].unique()
    investments = pd.read_csv('feature/selected_selected_investments.csv')
    #investments['funded_month_time'] = investments.apply(lambda row:datetime.datetime.strptime(row['funded_month'],'%Y-%m'), axis = 1)
    #investments['funding_round_type_code']=investments.apply(lambda row:row['funding_round_type']+'_'+row['funding_round_code'] if row['funding_round_type']=='venture' and row['funding_round_code'] is not None and row['funding_round_code']==row['funding_round_code'] else row['funding_round_type'], axis=1)
    investments_investor_indexed = investments.set_index('investor_permalink', drop=False)
        
    logistic = linear_model.LogisticRegression()
    rf = RandomForestClassifier(criterion='entropy')
    #svc = SVC(kernel = 'linear', probability = True)
    recall_list = []
    #accuracy_list = []
    baseline_list = []
    
    feature_cols=['categories','location', 'round_match','previous_round_norm', 'competitor_norm', 'network_1st_norm', 'network_23_norm']
    feature_cols_label=['categories','location', 'round_match','previous_round_norm', 'competitor_norm', 'network_1st_norm', 'network_23_norm','label']


    #num_predicts = []
    
    for company_permalink in companies:
        print company_permalink
        
        train_set_origin = data[data['company_permalink']!=company_permalink][feature_cols_label]
        train_set = train_set_origin.dropna()
        rounds = data[data['company_permalink']==company_permalink]['round_permalink'].unique()
        for fr in rounds:
            test_file = 'feature/test_app/' + company_permalink + '_' + fr.split('/')[-1]+'.csv'
            test_set = pd.read_csv(test_file, index_col = 0).dropna()
            #print test_file
        
            rf.fit(train_set[feature_cols], train_set['label'])
            #svc.fit(train_set[feature_cols], train_set['label'])

        
            prob_result = rf.predict_proba(test_set[feature_cols])
            #accuracy_list.append(rf.score(test_set[feature_cols], test_set['label']))
            #logistic.score(test_set[feature_cols], test_set['label']) #accuracy
            
            prob_result_df = pd.DataFrame(prob_result, index = test_set.index).rename(columns={0:'neg',1:'pos'})
        
            final = test_set.join(prob_result_df)
            final_indexed = final.sort('pos', ascending=False).set_index('investor_permalink')
             
            #test_indexed = test_set.set_index('investor_permalink', drop = False)           
            #baseline_investor_list = []
            #baseline_sorted_df = test_indexed[(test_indexed['location']!=0) & (test_indexed['categories']!=0) ]
            #baseline_sorted_df = test_indexed
            #invests = investments_investor_indexed.ix[baseline_sorted_df.index]
                    
            #baseline_sample = invests['investor_permalink'].value_counts().keys()[:30]
            
            #if len(baseline_sorted_df.index)>30:
             #  baseline_sample = random.sample(baseline_sorted_df.index.unique(), 30)
            #else:
             #   baseline_sample = baseline_sorted_df.index.unique()
                
            predicted_investors = final_indexed[:30].index.unique()
            #predicted_investors = final_indexed[final_indexed['pos']>0.2].index.unique()
            
            ground_truth = final_indexed[final_indexed['label']==1].index.unique()
            
            #print ground_truth
            if len(ground_truth)>0:
                recall = len(set(ground_truth).intersection(set(predicted_investors)))/float(len(ground_truth))
                recall_list.append(recall)
                
                #baseline = len(set(ground_truth).intersection(set(baseline_sample)))/float(len(ground_truth))
                #baseline_list.append(baseline)
                #num_predicts.append(len(predicted_investors))
    
    recall_out = pd.DataFrame(recall_list)
    recall_out.to_csv('random_forest_recall_entropy.csv')
    print np.mean(recall_list)
    print np.median(recall_list)
    #print np.mean(baseline_list)
    #print np.mean(accuracy_list)
    #print np.mean(num_predicts)
    
            

if __name__ == '__main__':
    main()