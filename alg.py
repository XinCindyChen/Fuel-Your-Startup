#!/usr/bin/env python
import pandas as pd
import numpy as np
import sys
import pymysql as mdb
import operator

from sklearn import linear_model
import random




def recommender_model(company_name):
    con = mdb.connect('localhost', 'root', '', 'crunchbase', charset='utf8')
    with con:
        cur = con.cursor()
        cur.execute("SELECT permalink, org_name, location_city,  categories, next_round, next_round_c FROM bayarea_post2012_fewer4 WHERE org_name='%s';" % company_name)
        query_result = cur.fetchall()
    
    if len(query_result)==0:
        return 'error1' # not exist in database
    else:
        company_permalink = query_result[0][0]
        company_origin_name = query_result[0][1]
        company_location = query_result[0][2]
        company_categories = query_result[0][3].strip('|')
        next_round = query_result[0][4]
        next_round_display = query_result[0][5]
        
        data = pd.read_csv('feature/cleaned_train_set.csv', index_col=0).dropna()
        
        logistic = linear_model.LogisticRegression()
        
        train_set = data[data['company_permalink']!=company_permalink] #remove the testing case, and everything else can be used as training case
        #test_set = data[data['company_permalink']==company_permalink]
        test_file = 'feature/cleaned_test_app/' + company_permalink+'_next_' + next_round+'.csv'
        test_set = pd.read_csv(test_file, index_col = 0)
        
        
        feature_cols=['categories','location', 'round_match','previous_round_norm', 'competitor_norm', 'network_1st_norm','network_23_norm']
        
        logistic.fit(train_set[feature_cols], train_set['label'])
        
        prob_result = logistic.predict_proba(test_set[feature_cols])
        prob_result_df = pd.DataFrame(prob_result, index = test_set.index).rename(columns={0:'neg',1:'pos'})
        
        final = test_set.join(prob_result_df)
        final_indexed = final.sort('pos', ascending=False).set_index('investor_permalink')
        
        predicted_investors = final_indexed[:30].index.unique()
       
        investors = str(tuple(predicted_investors))
    
        #print investors
        with con:
            cur = con.cursor()
            sql = 'select permalink, org_name,  location_city, categories from organization where permalink in ' + investors
            cur.execute(sql)
            query_results_org = cur.fetchall()
        with con:
            cur = con.cursor()
            sql = 'select permalink, first_name,  last_name, location_city, title from people where permalink in ' + investors
            cur.execute(sql)
            query_results_person = cur.fetchall()
            
        investor_list = []
        for result in query_results_org:
            probi = final_indexed.ix[result[0]]['pos']
            #label = final_indexed.ix[result[0]]['label']
            
            investor_list.append(dict(name=result[1], location_city=result[2], categories=result[3], investor_type='organization', prob = probi))
        for result in query_results_person:
            probi = final_indexed.ix[result[0]]['pos']
            #label = final_indexed.ix[result[0]]['label']
        
            investor_list.append(dict(name=result[1]+' '+result[2], location_city=result[3], categories=result[4], investor_type='person', prob = probi))
        sorted_investor_list = sorted(investor_list, key=operator.itemgetter('prob'), reverse = True)
        network_filename = '../static/network_json_new/'+company_permalink+'_next.json'
        con.close() # close mysql connection    
        
    return tuple(predicted_investors), sorted_investor_list, network_filename, company_origin_name, company_location, company_categories, next_round_display
