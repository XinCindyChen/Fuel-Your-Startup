#!/usr/bin/env python
import pandas as pd
import numpy as np
import random
import glob
import time
import datetime
from collections import Counter
import pymysql as mdb


con = mdb.connect('localhost', 'root', '', 'crunchbase', charset='utf8')
cur = con.cursor()


def main():
    letter_list = ['A', 'B', 'C', 'D','E','F','G','H']
    train_df = pd.read_csv('feature/cleaned_train_set.csv')
    simple_feature_path = 'feature/feature_one_post2012/*.csv'
    network_feature_path = 'feature/feature_network_post2012/'
    investments = pd.read_csv('feature/all_investments.csv')
    investments['funding_round_type_code']=investments.apply(lambda row:row['funding_round_type']+'_'+row['funding_round_code'] if row['funding_round_type']=='venture' and row['funding_round_code'] is not None and row['funding_round_code']==row['funding_round_code'] else row['funding_round_type'], axis=1)
    investments['funded_month_time'] = investments.apply(lambda row:datetime.datetime.strptime(row['funded_month'],'%Y-%m'), axis = 1)
    investments_investor_indexed = investments.set_index('investor_permalink') #to-do: using multiple index
    investments_company_indexed = investments.set_index('company_permalink')



    for sf in glob.glob(simple_feature_path):
        company_permalink = sf.split('_')[-1].split('.')[0]
        print company_permalink
        sf_df = pd.read_csv(sf, index_col = 0)
        sf_df['index'] = sf_df.apply(lambda row: str(row['company_permalink']) +'_' + str(row['investor_permalink']) + '_' + row['round_permalink'], axis =1)
        sf_df_indexed = sf_df.set_index('index')
        sf_selected = sf_df_indexed[['round_type', 'round_match_imputation','round_match','previous_round','categories','location']]
        
        nf = network_feature_path + 'feature_network_' + company_permalink + '.csv'
        nf_df = pd.read_csv(nf, index_col = 0)
        nf_df['index'] = nf_df.apply(lambda row: str(row['company_permalink']) +'_' + str(row['investor_permalink']) + '_' + row['round_permalink'], axis=1)
        nf_df_indexed = nf_df.set_index('index')
        
        full_feature = nf_df_indexed.join(sf_selected)
        
        #funding_rounds = full_feature['round_permalink'].unique()
        #funding_types = tuple(full_feature['round_type'].unique())
        
        #for fr in funding_rounds:
        #round_df = full_feature[full_feature['round_permalink']==fr]
        normalize_df = normalize_to_df(full_feature, ['competitor', 'network_1st','network_23', 'previous_round'], train_df)
        #if fr!='next':
            #file_name = company_permalink + '_' + fr.split('/')[-1] + '.csv'   
        #if fr=='next':
            
        invests = investments_company_indexed.ix[[company_permalink]]
    
        funding_rounds = invests.drop_duplicates('funding_round_permalink')[['funding_round_permalink','funding_round_type','funding_round_code','funded_month_time','funded_month']]
        if type(funding_rounds) == pd.core.series.Series:
            funding_rounds_df = pd.DataFrame(funding_rounds).transpose().set_index('funding_round_permalink')
        else:
            funding_rounds_df = pd.DataFrame(funding_rounds).set_index('funding_round_permalink').sort('funded_month_time')
    
        #num_v_rounds = Counter(list(funding_rounds_df['funding_round_type']))['venture']
            
        funding_types = funding_rounds_df['funding_round_type'].unique()
        #sql = "select funding_round_type, funding_round_code, funded_month from investment  where funding_round_permalink in " + str(tuple(funding_rounds)) + " group by funding_round_permalink order by funded_month;"  
        #cur.execute(sql)
        #results = cur.fetchall()
        
        freqs = Counter(funding_types)
        last_round = funding_rounds_df.iloc[-1]['funding_round_type']
        last_type_code = funding_rounds_df.iloc[-1]['funding_round_code']
        if last_round=='venture':
            if last_type_code == last_type_code:
                i=letter_list.index(last_type_code) + 1
                next_round = 'venture_' + str(letter_list[i])
            elif freqs['venture']>0:
                next_round = 'venture_' + str(letter_list[freqs['venture']])
        elif last_round=='angel':
            next_round = 'venture_A'
        elif last_round=='seed':
            next_round = 'angel'
        else:
            next_round = 'seed'
        
        
        normalize_df['round_match'] = normalize_df.apply(calculate_round_match, args=(next_round,investments_investor_indexed,), axis=1)
            
        file_name = company_permalink + '_next_' + next_round + '.csv'
        
        normalize_df.to_csv('feature/test_app_new/'+file_name)
  
    
                    

def calculate_round_match(row, next_round, investments_investor_indexed):
    investor_permalink = row['investor_permalink']
    invests_portfolio = investments_investor_indexed.ix[investor_permalink]
    funding_round_similarity = get_similarity_measure('funding_round_type_code', invests_portfolio, next_round)
    return funding_round_similarity
    
    
def get_similarity_measure(col_name, investee_history_df, company_info):
    instances = investee_history_df[col_name].dropna().tolist()
    if 'undisclosed' in instances: # I already filtered out undisclosed, should not exist, but just to double secure
        instances.remove('undisclosed')
    if '' in instances:
        instances.remove('')
    total = len(instances)
    freqs = Counter(instances)
    if type(company_info)==str:
        count = freqs[company_info]
    else:
        count = 0
        for c in company_info:
            count+= freqs[c]
            
    if total>0:
        similarity = count/float(total)
    else: similarity = np.nan
    return similarity

def normalize_to_df(df, col_names, train_df): # normalize with train_df
    for col_name in col_names:
        avg = np.mean(train_df[col_name].dropna())
        new_col = col_name + '_fill'
        df[new_col] = df.apply(lambda row: row[col_name] if row[col_name]==row[col_name] else avg, axis=1)
        max_v = max(train_df[new_col])
        min_v = min(train_df[new_col])
        new_col_2 = col_name + '_norm'
        df[new_col_2] = df.apply(lambda row: (row[new_col]-min_v)/float(max_v-min_v), axis=1)
    
    return df
        

if __name__ == '__main__':
    main()