#!/usr/bin/env python

import pandas as pd
from collections import defaultdict
from collections import Counter
import random
import time
import datetime
import numpy as np
#import pymysql as mdb
import sys

#con = mdb.connect('localhost', 'root', '', 'crunchbase', charset='utf8')

def main():
    companies_pool = pd.read_csv('../feature/post2012_not_in_training.csv', index_col = 0)
    companies = pd.read_csv('../feature/companies_post2012_merged.csv', index_col = 0).set_index('permalink')

    org_investors = pd.read_csv('../feature/selected_org_info.csv', index_col = 0)
    person_investors = pd.read_csv('../feature/selected_person_info.csv', index_col = 0)
    investments = pd.read_csv('../feature/selected_selected_investments.csv')
    investments['funded_month_time'] = investments.apply(lambda row:datetime.datetime.strptime(row['funded_month'],'%Y-%m'), axis = 1)
    investments['funding_round_type_code']=investments.apply(lambda row:row['funding_round_type']+'_'+row['funding_round_code'] if row['funding_round_type']=='venture' and row['funding_round_code'] is not None and row['funding_round_code']==row['funding_round_code'] else row['funding_round_type'], axis=1)

    #try:
    feature_extraction(companies_pool,companies,org_investors,person_investors,investments)
    #except:
        #print sys.exc_info()[0]
    #print feature_dict_df

def get_category_similarity_measure(company_categories, investee_history_df):
    cate = investee_history_df.dropna(subset=['categories']).apply(lambda row: row['categories'].strip('|').split('|'), axis=1)
    #print cate
    categories = [item for sublist in cate.tolist() for item in sublist if item!='']
    total = len(categories)
    count = 0
    freqs = Counter(categories)
    for c in company_categories:
        count += freqs[c]
        if total >0:
            cate_similarity = count/float(total)
        else: #if investee history has no categeries available
            cate_similarity = np.nan
    return cate_similarity

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

def feature_extraction(companies_pool, companies, org_investors,person_investors,investments):
    feature_dict = defaultdict(lambda:{'company_permalink':'', 'investor_permalink':'', 'round_permalink':'', 'round_type':'',
                                       'categories':0, 'location':0,  'round_match':0, 'round_match_imputation':0,  # 1 is imputed, 0 is not imputed
                                       'previous_round':0, 'label':0})
    
    investments_investor_indexed = investments.set_index('investor_permalink') #to-do: using multiple index
    investments_company_indexed = investments.set_index('company_permalink')
    investment_index = investments.set_index(['company_permalink','investor_permalink'])
    
    investor_pool = set(org_investors.index).union(set(person_investors.index))
    

    index = 0
    for company_permalink, row in companies_pool.iterrows():
        
        company_categories = row['categories'].strip('|').split('|')
        company_city = row['location_city'] 
        
        '''
        invests = investments_company_indexed.ix[[company_permalink]]
        funding_rounds = invests.drop_duplicates('funding_round_permalink')[['funding_round_permalink','funding_round_type_code','funded_month_time']]
        if type(funding_rounds) == pd.core.series.Series:
            funding_rounds_df = pd.DataFrame(funding_rounds).transpose().set_index('funding_round_permalink')
        else:
            funding_rounds_df = pd.DataFrame(funding_rounds).set_index('funding_round_permalink')
        
        
        for fr, f_round in funding_rounds_df.iterrows():
            pos_investors = set(invests[invests['funding_round_permalink']==fr]['investor_permalink'].unique())
            neg_investors = investor_pool.difference(pos_investors)
            cut_date = f_round['funded_month_time']
            crt_round = f_round['funding_round_type_code']
        
            for investor in pos_investors:
                
                invests_portfolio = investments_investor_indexed.ix[investor]  
                invests_portfolio_cut = invests_portfolio[invests_portfolio['funded_month_time']<cut_date]
                
                previous_round = len(invests_portfolio_cut[invests_portfolio_cut['company_permalink']==company_permalink])
                   
                if previous_round>0:
                    invests_portfolio_cut = invests_portfolio_cut[invests_portfolio_cut['company_permalink']!=company_permalink]
                   
                    
                investee_history = invests_portfolio_cut['company_permalink'].unique()
                
                if len(investee_history)>0:
                    investee_history_info = companies.ix[investee_history]
                    #categories similarity
                    category_similarity = get_category_similarity_measure(company_categories, investee_history_info)
        
                    #location similarity
                    location_similarity = get_similarity_measure('location_city', investee_history_info, company_city)
                
                    #funding round 
                    funding_round_similarity = get_similarity_measure('funding_round_type_code', invests_portfolio_cut, crt_round)
                        
                    feature_dict[index]['company_permalink'] = company_permalink
                    feature_dict[index]['investor_permalink'] = investor
                    feature_dict[index]['round_permalink'] = fr
                    feature_dict[index]['round_type'] = crt_round
                    feature_dict[index]['previous_round'] = previous_round
                    feature_dict[index]['categories'] = category_similarity
                    feature_dict[index]['location'] = location_similarity
                    feature_dict[index]['round_match'] = funding_round_similarity
                    feature_dict[index]['label'] = 1
                    
                    #print index
                    index += 1
          
            for investor in neg_investors:
                
                invests_portfolio = investments_investor_indexed.ix[investor]
                invests_portfolio_cut = invests_portfolio[invests_portfolio['funded_month_time']<cut_date]
               
                previous_round = len(invests_portfolio_cut[invests_portfolio_cut['company_permalink']==company_permalink])
                   
                if previous_round>0:
                    invests_portfolio_cut = invests_portfolio_cut[invests_portfolio_cut['company_permalink']!=company_permalink]
                
                
                investee_history = invests_portfolio_cut['company_permalink'].unique()
                
                if len(investee_history)>0:
                    investee_history_info = companies.ix[investee_history]
                    #categories similarity
                    category_similarity = get_category_similarity_measure(company_categories, investee_history_info)
        
                    #location similarity
                    location_similarity = get_similarity_measure('location_city', investee_history_info, company_city)
                
                    #funding round 
                    funding_round_similarity = get_similarity_measure('funding_round_type_code', invests_portfolio_cut, crt_round)
                    
                    feature_dict[index]['company_permalink'] = company_permalink
                    feature_dict[index]['investor_permalink'] = investor
                    feature_dict[index]['round_permalink'] = fr
                    feature_dict[index]['round_type'] = crt_round
                    feature_dict[index]['previous_round'] = previous_round
                    feature_dict[index]['categories'] = category_similarity
                    feature_dict[index]['location'] = location_similarity
                    feature_dict[index]['round_match'] = funding_round_similarity
                    feature_dict[index]['label'] = 0
                    #print index    
                    index += 1
        '''
        #feature for next future round
        for investor in investor_pool:
            invests_portfolio = investments_investor_indexed.ix[investor]
            previous_round = len(invests_portfolio[invests_portfolio['company_permalink']==company_permalink])
                   
            if previous_round>0:
                invests_portfolio = invests_portfolio[invests_portfolio['company_permalink']!=company_permalink]
            
            investee_history = invests_portfolio['company_permalink'].unique()
            
            if len(investee_history)>0:
                    investee_history_info = companies.ix[investee_history]
                    #categories similarity
                    #print investor
                    #print investee_history
                    #print investee_history_info
                    category_similarity = get_category_similarity_measure(company_categories, investee_history_info)
        
                    #location similarity
                    location_similarity = get_similarity_measure('location_city', investee_history_info, company_city)
                
                    #funding round 
                    
                    feature_dict[index]['company_permalink'] = company_permalink
                    feature_dict[index]['investor_permalink'] = investor
                    feature_dict[index]['round_permalink'] = 'next'
                    feature_dict[index]['round_type'] = 'next'
                    feature_dict[index]['previous_round'] = previous_round
                    feature_dict[index]['categories'] = category_similarity
                    feature_dict[index]['location'] = location_similarity
                    feature_dict[index]['round_match'] = np.nan
                    feature_dict[index]['round_match_imputation'] = 1
                    feature_dict[index]['label'] = np.nan
                    #print index    
                    index += 1
        feature_dict_df = pd.DataFrame.from_dict(feature_dict, orient='index')
        file_path = '../feature/feature_one_post2012/simple_feature_' + company_permalink + '.csv'
        feature_dict_df.to_csv(file_path)
        
        #reset feature_dict
        feature_dict = defaultdict(lambda:{'company_permalink':'', 'investor_permalink':'', 'round_permalink':'', 'round_type':'',
                                       'categories':0, 'location':0,  'round_match':0, 'round_match_imputation':0,  # 1 is imputed, 0 is not imputed
                                       'previous_round':0, 'label':0})
    
    return 1



if __name__ == '__main__':
    main()
