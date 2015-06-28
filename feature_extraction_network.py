#!/usr/bin/env python

import pandas as pd
from collections import defaultdict
from collections import Counter
import random
import time
import datetime
import numpy as np
import pymysql as mdb
import sys

con = mdb.connect('localhost', 'root', '', 'crunchbase', charset='utf8')
cur = con.cursor()


def main():
    #companies = pd.read_csv('../feature/selected_companies_info.csv', index_col = 0)
    
    companies_pool = pd.read_csv('../feature/post2012_not_in_training.csv', index_col = 0)
    companies = pd.read_csv('../feature/companies_post2012_merged.csv', index_col = 0).set_index('permalink')
    
    org_investors = pd.read_csv('../feature/selected_org_info.csv', index_col = 0)
    person_investors = pd.read_csv('../feature/selected_person_info.csv', index_col = 0)
    competitor_relationship = pd.read_csv('../feature/competitor_relationship.csv', index_col = 0, low_memory=False)
    investments = pd.read_csv('../feature/selected_selected_investments.csv')
    investments['funded_month_time'] = investments.apply(lambda row:datetime.datetime.strptime(row['funded_month'],'%Y-%m'), axis = 1)
    investments['funding_round_type_code']=investments.apply(lambda row:row['funding_round_type']+'_'+row['funding_round_code'] if row['funding_round_type']=='venture' and row['funding_round_code'] is not None and row['funding_round_code']==row['funding_round_code'] else row['funding_round_type'], axis=1)

    #try:
    feature_extraction(companies,companies_pool, org_investors,person_investors,investments, competitor_relationship)
    #except:
        #print sys.exc_info()
    #print feature_dict_df

def find_3rd_degree_network(company_permalink, cut_date_str, cut_date, invests):
        sql = "select people_permalink from org_people_relationship where org_permalink=%s and (started_on <%s or started_on is null);"
        cur.execute(sql, (company_permalink, cut_date_str))
        query_result = cur.fetchall()

        all_workers =list()
        for result in query_result:
            all_workers.append(result[0])
        
        if cut_date==cut_date:    
            all_previous_org_investors = list(invests[(invests['funded_month_time']<cut_date) &(invests['investor_type']=='organization') ]['investor_permalink'].unique())
            all_previous_person_investors = list(invests[(invests['funded_month_time']<cut_date) &(invests['investor_type']=='person') ]['investor_permalink'].unique())
        else:
            all_previous_org_investors = list(invests[invests['investor_type']=='organization' ]['investor_permalink'].unique())
            all_previous_person_investors = list(invests[invests['investor_type']=='person']['investor_permalink'].unique())
        
        person_co_invest_list = list()
        person_co_work_list = list()
        person_work_list = list()
        org_co_invest_list = list()
        org_work_list = list()
        people_1st = set(all_workers + all_previous_person_investors)

        for person in people_1st:
            sql_co_invest = "select permalink1, permalink2 from co_investment_relationship where started_on <%s and (permalink1=%s or permalink2=%s);"
            cur.execute(sql_co_invest, (cut_date_str, person, person))
            query_result = cur.fetchall()
            for result in query_result:
                person_co_invest_list.append(result[0])
                person_co_invest_list.append(result[1])
            
            sql_co_work = "select permalink1, permalink2 from co_work_relationship where started_on < %s and (permalink1=%s or permalink2=%s);"
            cur.execute(sql_co_work, (cut_date_str, person, person))
            query_result = cur.fetchall()
            for result in query_result:
                person_co_work_list.append(result[0])
                person_co_work_list.append(result[1])
            
            sql_work = "select org_permalink from org_people_relationship where people_permalink=%s and (started_on <%s or started_on is null);"
            cur.execute(sql_work, (person, cut_date_str))
            query_result = cur.fetchall()
            for result in query_result:
                person_work_list.append(result[0])
                
        
        for org in all_previous_org_investors:
            sql_co_invest = "select permalink1, permalink2 from co_investment_relationship where started_on <%s and (permalink1=%s or permalink2=%s);"
            cur.execute(sql_co_invest, (cut_date_str, org, org))
            query_result = cur.fetchall()
            for result in query_result:
                org_co_invest_list.append(result[0])
                org_co_invest_list.append(result[1])
            
            sql_work = "select people_permalink from org_people_relationship where org_permalink=%s and (started_on <%s or started_on is null);"
            cur.execute(sql_work, (org, cut_date_str))
            query_result = cur.fetchall()
            for result in query_result:
                org_work_list.append(result[0])
                
        nodes_1 = list(people_1st)+ all_previous_org_investors
        network1_size = len(nodes_1)
        nodes_123 = person_co_invest_list + person_co_work_list + person_work_list + org_co_invest_list + org_work_list
        nodes_23 = [node for node in nodes_123 if node not in nodes_1]
        freqs = Counter(nodes_23)
        network2_size = len(freqs)
        
        
        return (all_workers, freqs, network1_size, network2_size)    
    

def feature_extraction(companies,companies_pool, org_investors,person_investors,investments, competitor_relationship):
    feature_dict = defaultdict(lambda:{'company_permalink':'', 'investor_permalink':'', 'round_permalink':'', 
                                        'competitor':0, 'competitor_imputation':0,  # 1 is imputed, 0 is not imputed
                                       'network_1st':0, 'network_23':0,
                                       'label':0})
    #network_size_dict = defaultdict(lambda:{'company_permalink':'', 'round_permalink':'', 'funding_round_type_code':'', 'round_time_str':'', 'network1_size':0, 'network2_size':0})
    
    investments_investor_indexed = investments.set_index('investor_permalink') #to-do: using multiple index
    investments_company_indexed = investments.set_index('company_permalink')
    investment_index = investments.set_index(['company_permalink','investor_permalink'])
    
    investor_pool = set(org_investors.index).union(set(person_investors.index))
    

    index = 0
    nt_index = 0
    for company_permalink, row in companies_pool.iterrows():
        
        company_categories = row['categories'].strip('|').split('|')
        company_city = row['location_city']
        if company_permalink in competitor_relationship.index:
            competitors = set(competitor_relationship.ix[company_permalink])
            #competitors.discard(np.nan) #np.nan doesn't really influence anything, so do not need to remove
        else:
            competitors = set()
            num_competitor = np.nan
            competitor_imputation = 1
            
        
        invests = investments_company_indexed.ix[[company_permalink]]
        
        funding_rounds = invests.drop_duplicates('funding_round_permalink')[['funding_round_permalink','funding_round_type','funding_round_type_code','funded_month_time','funded_month']]
        if type(funding_rounds) == pd.core.series.Series:
            funding_rounds_df = pd.DataFrame(funding_rounds).transpose().set_index('funding_round_permalink')
        else:
            funding_rounds_df = pd.DataFrame(funding_rounds).set_index('funding_round_permalink')
        
        num_v_rounds = Counter(list(funding_rounds_df['funding_round_type']))['venture']
        
        '''
        for fr, f_round in funding_rounds_df.iterrows():
            pos_investors = set(invests[invests['funding_round_permalink']==fr]['investor_permalink'].unique())
            neg_investors = investor_pool.difference(pos_investors)
            cut_date = f_round['funded_month_time']
            cut_date_str = f_round['funded_month']+'-01'
            crt_round = f_round['funding_round_type_code']
            all_workers, nodes_23, network1_size, network2_size = find_3rd_degree_network(company_permalink, cut_date_str, cut_date, invests)
            
            network_size_dict[nt_index]['company_permalink'] = company_permalink
            network_size_dict[nt_index]['funding_round_type_code'] = crt_round
            network_size_dict[nt_index]['round_permalink'] = fr
            network_size_dict[nt_index]['round_time_str'] = cut_date_str
            network_size_dict[nt_index]['network1_size'] = network1_size
            network_size_dict[nt_index]['network2_size'] = network2_size
            nt_index+=1
            
            for investor in pos_investors:
               
                invests_portfolio = investments_investor_indexed.ix[investor]
                invests_portfolio_cut = invests_portfolio[invests_portfolio['funded_month_time']<cut_date]  
                    
                investee_history = invests_portfolio_cut['company_permalink'].unique()
                
                
                if len(investee_history)>0:
                    
                    network_1st = 1 if investor in all_workers else 0
                    network_23 = nodes_23[investor]
                    
                    investee_history_info = companies.ix[investee_history]
                    if len(competitors)>0:
                        num_competitor = len(competitors.intersection(set(investee_history)))
                        competitor_imputation = 0
                    
                    
                        
                    feature_dict[index]['company_permalink'] = company_permalink
                    feature_dict[index]['investor_permalink'] = investor
                    feature_dict[index]['round_permalink'] = fr
                    feature_dict[index]['competitor'] = num_competitor
                    feature_dict[index]['competitor_imputation'] = competitor_imputation
                    feature_dict[index]['network_1st'] = network_1st
                    feature_dict[index]['network_23'] = network_23
                    feature_dict[index]['label'] = 1
                    
                    #print index
                    index += 1
          
            for investor in neg_investors:
                
                invests_portfolio = investments_investor_indexed.ix[investor]
                invests_portfolio_cut = invests_portfolio[invests_portfolio['funded_month_time']<cut_date]
                
                investee_history = invests_portfolio_cut['company_permalink'].unique()
                
                if len(investee_history)>0:
                    network_1st = 1 if investor in all_workers else 0
                    network_23 = nodes_23[investor]
                    
                    investee_history_info = companies.ix[investee_history]
                    if len(competitors)>0:
                        num_competitor = len(competitors.intersection(set(investee_history)))
                        competitor_imputation = 0
                    
                        
                    feature_dict[index]['company_permalink'] = company_permalink
                    feature_dict[index]['investor_permalink'] = investor
                    feature_dict[index]['round_permalink'] = fr
                    feature_dict[index]['competitor'] = num_competitor
                    feature_dict[index]['competitor_imputation'] = competitor_imputation
                    feature_dict[index]['network_1st'] = network_1st
                    feature_dict[index]['network_23'] = network_23
                    feature_dict[index]['label'] = 0
                    #print index    
                    index += 1

       
        
                    
        network_size_dict[nt_index]['company_permalink'] = company_permalink
        network_size_dict[nt_index]['funding_round_type_code'] = 'next'
        network_size_dict[nt_index]['round_permalink'] = 'next'
        network_size_dict[nt_index]['round_time_str'] = '2015-07-01'
        network_size_dict[nt_index]['network1_size'] = network1_size
        network_size_dict[nt_index]['network2_size'] = network2_size
        nt_index+=1
        
        network_size_dict_df = pd.DataFrame.from_dict(network_size_dict, orient='index')
        file_path = '../feature/network_growth/network_growth_' + company_permalink + '.csv'
        network_size_dict_df.to_csv(file_path)
        
        network_size_dict = defaultdict(lambda:{'company_permalink':'', 'round_permalink':'', 'funding_round_type_code':'','round_time_str':'', 'network1_size':0, 'network2_size':0})
        nt_index = 0
        '''
         #feature for next future round
        all_workers, nodes_23, network1_size, network2_size = find_3rd_degree_network(company_permalink, '2015-07-01', np.nan, invests) #to-do: use the actual datetime object to str of today


        if num_v_rounds<4:
            for investor in investor_pool:
                invests_portfolio = investments_investor_indexed.ix[investor]            
                investee_history = invests_portfolio['company_permalink'].unique()
                
                if len(investee_history)>0:
                    network_1st = 1 if investor in all_workers else 0
                    network_23 = nodes_23[investor]
                    
                    investee_history_info = companies.ix[investee_history]
                    if len(competitors)>0:
                        num_competitor = len(competitors.intersection(set(investee_history)))
                        competitor_imputation = 0
                    
                        
                    feature_dict[index]['company_permalink'] = company_permalink
                    feature_dict[index]['investor_permalink'] = investor
                    feature_dict[index]['round_permalink'] = 'next'
                    feature_dict[index]['competitor'] = num_competitor
                    feature_dict[index]['competitor_imputation'] = competitor_imputation
                    feature_dict[index]['network_1st'] = network_1st
                    feature_dict[index]['network_23'] = network_23
                    feature_dict[index]['label'] = np.nan
                    #print index    
                    index += 1
        feature_dict_df = pd.DataFrame.from_dict(feature_dict, orient='index')
        file_path = '../feature/feature_network_post2012/feature_network_' + company_permalink + '.csv'
        feature_dict_df.to_csv(file_path)
        
        #reset feature_dict
        feature_dict = defaultdict(lambda:{'company_permalink':'', 'investor_permalink':'', 'round_permalink':'', 
                                        'competitor':0, 'competitor_imputation':0,  # 1 is imputed, 0 is not imputed
                                       'network_1st':0, 'network_23':0,
                                       'label':0})
        index = 0
    
    return 1

    
        
        


if __name__ == '__main__':
    main()
