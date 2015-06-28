#!/usr/bin/env python
import pandas as pd
import pymysql as mdb 
import glob
import os
import re


con = mdb.connect('localhost', 'root', '', 'crunchbase', charset='utf8')
#cur = con.cursor()

def main():
    #investments = pd.read_csv('feature/selected_selected_investments.csv')
    #investments['funding_round_type_code']=investments.apply(lambda row:row['funding_round_type']+'_'+row['funding_round_code'] if row['funding_round_type']=='venture' and row['funding_round_code'] is not None and row['funding_round_code']==row['funding_round_code'] else row['funding_round_type'], axis=1)
    #investments['funded_month_time'] = investments.apply(lambda row:datetime.datetime.strptime(row['funded_month'],'%Y-%m'), axis = 1)
    #investments_investor_indexed = investments.set_index('investor_permalink') #to-do: using multiple index
    #investments_company_indexed = investments.set_index('company_permalink')
    
    #fewer4_list = []
      
    
    feature_file_path = 'feature/test_app_new/'
    with con:
        cur = con.cursor()
        sql = 'select permalink, org_name, categories, profile_image_url, location_city, founded_date from bayarea_post2012;'
        cur.execute(sql)
        results = cur.fetchall()
    
    for result in results:
        permalink = result[0]
        org_name = result[1]
        categories = result[2]
        profile_image_url = result[3]
        location_city = result[4]
        founded_date = result[5]
        
        file_name = permalink+'_next_'+'*.csv'
        
        for ffile in glob.glob(feature_file_path+file_name):
           
            next_round = ffile.split('.')[0].split('_next_')[-1]
            next_round1 = next_round.replace('venture_', 'Series ')
            next_round2 = next_round1.title()
            
            
            
            
            with con:
                cur = con.cursor()
                sql_insert = 'Update bayarea_post2012_fewer4 set next_round=%s, next_round_c=%s where permalink=%s;'
                cur.execute(sql_insert, (next_round, next_round2, permalink))
            
    
        
       
        
    

if __name__ == '__main__':
    main()

