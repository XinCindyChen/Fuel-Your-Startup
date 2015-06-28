#!/usr/bin/env python

import pandas as pd
from collections import defaultdict
import time
import datetime
import numpy as np
#import pymysql as mdb

#con = mdb.connect('localhost', 'root', '', 'crunchbase', charset='utf8')
#cur = con.cursor()


def main():

    people_org_relationship = pd.read_csv('feature/org_people_relationship_view.csv', index_col = 0)
    people_org_relationship['started_time'] = people_org_relationship.apply(lambda row:datetime.datetime.strptime(row['started_on'],'%Y-%m-%d') if row['started_on']==row['started_on'] else np.nan, axis = 1)

    relationship_dict = defaultdict(lambda:{'permalink1':'', 'permalink2':'', 'permalink1_position':'', 'permalink2_position':'',
                                            'permalink1_title':'', 'permalink2_title':'',
                                            'relationship':'co-work', 'company_permalink':'', 'started_on':''})
    
    index = 0
            
    
    org_list = people_org_relationship.index.unique()
    
     
    for org in org_list:
        
        data = people_org_relationship.ix[[org]].set_index('people_permalink')
        person_list = data.index.unique()

        
        for i, person1 in enumerate(person_list):
            #one person can have multiple relationship with one organization, use the earliest one
            person1_data = data.ix[[person1]].sort('started_time', ascending=False)
            person1_position = person1_data.iloc[-1]['relationship']
            person1_title = person1_data.iloc[-1]['title']
            person1_time = person1_data.iloc[-1]['started_time']
            for j in range(i+1, len(person_list)):
                person2 = person_list[j]
                person2_data = data.ix[[person1]].sort('started_time', ascending=False)
                person2_position = person2_data.iloc[-1]['relationship']
                person2_title = person2_data.iloc[-1]['title']
                person2_time = person2_data.iloc[-1]['started_time']

                #print person1_time
                #print person2_time
                if person1_time==person1_time and person2_time==person2_time:
                    started_time = max(person1_time, person2_time)
                elif person1_time==person1_time and person2_time!=person2_time:
                    started_time=person1_time
                elif person1_time!=person1_time and person2_time==person2_time :
                    started_time = person2_time
                else:
                    started_time = np.nan
                        
                relationship_dict[index]['permalink1'] = person1
                relationship_dict[index]['permalink2'] = person2
                relationship_dict[index]['permalink1_position'] = person1_position
                relationship_dict[index]['permalink2_position'] = person2_position
                relationship_dict[index]['permalink1_title'] = person1_title
                relationship_dict[index]['permalink2_title'] = person2_title
                relationship_dict[index]['company_permalink'] = org
                relationship_dict[index]['started_on'] =started_time.strftime('%Y-%m-%d') if started_time==started_time and started_time.year>1900 else '0000-00-00' 


                index += 1
    
    df = pd.DataFrame.from_dict(relationship_dict, orient = 'index')
    df.to_csv('feature/co_work_relationship.csv', index=False)
                
        


if __name__ == '__main__':
    main()