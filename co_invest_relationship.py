#!/usr/bin/env python

import pandas as pd
from collections import defaultdict


def main():
    investments = pd.read_csv('feature/selected_selected_investments.csv')
    #investments['funded_month_time'] = investments.apply(lambda row:datetime.datetime.strptime(row['funded_month'],'%Y-%m'), axis = 1)
    rounds = investments['funding_round_permalink'].unique()
    investments_indexed = investments.set_index('funding_round_permalink')

    relationship_dict = defaultdict(lambda:{'permalink1':'', 'permalink2':'', 'permalink1_type':'', 'permalink2_type':'', 'relationship':'co-invest', 'company_permalink':'','funding_round_permalink':'', 'started_on':''})
    
    index = 0
    for r in rounds:
        round_investments = investments_indexed.ix[[r]].set_index('investor_permalink')
        started_on = round_investments.iloc[0]['funded_month']+'-01'
        company_permalink = round_investments.iloc[0]['company_permalink']

        investors = round_investments.index
        
        for i, investor1 in enumerate(investors):
            investor1_type = round_investments.ix[investor1]['investor_type']
            for j in range(i+1, len(investors)):
                investor2 = investors[j]
                investor2_type = round_investments.ix[investor2]['investor_type']
                relationship_dict[index]['permalink1'] = investor1
                relationship_dict[index]['permalink2'] = investor2
                relationship_dict[index]['permalink1_type'] = investor1_type
                relationship_dict[index]['permalink2_type'] = investor2_type
                relationship_dict[index]['company_permalink'] = company_permalink
                relationship_dict[index]['started_on'] = started_on if started_on==started_on else '0000-00-00'
                relationship_dict[index]['funding_round_permalink'] = r
                index += 1
    
    df = pd.DataFrame.from_dict(relationship_dict, orient = 'index')
    df.to_csv('feature/co_investment_relationship.csv', index=False)
                
        


if __name__ == '__main__':
    main()