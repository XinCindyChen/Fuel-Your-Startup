#!/usr/bin/env python

import pandas as pd
import sys
import glob
import random
import numpy as np

def main():
    simple_feature_path = 'feature/feature_one/*.csv'
    network_feature_path = 'feature/feature_network/'
    train_set = pd.DataFrame()
    
    for sf in glob.glob(simple_feature_path):
        company_permalink = sf.split('_')[-1].split('.')[0]
        sf_df = pd.read_csv(sf, index_col = 0)
        sf_df['index'] = sf_df.apply(lambda row: str(row['company_permalink']) +'_' + str(row['investor_permalink']) + '_' + row['round_permalink'], axis =1)
        sf_df_indexed = sf_df.set_index('index')
        sf_selected = sf_df_indexed[['round_type', 'round_match_imputation','round_match','previous_round','categories','location']]
        
        nf = network_feature_path + 'feature_network_' + company_permalink + '.csv'
        nf_df = pd.read_csv(nf, index_col = 0)
        nf_df['index'] = nf_df.apply(lambda row: str(row['company_permalink']) +'_' + str(row['investor_permalink']) + '_' + row['round_permalink'], axis=1)
        nf_df_indexed = nf_df.set_index('index')
        
        full_feature = nf_df_indexed.join(sf_selected) # nf has to be left, because it has fewer rounds, some doesn't have next round
        pos = full_feature[full_feature['label']==1]
        neg = full_feature[full_feature['label']==0]
        if len(pos)>0:
            if len(neg)> 10*len(pos):
                neg_sample_index = random.sample(neg.index, 10*len(pos))
                neg_sample = neg.ix[neg_sample_index]
            else:
                print company_permalink
                neg_sample = neg
            train_set = pd.concat([train_set, pos, neg_sample])
    
    normalize_train_set = normalize_df(train_set, ['competitor', 'network_1st','network_23', 'previous_round'])
    normalize_train_set.to_csv('feature/cleaned_train_set.csv')
    
def normalize_df(df, col_names):
    for col_name in col_names:
        avg = np.mean(df[col_name].dropna())
        new_col = col_name + '_fill'
        df[new_col] = df.apply(lambda row: row[col_name] if row[col_name]==row[col_name] else avg, axis=1)
        max_v = max(df[new_col])
        min_v = min(df[new_col])
        new_col_2 = col_name + '_norm'
        df[new_col_2] = df.apply(lambda row: (row[new_col]-min_v)/float(max_v-min_v), axis=1)
    
    return df



if __name__ == '__main__':
    main()