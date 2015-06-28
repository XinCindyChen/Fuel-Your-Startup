#!/usr/bin/env python
import pandas as pd
import pymysql as mdb 
import glob
import os
import re
import shutil


con = mdb.connect('localhost', 'root', '', 'crunchbase', charset='utf8')
#cur = con.cursor()

def main():
    
    feature_file_path1 = 'feature/test_app/'
    feature_file_path2 = 'feature/test_app_new/'
    trg_file_path = 'feature/cleaned_test_app/'

    with con:
        cur = con.cursor()
        sql = 'select permalink, next_round from bayarea_post2012_fewer4;'
        cur.execute(sql)
        results = cur.fetchall()
        
        for result in results:
            permalink = result[0]
            next_round = result[1]
        
            file_name = permalink+'_next_'+ next_round + '.csv'
            
            file1 = feature_file_path1 + file_name
            file2 = feature_file_path2 + file_name
            target = trg_file_path + file_name
            
            if os.path.exists(file2):
                shutil.copyfile(file2, target)
                
            elif os.path.exists(file1):
                shutil.copyfile(file1, target)
                    


if __name__ == '__main__':
    main()