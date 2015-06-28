#!/usr/bin/env python
import glob
import pymysql as mdb
import os

con = mdb.connect('localhost', 'root', '', 'crunchbase', charset='utf8')


def main():
    network_file_path = 'feature/network_json_new/'
    with con:
        cur = con.cursor()
        sql = 'select permalink from bayarea_post2012_fewer4_trim;'
        cur.execute(sql)
        results = cur.fetchall()
        count = 0
        for result in results:
            company_permalink = result[0]
        
            network_file = network_file_path + company_permalink+'_next.json'
        
            if os.path.exists(network_file):
                pass
            else:
                count += 1
                with con:
                    cur = con.cursor()
                    sql = 'delete from bayarea_post2012_fewer4_trim where permalink=%s;'
                    cur.execute(sql, (company_permalink))
                print count
            
        

if __name__ == '__main__':
    main()
