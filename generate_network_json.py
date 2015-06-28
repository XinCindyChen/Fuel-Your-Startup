#!/usr/bin/env python

import pandas as pd
import pymysql as mdb
import time
import datetime
import numpy as np
from collections import defaultdict
import json
import sys

con = mdb.connect('localhost', 'root', '', 'crunchbase', charset='utf8')
cur = con.cursor()

def main():
    companies_info = pd.read_csv('feature/bayarea_post2012_add.csv', index_col = 0)
    competitor_relationship = pd.read_csv('feature/competitor_relationship.csv', index_col = 0, low_memory=False)

    get_network(companies_info,competitor_relationship)
    
def sql_to_df(results, col_names):
    index = 0
    df_dict = defaultdict(dict)
    for result in results:
        for i, col in enumerate(col_names):
            df_dict[index][col] = result[i]
        index+=1
    df = pd.DataFrame.from_dict(df_dict, orient='index')
    df['funded_time'] = df.apply(lambda row:datetime.datetime.strptime(row['funded_month'],'%Y-%m'), axis = 1)
    sorted_df = df.sort('funded_time').set_index('funding_round_permalink')

    return sorted_df

def permalink_to_name(permalink, entity_type):
    if entity_type=='organization':
        sql = "select org_name from organization where permalink=%s;"
        cur.execute(sql, (permalink))
        name_result = cur.fetchall()
        if len(name_result)>0:
            name = name_result[0][0]
        else:
            name = None
        
    elif entity_type=='person':
        sql = 'select first_name, last_name from people where permalink = %s;'
        cur.execute(sql, (permalink))
        name_result = cur.fetchall()
        if len(name_result)>0:
            name = name_result[0][0] + ' ' + name_result[0][1]
        else:
            name = None
        
    return name

def find_networks(center_node, competitors, cut_date_str):
    nodes = defaultdict(dict)
    links = defaultdict(int)
    company_permalink = center_node['id']
    
    
    #1st layer
    #competitors
    valid_competitors = []
    if len(competitors)>0:
                if len(competitors)>1:
                    sql_competitor = "select permalink, org_name from organization where permalink in " + str(tuple(competitors)) + " and founded_on < %s;"
                else:
                    sql_competitor = "select permalink, org_name from organization where permalink = '" + tuple(competitors)[0] + "' and founded_on < %s;"
                cur.execute(sql_competitor, (cut_date_str))
                results = cur.fetchall()
                if len(results)>0:
                    #find competitors' investors
                    for result in results:
                        valid_competitors.append(result[0])
                        competitor_node = {'id':result[0],'name':result[1], 'group':1, 'type':2}
                        nodes[result[0]] = competitor_node
                        link = (company_permalink, result[0])
                        links[link] = 1
    
    #people who work here
    valid_1st_people = []
    sql_work_people = "select people_permalink, first_name, last_name from org_people_relationship inner join people on org_people_relationship.people_permalink = people.permalink where org_people_relationship.org_permalink=%s and (started_on <%s or started_on is null);"
    cur.execute(sql_work_people, (company_permalink, cut_date_str))
    work_people_results = cur.fetchall()
    for worker in work_people_results:
        valid_1st_people.append(worker[0])
        worker_node = {'id': worker[0], 'name': worker[1]+' '+worker[2], 'group':1, 'type':1}
        nodes[worker[0]] = worker_node
        link = (company_permalink, worker[0])
        links[link] = 1
    
    #previous investors
    sql_investor_person = "select investor_permalink, first_name, last_name from investment inner join people on investment.investor_permalink = people.permalink where company_permalink=%s and investor_type='person';"
    cur.execute(sql_investor_person, (company_permalink))
    investor_person_results = cur.fetchall()
    for person in investor_person_results:
        if person[0] not in nodes:
            valid_1st_people.append(person[0])
            investor_person_node = {'id':person[0], 'name': person[1]+' '+person[2], 'group':1, 'type':1}
            nodes[person[0]] = investor_person_node
            link = (company_permalink, person[0])
            links[link] = 1
    
    valid_org_investors = []
    sql_investor_org = "select investor_permalink, org_name from investment inner join organization on investment.investor_permalink = organization.permalink where company_permalink=%s and investor_type = 'organization';"
    cur.execute(sql_investor_org, (company_permalink))
    investor_org_results = cur.fetchall()
    for org in investor_org_results:
        valid_org_investors.append(org[0])
        investor_org_node = {'id':org[0], 'name': org[1], 'group':1, 'type':0}
        nodes[org[0]] = investor_org_node
        link = (company_permalink, org[0])
        links[link] = 1
    
    
    #2nd layer
    #competitor's investors
    for comp in valid_competitors:
        sql_competitor_investor_org = "select investor_permalink, org_name from investment inner join organization on investment.investor_permalink = organization.permalink where company_permalink=%s and investor_type='organization';"
        cur.execute(sql_competitor_investor_org, (comp))
        competitor_investor_org_results = cur.fetchall()
        for competitor_investor_org in competitor_investor_org_results:
            if competitor_investor_org[0] not in nodes:
                competitor_investor_org_node = {'id':competitor_investor_org[0], 'name':competitor_investor_org[1], 'group':2, 'type':0}
                nodes[competitor_investor_org[0]] = competitor_investor_org_node
            link = (result[0],competitor_investor_org[0])
            links[link] = 1
        sql_competitor_investor_person = "select investor_permalink, first_name, last_name from investment inner join people on investment.investor_permalink = people.permalink where company_permalink=%s and investor_type='person';"
        cur.execute(sql_competitor_investor_person, (comp))
        competitor_investor_person_results = cur.fetchall()
        for competitor_investor_person in competitor_investor_person_results:
            if competitor_investor_person[0] not in nodes:
                competitor_investor_person_node = {'id':competitor_investor_person[0], 'name':competitor_investor_person[1]+' '+competitor_investor_person[2], 'group':2, 'type':1}
                nodes[competitor_investor_person[0]] = competitor_investor_person_node
            link = (result[0],competitor_investor_person[0])
            links[link] = 1
            
    # all 1st layer person's 2nd layer relationship companies worked in, co-invest, co-work
    for person in valid_1st_people:
        #preious worked companies
        sql_previous_worked_companies = "select org_permalink, org_name from org_people_relationship inner join organization on org_people_relationship.org_permalink = organization.permalink  where people_permalink = %s  and (started_on < %s or started_on is null);"
        cur.execute(sql_previous_worked_companies, (person, cut_date_str))
        worked_companies_results = cur.fetchall()
        for org in worked_companies_results:
            if org[0]!=company_permalink:
                if org[0] not in nodes:
                    worked_company_node = {'id': org[0], 'name': org[1], 'group':2, 'type': 0}
                    nodes[org[0]] = worked_company_node
                link = (person, org[0])
                link_reverse = (org[0], person)
                if link in links:
                    links[link] +=1
                elif link_reverse in links:
                    links[link_reverse] += 1
                else:
                    links[link] = 1
        #co-work
        sql_co_work1 = "select permalink2, first_name, last_name from co_work_relationship inner join people on co_work_relationship.permalink2 = people.permalink where started_on < %s and permalink1=%s;"
        cur.execute(sql_co_work1, (cut_date_str, person))
        co_workers1_results = cur.fetchall()
        for co_worker1 in co_workers1_results:
            if co_worker1[0] not in valid_1st_people:
                if co_worker1[0] not in nodes:
                    co_worker1_node = {'id': co_worker1[0], 'name':co_worker1[1] + ' ' +co_worker1[2], 'group':2, 'type':1}
                    nodes[co_worker1[0]] = co_worker1_node
                link = (person, co_worker1[0])
                link_reverse = (co_worker1[0], person)
                if link in links:
                    links[link] +=1
                elif link_reverse in links:
                    links[link_reverse] += 1
                else:
                    links[link] = 1
                    
        
        sql_co_work2 = "select permalink1, first_name, last_name from co_work_relationship inner join people on co_work_relationship.permalink1 = people.permalink where started_on < %s and permalink2=%s;"
        cur.execute(sql_co_work2, (cut_date_str, person))
        co_workers2_results = cur.fetchall()
        for co_worker2 in co_workers2_results:
            if co_worker2[0] not in valid_1st_people:
                if co_worker2[0] not in nodes:
                    co_worker2_node = {'id': co_worker2[0], 'name':co_worker2[1] + ' ' +co_worker2[2], 'group':2, 'type':1}
                    nodes[co_worker2[0]] = co_worker2_node
                link = (person, co_worker2[0])
                link_reverse = (co_worker2[0], person)
                if link in links:
                    links[link] +=1
                elif link_reverse in links:
                    links[link_reverse] += 1
                else:
                    links[link] = 1
        #co-invest
        sql_co_invest = "select permalink2, permalink2_type from co_investment_relationship where started_on <%s and permalink1=%s;"
        cur.execute(sql_co_invest, (cut_date_str, person))
        query_result = cur.fetchall()
        for result in query_result:
            name = permalink_to_name(result[0], result[1])
            if name is not None:
                if result[0] not in nodes:
                    if result[1] == 'person':
                        entity_type = 1
                    else: entity_type = 0
                    node = {'id': result[0], 'name': name, 'layer':2, 'type':entity_type}
                    nodes[result[0]] = node
                link = (person, result[0])
                link_reverse = (result[0], person)
                if link in links:
                    links[link] +=1
                elif link_reverse in links:
                    links[link_reverse] += 1
                else:
                    links[link] = 1
                
        sql_co_invest = "select permalink1, permalink1_type from co_investment_relationship where started_on <%s and permalink2=%s;"
        cur.execute(sql_co_invest, (cut_date_str, person))
        query_result = cur.fetchall()
        for result in query_result:
            name = permalink_to_name(result[0], result[1])
            if name is not None:
                if result[0] not in nodes:
                    if result[1] == 'person':
                        entity_type = 1
                    else: entity_type = 0
                    node = {'id': result[0], 'name': name, 'group':2, 'type':entity_type}
                    nodes[result[0]] = node
                link = (person, result[0])
                link_reverse = (result[0], person)
                if link in links:
                    links[link] +=1
                elif link_reverse in links:
                    links[link_reverse] += 1
                else:
                    links[link] = 1
    
    for org in valid_org_investors:
        #people who worked here
        sql_work_people = "select people_permalink, first_name, last_name from org_people_relationship inner join people on org_people_relationship.people_permalink = people.permalink where org_people_relationship.org_permalink=%s and (started_on <%s or started_on is null);"
        cur.execute(sql_work_people, (org, cut_date_str))
        work_people_results = cur.fetchall()
        for worker in work_people_results:
            if worker[0] not in nodes:
                worker_node = {'id': worker[0], 'name': worker[1]+' '+worker[2], 'group':2, 'type':1}
                nodes[worker[0]] = worker_node
            link = (org, worker[0])
            link_reverse = (worker[0], org)
            if link in links:
                links[link] +=1
            elif link_reverse in links:
                links[link_reverse] += 1
            else:
                links[link] = 1    
    
        #co-invest
        sql_co_invest = "select permalink2, permalink2_type from co_investment_relationship where started_on <%s and permalink1=%s;"
        cur.execute(sql_co_invest, (cut_date_str, org))
        query_result = cur.fetchall()
        for result in query_result:
            name = permalink_to_name(result[0], result[1])
            if name is not None:
                if result[0] not in nodes:
                    if result[1] == 'person':
                        entity_type = 1
                    else: entity_type = 0
                    node = {'id': result[0], 'name': name, 'group':2, 'type':entity_type}
                    nodes[result[0]] = node
                link = (org, result[0])
                link_reverse = (result[0], org)
                if link in links:
                    links[link] +=1
                elif link_reverse in links:
                    links[link_reverse] += 1
                else:
                    links[link] = 1
                
        sql_co_invest = "select permalink1, permalink1_type from co_investment_relationship where started_on <%s and permalink2=%s;"
        cur.execute(sql_co_invest, (cut_date_str, org))
        query_result = cur.fetchall()
        for result in query_result:
            name = permalink_to_name(result[0], result[1])
            if name is not None:
                if result[0] not in nodes:
                    if result[1] == 'person':
                        entity_type = 1
                    else: entity_type = 0
                    node = {'id': result[0], 'name': name, 'group':2, 'type':entity_type}
                    nodes[result[0]] = node
                link = (org, result[0])
                link_reverse = (result[0], org)
                if link in links:
                    links[link] +=1
                elif link_reverse in links:
                    links[link_reverse] += 1
                else:
                    links[link] = 1
                    
    nodes[company_permalink] = center_node
    return (nodes, links)

def to_json(nodes, links, file_name):
    network_dict = dict()
    nodes_list = nodes.values()
    network_dict['nodes'] = nodes_list
    
    links_list = []
    for k,v in links.iteritems():
        
        s = nodes_list.index(nodes[k[0]])
        t = nodes_list.index(nodes[k[1]])
        links_list.append({'source':s, 'target':t, 'weight': v})
    network_dict['links'] = links_list
    try:
        j = json.dumps(network_dict)
        f = open('feature/network_json_new/' + file_name, 'w')
        print >> f, j
        f.close()
    except UnicodeDecodeError:
        print sys.exc_info()[0]
        
    
    
    

def get_network(companies_info, competitor_relationship):

    for company_permalink, info in companies_info[1200:].iterrows():
        #company_permalink = 'adsnative'
        #info = companies_info.ix[company_permalink]
        print company_permalink
       
        center_node = {'id':company_permalink, 'name':info['org_name'], 'group':0, 'type':0} #type, 0 is org, 1 is person, 2 is competitor
        if company_permalink in competitor_relationship.index:
            competitors = set(competitor_relationship.ix[company_permalink])
            competitors.discard(np.nan) 
        else:
            competitors = set()
        
            
        sql = "select distinct funding_round_permalink,  funding_round_type, funding_round_code, funded_month from investment where company_permalink = %s and funding_round_type in ('seed','angel','venture');"
        cur.execute(sql, (company_permalink))
        fr_results = cur.fetchall()
        
        if len(fr_results)>0:
            round_df = sql_to_df(fr_results, ['funding_round_permalink',  'funding_round_type', 'funding_round_code', 'funded_month'])
        '''
            num_venture = 0
            
            for round_permalink, f_round in round_df.iterrows():
                if f_round['funding_round_type']=='venture':
                    num_venture += 1
                    if num_venture >4:
                        break
                cut_date_str = f_round['funded_month']+'-01'
                nodes, links = find_networks(center_node, competitors, cut_date_str)
                
                if f_round['funding_round_code'] is not None:
                    file_name = company_permalink + '_' + round_permalink.split('/')[-1] + '_' + f_round['funding_round_type'] + f_round['funding_round_code'] + '.json'
                else:
                    file_name = company_permalink + '_' + round_permalink.split('/')[-1] + '_' + f_round['funding_round_type']  + '.json'
            
                to_json(nodes, links, file_name)
        '''    
            
        if len(fr_results)==0 or len(round_df[round_df['funding_round_type']=='venture'])<4:
            nodes, links = find_networks(center_node, competitors, '2015-07-01')
            
            file_name = company_permalink + '_next.json'
            to_json(nodes, links, file_name)
        




if __name__ == '__main__':
    main()