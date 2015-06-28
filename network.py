#!/usr/bin/env python
import pandas as pd
import networkx as nx
from networkx import graphviz_layout
import matplotlib.pyplot as plt

def main():
    investment = pd.read_csv('data/crunchbase_export_investments.csv')
    us_com = investment[investment['company_country_code']=='USA']
    clean = us_com.dropna(subset=['company_permalink','investor_permalink'])
    subset = clean[clean['company_market']=='Analytics'][:100]
    
    draw_graph(subset)


def draw_graph(df):
    
    G=nx.DiGraph()

    for row in df.iterrows():
        target = row[1]['company_permalink']
        source = row[1]['investor_permalink']
        G.add_node(target, color='green')
        G.add_node(source,color='green')
        G.add_edge(target, source, color='blue')
    
    # create networkx graph
    
    print G.number_of_nodes()
    
    # draw graph
    #pos = nx.shell_layout(G)
    #pos=nx.graphviz_layout(G,prog='twopi',args='')
    nx.draw(G, node_color="blue", alpha = 0.5)

    # show graph
    plt.show()

# draw example


if __name__ == '__main__':
    main()
