#!/usr/bin/env python
# coding: utf-8

import random
from collections import deque
from tqdm import tqdm as tqdm

MIN_NODES = 150000
MIN_EDGES = 1500000

def get_amounts(filtered_edges):
    """method returns the amoount of nodes and edges
    
    Arguments:
        filtered_edges {[dic(sets)]} -- [graph]
    
    Returns:
        [(int, int)] -- amount of nodes and edges
    """
    nodes = set()
    edges_amount = 0
    for src in filtered_edges.keys():
        nodes.add(src)
        edges_amount += len(filtered_edges[src])
        for dst in filtered_edges[src]:
            nodes.add(dst)
    return (len(nodes), edges_amount)

def read_graph():
    """method read graph from adjacency list
    
    Returns:
        [dic(sets)] -- graph representation
    """
    edges = {}
    with open("../data/wiki-topcats.txt") as f:
        print('reading the graph')
        for line in f:
            src, dst = list(map(int, line.split()))
            if src in edges:
                edges[src].append(dst)
            else:
                edges[src] = [dst]
    return edges

def add_new_edge(graph, src, dst, pbar):
    """method new edge to graph
    
    Arguments:
        graph {[dic(sets)]} -- [graph]
        src {[int]} -- [index of source node]
        dst {[int]} -- [index of destination node]
        pbar {[tqdm.pbar]} -- [progress bar]
    
    Returns:
        [bool] -- [True if edge is added, and False if it been]
    """
    is_old_edge = True
    if src in graph:
        is_old_edge = (dst in graph[src])

        graph[src].add(dst)
    else:
        graph[src] = set([dst])
    pbar.update(not is_old_edge)
    return not is_old_edge

def fill_nodes(filtered_edges, edges, queue, visited_nodes, all_nodes):
    """add nodes to filtered_edges(graph) from edges(graph) using bfs
    
    Arguments:
        filtered_edges {[dic(sets)]} -- [graph representation of new graph]
        edges {[dic(sets)]} -- [graph to shrink]
        queue {[deque(int)]} -- [queue with nodes, which will be considered as source nodes]
        visited_nodes {[set(int)]} -- [nodes which be as source nodes]
        all_nodes {[set(int)]} -- [indexes of nodes which are in the graph]
    """
    with tqdm(total=MIN_NODES, desc='nodes count') as pbar:
        while (len(all_nodes) <= MIN_NODES):
            current_node = queue.popleft() # queue for bfs
            visited_nodes.add(current_node) # for checking, if we be in this src node
            if current_node in edges:
                for dst_node in edges[current_node]:
                    if dst_node not in all_nodes:
                        queue.append(dst_node) # add new node in queue
                        if dst_node not in all_nodes: # to count how many nodes we add
                            all_nodes.add(dst_node) 
                            pbar.update(1)
                    if current_node in filtered_edges:
                        filtered_edges[current_node].add(dst_node)
                    else:
                        filtered_edges[current_node] = set([dst_node])
                    if len(all_nodes) > MIN_NODES:
                        break
    

def fill_edges(filtered_edges, edges, queue, visited_nodes, all_nodes):
    """add edges to make graph denser
    if queue is not empty we add new edge, only if source node and destination node already in new graph, and saving edges which we didn't add
    if queue empty we add one edge, which destination node is not added to new graph, and adding this node to queue
    
    Arguments:
        filtered_edges {[dic(sets)]} -- [graph representation of new graph]
        edges {[dic(sets)]} -- [graph to shrink]
        queue {[deque(int)]} -- [queue with nodes, which will be considered as source nodes]
        visited_nodes {[set(int)]} -- [nodes which be as source nodes]
        all_nodes {[set(int)]} -- [indexes of nodes which are in the graph]
    """
    with tqdm(total=MIN_EDGES, desc='edges count') as pbar:
        edges_amount = get_amounts(filtered_edges)[1]
        pbar.update(edges_amount)
        not_added_nodes_queue = deque()
        while (edges_amount <= MIN_EDGES) and (len(queue)!=0 or len(not_added_nodes_queue)!=0):
            if len(queue)!= 0:
                current_node = queue.popleft()
            else:
                src_node, current_node = not_added_nodes_queue.popleft()
                edges_amount += add_new_edge(filtered_edges, src_node, current_node, pbar)
            if current_node in visited_nodes:
                continue
            visited_nodes.add(current_node)
            
            if current_node in edges:
                for dst_node in edges[current_node]:
                    if dst_node in all_nodes:
                        edges_amount += add_new_edge(filtered_edges, current_node, dst_node, pbar)
                        if edges_amount > MIN_EDGES:
                            break              
                    else:
                        not_added_nodes_queue.append((current_node, dst_node))
                        

if __name__ == "__main__":
    """shirnkage of graph
    """
    edges = read_graph()

    edges_keys = list(edges.keys())
    init_node = edges_keys[random.randint(0, len(edges_keys))]
    filtered_edges = {}
    queue = deque([init_node])
    visited_nodes = set([init_node])
    all_nodes = set([init_node])
    fill_nodes(filtered_edges, edges, queue, visited_nodes, all_nodes)
    fill_edges(filtered_edges, edges, queue, visited_nodes, all_nodes)

    print("nodes = {}, edges = {}".format(*get_amounts(filtered_edges)))

    with open('../data/filtered-wiki-topcats_dfs.txt', 'w') as f:
        for src in sorted(filtered_edges.keys()):
            for dst in sorted(filtered_edges[src]):
                print(src, dst, file = f)
                
    with open('../data/wiki-topcats-page-names.txt', 'r') as f:
        page_names = []
        for line in f:
            page_names.append(" ".join(line.split()[1:]))

    with open('../data/filtered-wiki-nodes_dfs.txt', 'w') as f:
        for node in sorted(list(all_nodes)):
            print(node, page_names[node], file=f)



