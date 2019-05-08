#!/usr/bin/env python
# coding: utf-8

# In[1]:


import random
from collections import deque
from tqdm import tqdm_notebook as tqdm


# In[2]:


def get_amounts(filtered_edges):
    nodes = set()
    edges_amount = 0
    for src in filtered_edges.keys():
        nodes.add(src)
        edges_amount += len(filtered_edges[src])
        for dst in filtered_edges[src]:
            nodes.add(dst)
    return (len(nodes), edges_amount)


# In[3]:
edges = {}
edges_inverse = {}
with open("../data/wiki-topcats.txt") as f:
    for line in f:
        src, dst = list(map(int, line.split()))
        if src in edges:
            edges[src].append(dst)
        else:
            edges[src] = [dst]
        if dst in edges_inverse:
            edges_inverse[dst].append(src)
        else:
            edges_inverse[dst] = [src]


# In[9]:


def add_new_edge(graph, src, dst, pbar):
    is_old_edge = True
    if src in graph:
        is_old_edge = (dst in graph[src])

        graph[src].add(dst)
    else:
        graph[src] = set([dst])
    pbar.update(not is_old_edge)
    return not is_old_edge


# In[4]:


MIN_NODES = 150000
MIN_EDGES = 1500000


# In[5]:


nodes_amount = 1
edges_amount = 0
edges_keys = list(edges.keys())
init_node = edges_keys[random.randint(0, len(edges_keys))]
filtered_edges = {}
queue = deque([init_node])
visited_nodes = set([init_node])
all_nodes = set([init_node])


# In[6]:


with tqdm(total=MIN_NODES, desc='nodes count') as pbar:
    while (len(all_nodes) <= MIN_NODES):
        current_node = queue.popleft()
        visited_nodes.add(current_node)
        if current_node in edges:
            for dst_node in edges[current_node]:
                if dst_node not in all_nodes:
                    queue.append(dst_node)
                    if dst_node not in all_nodes:
                        all_nodes.add(dst_node)
                        pbar.update(1)
                if current_node in filtered_edges:
                    filtered_edges[current_node].add(dst_node)
                else:
                    filtered_edges[current_node] = set([dst_node])
                if len(all_nodes) > MIN_NODES:
                    break


# In[7]:


not_added_nodes_queue = deque()


# In[10]:


with tqdm(total=MIN_EDGES, desc='edges count') as pbar:
    edges_amount = get_amounts(filtered_edges)[1]
    pbar.update(edges_amount)
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
                    


# In[1]:


print("nodes = {}, edges = {}".format(get_amounts(filtered_edges)))




# In[ ]:


with open('../data/filtered-wiki-topcats_dfs.txt', 'w') as f:
    for src in sorted(filtered_edges.keys()):
        for dst in sorted(filtered_edges[src]):
            print(src, dst, file = f)
            


# In[ ]:


with open('../data/wiki-topcats-page-names.txt', 'r') as f:
    page_names = []
    for line in f:
        page_names.append(" ".join(line.split()[1:]))


# In[ ]:


with open('../data/filtered-wiki-nodes_dfs.txt', 'w') as f:
    for node in sorted(list(all_nodes)):
        print(node, page_names[node], file=f)


# In[ ]:




