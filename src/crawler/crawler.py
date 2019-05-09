#!/usr/bin/env python
# coding: utf-8

from bs4 import BeautifulSoup
import requests
import re
from tqdm import tqdm as tqdm
from collections import deque
import time
import sys

def get_links(page_url, reg_exp):
    """getting article links from page
    
    Arguments:
        page_url {[string]} -- [relative path]
        reg_exp {[re.compile]} -- [regular expression]
    
    Returns:
        [iterable(string)] -- [iterable of url]
    """
    page = BeautifulSoup(requests.get(url_wiki+page_url).content)
    body_content = page.find('div', {'id':'bodyContent'})
    result = reg_exp.findall(str(body_content))
    return result

if __name__ == "__main__":
    """wikipedia crawler
    output: adjacency list of crawled wikipedia articles (graph)
    output: description for each node
    arg1: page in which crawler starts
    arg2: path to save adjacency list
    arg3: path to save list of description for each node (wiki url)
    arg4: amount of page to crawl
    arg5: time delay, to avoid ban
    """
    url_wiki = "https://en.wikipedia.org"
    reg_exp = re.compile('href\=\"(\/wiki\/[A-Za-z\_]+)\" title')
    initial_page_url = "/wiki/Reunification_Day"
    page_names = "page_names.txt"
    graph_path = "graph.txt"
    MIN_PAGES = 100000
    TIME_DELAY = 0.1

    if len(sys.argv) > 1:
        initial_page_url = sys.argv[1]
    if len(sys.argv) > 3:
        graph_path = sys.argv[2]
        page_names = sys.argv[3]
    if len(sys.argv) > 4:
        MIN_PAGES = int(sys.argv[4])
    if len(sys.argv) > 5:
        TIME_DELAY = float(sys.argv[5])
        
    graph_ind = 0
    pages = {initial_page_url:graph_ind}
    graph_ind +=1
    queue = deque([initial_page_url])
    graph = []

    with tqdm(total=MIN_PAGES, desc='pages collected') as pbar:
        while len(queue) and graph_ind < MIN_PAGES:
            time.sleep(TIME_DELAY) # delay for avoid ban
            current_page = queue.pop() # queue for dfs
            for new_page_url in get_links(current_page, reg_exp):
                if new_page_url not in pages: # checking do we add this page to queue
                    queue.append(new_page_url) #
                    pages[new_page_url] = graph_ind 
                    graph_ind+=1
                    graph.append((pages[current_page], pages[new_page_url]))
                    pbar.update(1)

    with open(page_names, 'w') as f:
        for k, v in pages.items():
            print(v, k, file=f)

    with open(graph_path, 'w') as f:
        for s, d in graph:
            print(s, d, file=f)

