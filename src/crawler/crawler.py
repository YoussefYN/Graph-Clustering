#!/usr/bin/env python
# coding: utf-8


import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from bs4 import BeautifulSoup
import requests
import re
from tqdm import tqdm as tqdm
from collections import deque
import time



url_wiki = "https://en.wikipedia.org"
reg_exp = re.compile('href\=\"(\/wiki\/[A-Za-z\_]+)\" title')
initial_page_url = "/wiki/Reunification_Day"
page_names = "page_names.txt"
graph_path = "graph.txt"
MIN_PAGES = 100000
TIME_DELAY = 0.05


def get_links(page_url):
    page = BeautifulSoup(requests.get(url_wiki+page_url).content)
    body_content = page.find('div', {'id':'bodyContent'})
    result = reg_exp.findall(str(body_content))
    return result

if __name__ == "__main__":
    graph_ind = 0
    pages = {initial_page_url:graph_ind}
    graph_ind+=1
    queue = deque([initial_page_url])
    graph = []

    with tqdm(total=MIN_PAGES, desc='pages collected') as pbar:
        
        while len(queue) and graph_ind < MIN_PAGES:
            time.sleep(TIME_DELAY)
            current_page = queue.pop()
            for new_page_url in get_links(current_page):
                if new_page_url not in pages:
                    queue.append(new_page_url)
                    pages[new_page_url] = graph_ind
                    graph_ind+=1
                    graph.append((pages[current_page], pages[new_page_url]))
                    pbar.update(1)


    with open(page_names, 'w') as f:
        for k, v in pages.items():
            print(k, v, file=f)

    with open(graph_path, 'w') as f:
        for s, d in graph:
            print(s, d, file=f)

