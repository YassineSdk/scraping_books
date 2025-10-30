import pandas as pd 
import numpy as np 
from playwright.sync_api import sync_playwright
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from tqdm import tqdm
import csv 

from functions import get_links,getting_meta_data,log_summary


#wrapper  function
def full_scrape_job(start,end,name,idx):
    """
    wrapper of the functions
    """
    startime = datetime.now()
    print(f"🚀 Starting job: {name} (pages {start}–{end})")
    links = get_links(start,end)
    file_name , errors , total_links = getting_meta_data(start,end,name,links,idx)

    endtime = datetime.now()
    log_summary(name,total_links,errors,startime,endtime)
    return file_name




# deviding workload into chunks
jobs = [
    (1,2,"first_job",0),
    (3,5,"second_job",1),
    (6,7,"third_job",2)
]

with ThreadPoolExecutor(max_workers=3) as executer:
    futures = [executer.submit(full_scrape_job,start,end,name,idx) for start , end , name,idx in jobs ]
    for f in futures:
        f.result()



