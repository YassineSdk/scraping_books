import pandas as pd 
import numpy as np 
from playwright.sync_api import sync_playwright
import time 
from tqdm import tqdm 

def get_links(num_page:int):
    links = []
    with sync_playwright() as pw :
        browser = pw.firefox.launch(headless = False)
        page = browser.new_page()
        for p in range(1,num_page + 1):
            page.goto(f"https://manybooks.net/search-book?field_genre%5B10%5D=10&page={p}")
            books = page.locator("div.field.field--name-field-title.field--type-string.field--label-hidden.field--item a")
            count = books.count()
            for i in range(1,count):
                link = f"https://manybooks.net{books.nth(i).get_attribute('href')}"
                links.append(link)
        browser.close()
        return links



def getting_meta_data(num_pages):
    metadata = []
    links = get_links(num_pages)
    with sync_playwright() as pw :
        browser = pw.firefox.launch(headless=False)
        page = browser.new_page()
        for link in tqdm(links):
            try:
                page.goto(link)
                time.sleep(1)
                name = page.locator('[itemprop="name"]').text_content()
                author = page.locator('[itemprop="author"]').text_content()
                desc = page.locator("div.field.field--name-field-description").text_content()
                year_pub = page.locator("div.field.field--name-field-published-year").text_content()
                download = page.locator("div.field.field--name-field-downloads").text_content()
                metadata.append({
                    "name":name,
                    "author":author,
                    "description":desc,
                    "year_pub":year_pub,
                    "nb_download":download,
                    "link":link
                })
            except :
                continue

        browser.close()
        return metadata

data = getting_meta_data(30)
print(len(data))
data = pd.DataFrame(data)
data.to_csv("data.csv")









