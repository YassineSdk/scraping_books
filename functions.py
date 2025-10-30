import pandas as pd 
import numpy as np 
from playwright.sync_api import sync_playwright
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from tqdm import tqdm
import csv 



def log_summary(name,total_links,errors,start,end):

    duration = (end - start ).total_seconds()
    success = total_links - errors

    with open("scrape_summary.csv","a",newline="",encoding="utf-8") as f :
        writer = csv.writer(f)
        if not f:
            writer.writerow(["Job Name", "Total Links", "Success", "Errors", "Start Time", "End Time", "Duration (s)"])
        writer.writerow([name, total_links, success, errors, start, end, duration])
        f.flush() 


# getting the links 
def get_links(start, end ):
    """
    this function fetches the links of each listing for each page ,
    this  links will serve the next fucntion that will perform a deep scraping to each listing 
    """
    links = []
    with sync_playwright() as pw:
        browzer = pw.firefox.launch(
        headless=True,
        args=[
                "-headless",
                "--disable-gpu",
                "--no-remote",
                "--private",  # run in private mode
                "--disable-default-apps",
                "--disable-extensions",
            ]
    )
        page = browzer.new_page()

        # getting the links of the offer for each page

        for p in range(start,end+1):
            selector = "a.box-border.flex.flex-col.px-3\\.5.py-2.text-primary-text.hover\\:no-underline.h-full"
            page.goto(f"https://www.forrent.com/find/NY/metro-NYC/New+York/extras-Rentals/page-{p}",wait_until="domcontentloaded")
            page.wait_for_selector(selector)
            offers = page.locator(selector)
            num_offer = offers.count()

            ### looping over the links and storing them

            for i in range(0,num_offer):
                simple_link = offers.nth(i).get_attribute("href")
                links.append(''.join(["https://www.forrent.com",simple_link]))

        browzer.close()
        return links
################################################################
def getting_meta_data(start, end,name,links,idx):
    """
    this function takes the links of each listing provided by the previews function and performe
    a deep scraping of the listing informations (price ,address , number of baths , rooms ......... )
    """
    metadata = []
    errors = 0
    with sync_playwright() as pw:
        browzer  = pw.firefox.launch(headless=True)
        page = browzer.new_page()
        for link in tqdm(links[:5],position=idx):
            try:
                page.goto(link)
                page.wait_for_timeout(1000)
                try :
                    price = page.locator("span.text-heading.font-semibold.text-4xl").text_content()
                except :
                    price = np.nan
                try:
                    address =  page.locator("h1.address.text-base.font-normal").text_content()
                except :
                    address = np.nan
                try:
                    desc = page.locator("#propertyDetailsSection > div:nth-child(2) > fr-read-more-text").text_content()
                except :
                    desc = np.nan
                    
                try:
                    page.wait_for_selector("#propertyDetailsSection fr-floor-plan-summary ul li", timeout=10000)
                    li_texts = page.locator("#propertyDetailsSection fr-floor-plan-summary ul li").all_text_contents()
                    li_texts = [t.strip() for t in li_texts if t.strip()]

                    details = {
                    "price": next((t for t in li_texts if "$" in t), np.nan),
                    "rooms": next((t for t in li_texts if "Bed" in t), np.nan),
                    "baths": next((t for t in li_texts if "Bath" in t), np.nan),
                    "surface": next((t for t in li_texts if "sq." in t), np.nan),
                }

                except :
                    details = {"rooms": np.nan, "baths": np.nan, "surface": np.nan}

                metadata.append(
                            {
                            "price":price,
                            "address":address,
                            "desc":desc,
                            **details   }
                            
                        )

            except :
                errors += 1

    df = pd.DataFrame(metadata)
    file_name = f'housing_data{name}.csv'
    df.to_csv(file_name)
    return file_name , errors , len(links)


