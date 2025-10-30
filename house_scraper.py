import pandas as pd 
import numpy as np 
import numpy as np
from playwright.sync_api import sync_playwright
import time
from tqdm import tqdm
import time
from concurrent.futures import ThreadPoolExecutor


# getting the links 
def get_links(start, end ):
    """
    this function fetches the links of each listing for each page ,
    this  links will serve the next fucntion that will perform a deep scraping to each listing 
    """
    links = []
    with sync_playwright() as pw:
        browzer = pw.firefox.launch(
        headless=True
    )
        page = browzer.new_page()

        # getting the links of the offer for each page

        for p in range(start,end+1):
            selector = "a.box-border.flex.flex-col.px-3\\.5.py-2.text-primary-text.hover\\:no-underline.h-full"
            page.goto(f"https://www.forrent.com/find/NY/metro-NYC/New+York/extras-Rentals/page-{p}")
            page.wait_for_selector(selector)
            offers = page.locator(selector)
            num_offer = offers.count()
            print(f"number of offers in page {p} ",num_offer)

            ### looping over the links and storing them

            for i in range(0,num_offer):
                links.append(offers.nth(i).get_attribute("href"))

        browzer.close()
        return links
################################################################

def getting_meta_data(start, end):
    """
    this function takes the links of each listing provided by the previews function and performe
    a deep scraping of the listing informations (price ,address , number of baths , rooms ......... )
    """
    metadata = []
    errors = 0

    links = get_links(start,end)
    with sync_playwright() as pw:
        browzer  = pw.firefox.launch(headless=True)


        page = browzer.new_page()
        for link in tqdm(links,desc=f"scraping listings..."):
            try:
                page.goto(''.join(["https://www.forrent.com",link]))
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
                    unit_box = page.locator("div.box-border.border").first
                    li_texts = unit_box.locator("ul li").all_text_contents()

                    details = {
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
                        
            except Exception as e :
                print(f"having error scaping link {link}",e)
                errors += 1

    df = pd.DataFrame(metadata)
    file_name = f'housing_data{start}_{end}.csv'
    print(f"""
        ✅ Scraping finished:
        Pages scraped: {num_pages}
        Total links: {len(links)}
        Errors: {errors}
        Success: {len(links) - errors}
        """)
    df.to_csv(file_name)

    return file_name

# deviding workload into chunks
jobs = [
    (1,2),
    (3,5),
    (5,7)
]

with ThreadPoolExecutor(max_workers=3) as executer:
    futures = [executer.submit(getting_meta_data,start,end) for start , end in jobs ]
    for f in futures:
        print(f"finished job : {f.result()}")



