import os
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import random
from bs4 import BeautifulSoup 
import pandas as pd
import numpy as np
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

file = 'movielens_2025-11-19-1151.csv'



# Setting up session for persistent connection, instead of creating new connection for every request
# Help improve speed
def create_session():
    session = requests.Session()

    # Mimic a real browser 
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }

    session.headers.update(headers)

    # Create a retry strategy 
    retry_strategy = Retry (
        total=3, # Number of retries
        backoff_factor=1, # Wait 1s, 2s, 4s between retries
        status_forcelist=[429, 500, 502, 503, 504], # Error code to watchout for for retry
        allowed_methods=['HEAD', 'GET', 'OPTIONS']
    )

    # Create an adapter that requests library will use to read the strategy above
    adapter = HTTPAdapter(max_retries=retry_strategy)

    # Mount the strategy to different link types
    session.mount(prefix='https://', adapter=adapter)
    session.mount(prefix='http://', adapter=adapter)
    return session



# Creating session for global use
session = create_session()



def currency_cleaner(text):
    try:
        # print(text)
        if text in {'Budget -', 'Revenue -'}:
            return np.nan
        else:
            cleaned_value = text.replace('Budget ', '').replace('Revenue ', '').replace('$', '').replace(',', '').strip()
            return float(cleaned_value)
    
    except Exception as e:
        print(f'Error cleaning currency data: {e}')
        return np.nan



def lang_cleaner(text):
    try: 
        original_language = text.replace('Original Language ', '').strip()

    except Exception as e:
        print(f'Error cleaning language: {e}')
        original_language = 'Not Available'

    finally:
        return original_language



def data_scraper(row_tuple):
    try:
        # Defining variable
        link = row_tuple[5]
        scraped_data_dict = {
            'original_index': row_tuple[0],
            'img_link': '',
            'budget_value': 0.0,
            'revenue_value': 0.0,
            'lang': ''
        }

        # --- Setting up the request ---
        
        #print(f'Link to scrape: {link}')

        # Using random sleep period to mimic human use
        time.sleep(random.uniform(0.5, 1.5)) 

       
        response = session.get(link, timeout=10)
        response.raise_for_status()

        soup = BeautifulSoup(response.content, 'html.parser')

        # Scraping poster image link and loading to list
        try: # Covering Nonetype cases
            poster_image_link = soup.find('img', class_='poster w-full')['src']
        except Exception as e:
            print(f'Error scraping poster image: {e}')
            poster_image_link = 'Not Available'

    
        video_data = soup.select_one('section.facts.left_column').find_all('p')

        if len(video_data) == 4:

            budget_USD = currency_cleaner(video_data[2].text)
            revenue_USD = currency_cleaner(video_data[3].text)
            original_language = lang_cleaner(video_data[1].text)

        else:
            budget_USD = currency_cleaner(video_data[3].text)
            revenue_USD = currency_cleaner(video_data[4].text)
            original_language = lang_cleaner(video_data[2].text)

       # Appending to dictionary
        scraped_data_dict['img_link'] = poster_image_link
        scraped_data_dict['budget_value'] = budget_USD
        scraped_data_dict['revenue_value'] = revenue_USD
        scraped_data_dict['lang'] = original_language
    
    except Exception as e:
        print(f'Error Scraping the link {link}: {e}')
    
    finally:
        return scraped_data_dict

           

def main(filepath):
    try:
        output_file = 'dim_movielens_enriched.csv'
        # Loading the csv data
        print('Loading data from csv...')
        df = pd.read_csv(filepath, dtype=object)

        # Iterating through the dataframe
        tuple_list = [row for row in df.itertuples(index=True, name=None)]

        # Defining some variable
        total_links = len(tuple_list)
        print(f'Processing {total_links} links')

        results = []
        batch_size = 1000

        with ThreadPoolExecutor(max_workers=10) as executor:

            # results = list(executor.map(data_scraper, tuple_list))
            # submit will be used faster approach
            # Map will follow order while submit doesn't
            # submit will process and deliver faster job without respect the data order

            scraping_ticket = {executor.submit(data_scraper, row): row for row in tuple_list} # List comprehension

            # Geting the scrape result for the scraped link
            for i, completed_ticket in enumerate(as_completed(scraping_ticket)):
                link = scraping_ticket[completed_ticket][5]
                print(f'Scraped Link: {link}')
                scraped_data = completed_ticket.result() # Return dict of the scraped data

                if scraped_data:
                    results.append(scraped_data)

                if (i + 1) % batch_size == 0 or (i + 1) == total_links:
                    print(f'Saving batch... ({i + 1} of {total_links})')
                    temp_df = pd.DataFrame(results)
                    temp_df.to_csv('temp_data.csv', index=False)

        print('Scraping complete. Finalizing merge...')
        # Create dataframe for the result
        result_df = pd.DataFrame(results)
        result_df.set_index('original_index', inplace=True)

        # Merge with the first df using the index
        df_final = df.join(result_df)

        # Renaming column Names
        df_final.rename(columns={
            'img_link': 'POSTER_IMAGE_LINK',
            'budget_value': 'BUDGET_USD',
            'revenue_value': 'REVENUE_USD',
            'lang': 'ORIGINAL_LANGUAGE'
        }, inplace=True)

    except Exception as e:
        print(f'Error scraping data: {e}')

    finally:
        del result_df
        del df
        print(f'Done! Saved to {output_file}')
        return df_final.to_csv(output_file, index=False)

#main(file)