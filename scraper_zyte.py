import asyncio
from zyte_api import AsyncZyteAPI, RequestError
from bs4 import BeautifulSoup
import os  
import base64
import pandas as pd
from scraper_threading import currency_cleaner, lang_cleaner
import re

output_file = 'dim_movielens_enriched.csv'
filepath = 'movielens_2025-11-19-1151.csv'
api_key = os.environ.get('ZYTE_API_KEY')



def get_movie_id(url):

    # Covering null links
    if not isinstance(url, str):
        return None
    
    match = re.search(r'movie/(\d+)', url)
    return match.group(1) if match else None


def mapping_dict(tuple_list):
    dict_map = {}
    for row in tuple_list:
        link = row[5]
        index = row[0]
        movie_id = get_movie_id(link)

        if movie_id:
            dict_map[movie_id] = index

    return dict_map


# Define function that takes in the encoded html and return dictionary of extracted data
def scraper(encoded_html, url, index):

    scraped_data_dict = {
    'original_index': index,
    'img_link': '',
    'budget_value': 0.0,
    'revenue_value': 0.0,
    'lang': ''
    }

    if encoded_html:
        html_bytes = base64.b64decode(encoded_html) # Convert to bytes
        html_body = html_bytes.decode('utf-8') # Convert to string
        soup = BeautifulSoup(html_body, 'html.parser')

        print(f'--- Getting data from: {url} ---')   
        poster_image_link = soup.find('img', class_='poster w-full')['src']  
    
        video_data = soup.select_one('section.facts.left_column').find_all('p')

        if len(video_data) == 4:

            budget_USD = currency_cleaner(video_data[2].text)
            revenue_USD = currency_cleaner(video_data[3].text)
            original_language = lang_cleaner(video_data[1].text)

        else:
            budget_USD = currency_cleaner(video_data[3].text)
            revenue_USD = currency_cleaner(video_data[4].text)
            original_language = lang_cleaner(video_data[2].text)

        scraped_data_dict['img_link'] = poster_image_link
        scraped_data_dict['budget_value'] = budget_USD
        scraped_data_dict['revenue_value'] = revenue_USD
        scraped_data_dict['lang'] = original_language
    
    return scraped_data_dict
    

async def main(filepath):

    client = AsyncZyteAPI(api_key=api_key)
    df = pd.read_csv(filepath, dtype=object)
    tuple_list = [row for row in df.itertuples(index=True, name=None)]
    total_links = len(tuple_list)

    data_accumulator = []
    id_mapping = mapping_dict(tuple_list)

    try:
        async with client.session() as session:
            queries = [
                {"url": item[5].strip(), 
                "httpResponseBody": True}
                for item in tuple_list 
                if isinstance(item[5], str) and item[5].strip() 
            ]

            for i, future in enumerate(session.iter(queries), start=1):
                try:
                    result = await future
                    encoded_html_body = result.get('httpResponseBody')
                    url = result.get('url')

                    # Getting the original index
                    index = id_mapping.get(get_movie_id(url))

                    if index is None:
                        print(f"Warning: Could not find index for URL {url}")
                        continue

                    # Decoding and Scraping the html
                    data = scraper(encoded_html_body, url, index)

                    # Appending the dict of scraped data
                    data_accumulator.append(data)

                    # Saving to Temporary file
                    if i % 1000 == 0 or i == total_links:
                        print(f'Saving batch... ({i} of {total_links})')
                        temp_df = pd.DataFrame(data_accumulator)   
                        temp_df.to_csv('temp_data.csv', index=False)

                except RequestError as e:
                    print(f"Request Error for a URL: {e}")
                    
                except Exception as e:
                    print(f"An unexpected error occurred: {e}")
            
            print('Scraping complete. Finalizing merge...')

            # Create dataframe for the result
            new_df = pd.DataFrame(data_accumulator)
            new_df.set_index('original_index', inplace=True)

            # Merge with the first df using the index
            final_df = df.join(new_df, how='left')

            # Saving dataframe to output file
            final_df.to_csv(output_file, index=False)

            # --- FILL MISSING VALUES FOR SKIPPED ROWS ---
            final_df['budget_value'] = final_df['budget_value'].fillna(0.0)
            final_df['revenue_value'] = final_df['revenue_value'].fillna(0.0)
            final_df['lang'] = final_df['lang'].fillna('Not Available')
            final_df['img_link'] = final_df['img_link'].fillna('')

            final_df.rename(columns={
                'img_link': 'POSTER_IMAGE_LINK',
                'budget_value': 'BUDGET_USD',
                'revenue_value': 'REVENUE_USD',
                'lang': 'ORIGINAL_LANGUAGE'
                }, inplace=True)

            print(f'Complete! Results saved to {output_file}')

    except Exception as e:
        print(f'Error Scraping: {e}') 

    finally:  
        # Clean up
        if 'new_df' in locals(): del new_df
        if 'df' in locals(): del df
        if 'temp_df' in locals(): del temp_df
        if 'final_df' in locals(): del final_df

if __name__ == "__main__":
    asyncio.run(main(filepath))