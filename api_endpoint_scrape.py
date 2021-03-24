# -*- coding: utf-8 -*-
"""
Created on Wed Mar 24 16:37:07 2021

@author: Lenovo
"""

import traceback
import time
import requests
import pandas as pd
from requests_module import Request


READ_FILE = 'Ecomm Data.csv'

df = pd.read_csv(READ_FILE)


def get_similar_columns(df, json):
#     json = requests.get(url).json()
    found = list()
    for col in df.columns:
        if col in json['brand']:
            found.append(col)

    return found


def get_data(json):
    D = dict()
    similar_cols = get_similar_columns(df, json)
    for col in similar_cols:
        D[col] = json['brand'][col]
#     D['average_rating'] = json['
    D['average_rating'] = json['brand']['brand_reviews_summary']['average_rating']
    D['number_of_reviews'] = json['brand']['brand_reviews_summary']['number_of_reviews']
    D['related_search_terms'] = '; '.join(json['related_search_terms'])
    try:
        url = json['brand']['url']
        shopify_url = url + 'products.json'
        print('Getting shopify url: ', shopify_url)
        resp = Request.get(shopify_url)
        print('Got shopify url')
        json2 = resp.json()
        if 'products' in json2.keys():
            D['is_shopify'] = 'true'
        else:
            D['is_shopify'] = 'false'
        
    except:
        traceback.print_exc()
        D['is_shopify'] = 'false'
    finally:
        print('Value of shopify_url is: ', D['is_shopify'])
    
    return D


def clean_dict(d):
    pass
    for key in d.keys():
        if type(d[key]) == list:
            d[key] = ', '.join(d[key])
    
    if 'minimum_order_amount' in d.keys() and type(d['minimum_order_amount']) == dict:
        d['minimum_order_amount'] = str(d['minimum_order_amount']['amount_cents']) + ' ' + str(d['minimum_order_amount']['currency'])
    return d


def main(start_index = None, end_index = None, ):
    try:
        df = pd.read_csv(READ_FILE)    
    except:
        df = pd.read_excel(READ_FILE)
    if start_index == None:
        start_index = 0
        
    if end_index == None:
        end_index = len(df) - 1
    
    
    for i in range(start_index, end_index + 1):
        print('--------------------------')
        print('Scraping: ', i)
        try:
            row = df.iloc[i]
            url = row['urls']
            print('Making request')
            resp = Request.get(url)
            print('Request made')
            print('Getting json')
            json = resp.json()
            print('Got json')
            unclean_json = get_data(json)
            clean_json = clean_dict(unclean_json)
            print('Cleaned JSON')
            for key in clean_json.keys():
                if key in df.columns:
                    df.loc[i, key] = clean_json[key]
            print('Added keys to CSV')
        
        
        except:
            print('Problem in Main')
            print(row['urls'])
            traceback.print_exc()
            
        finally:
            print('Sleeping')
            time.sleep(.2)
            
            
        if i % 10 ==0 and i != 0:
            try:
                df.to_csv(READ_FILE, index = False)
                print('Saving to CSV')
                    
            except:
                traceback.print_exc()
                
            
            finally:
                print(i, 'Rows Scraped.')
        
    df.to_csv(READ_FILE, index = False)
                    
    
    
def main_shopify(start_index = None, end_index = None, ):
    try:
        df = pd.read_csv(READ_FILE)    
    except:
        df = pd.read_excel(READ_FILE)
    if start_index == None:
        start_index = 0
        
    if end_index == None:
        end_index = len(df) - 1
    
    
    for i in range(start_index, end_index + 1):
        print('--------------------------')
        print('Scraping: ', i)
        try:
            row = df.iloc[i]
            shopify_url = row['url'] + 'products.json'
            print('Shopify URL:', shopify_url)
            resp = Request.get(shopify_url, timeout = 10)
            print('Got shopify_url')
            try:
                t = resp.json()
                if 'products' in t.keys():
                    is_shopify = True
                else:
                    is_shopify = False
            except:
                is_shopify = False
                traceback.print_exc()
            
            df.loc[i, 'is_shopify'] = is_shopify
            print('Added keys to CSV, value of shopify:', is_shopify)
        
        
        except:
            print('Problem in Main')
            traceback.print_exc()
            
        finally:
            pass
            
            
            
        if i % 10 ==0 and i != 0:
            try:
                df.to_csv(READ_FILE, index = False)
                print('Saving to CSV')
                    
            except:
                traceback.print_exc()
                
            
            finally:
                print(i, 'Rows Scraped.')
        
    df.to_csv(READ_FILE, index = False)
                    