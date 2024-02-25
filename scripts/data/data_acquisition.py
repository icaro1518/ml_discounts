import requests
import pandas as pd
import numpy as np 
from itertools import product
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from scripts.data.api_access import call_access_token

class DataAcquisition:

    def __init__(self, path_data, country_site = "MCO") -> None:
        """
        NOTA: Máximo offset permitido por la API de MercadoLibre es 1000

        Args:
            path_data (Path): Ubicación donde se guardará la data extraída.
            country_site (str, optional): Identificador del país para extraer data. Defaults to 'MCO'.
        """
        self.path_data = path_data
        self.country_site = country_site
        self.cats = self.get_categories()
        self.access_token = call_access_token()
        
    def get_categories(self):
        cats = requests.get(f'https://api.mercadolibre.com/sites/{self.country_site}/categories')
        cats = pd.DataFrame(cats.json())
        cats = cats.sort_values(by = "id").reset_index(drop = True)
        return cats
    
    @staticmethod
    def save_data(data, path, preffix, filename = ""):
        data.to_csv(path / f'{preffix}_{filename}.csv', index=False)
    
    @staticmethod
    def compile_data(preffix, path):
        files = list(path.glob(f'{preffix}*.csv'))
        data = pd.concat([pd.read_csv(file) for file in files], ignore_index=True)
        return data
    
class ItemDataAcquisition(DataAcquisition):
    
    def __init__(self, path_data, country_site = "MCO") -> None:
        super().__init__(path_data, country_site)
    
    @staticmethod
    def get_only_one_attr(row, attribute):
        for i in range(len(row)):
            if row[i]["id"] == attribute:
                return row[i]
        return None  
    
    def items_by_category(self, category, offset):
        
        url = f'https://api.mercadolibre.com/sites/{self.country_site}/search?category={category}&offset={offset}'       
        headers = {
            'Authorization': f'Bearer {self.access_token}'
            }
        payload = {}
        items = requests.request("GET", url, headers=headers, data=payload)
        items = pd.DataFrame(items.json()['results'])
        items['category'] = category
        items = items.dropna(axis=1, how='all')
        
        if items.shape[0] == 0:
            return items
                
        # Keep only brand attributes
        if 'attributes' in items.columns:
            items["brand"] = items["attributes"].apply(lambda x: self.get_only_one_attr(x, "BRAND"))
        # Generate new cols for item condition and brand
        extra_cols = self.explode_data(items)
        items = pd.concat([items, extra_cols], axis = 1)
        items = self.clean_data_cols(items)
        
        return items
    
    def get_all_items_by_cats(self, init_offset, final_offset):
        
        offset_range = np.arange(init_offset, final_offset+50, 50)
        for category, offset in product(self.cats['id'], offset_range):
            cat_data = self.items_by_category(category, offset)
            if cat_data.shape[0] == 0:
                continue
            # Saving extracted data
            self.save_data(cat_data, self.path_data, "data_items", f"{category}_{offset}")
    
    def explode_data(self, df_items):
        cols_explode = ['shipping', 'seller', 'installments', 'brand']
        df_explode_final = pd.DataFrame()
        for col in cols_explode:
            if col not in df_items.columns:
                continue
            df_explode = df_items[col].apply(pd.Series)
            df_explode.columns = [col + "_" + str(new_col) for new_col in df_explode.columns]
            df_explode_final = pd.concat([df_explode_final, df_explode], axis = 1)
        return df_explode_final
    
    @staticmethod
    def clean_data_cols(df):
        cols_delete = ['thumbnail_id', 'thumbnail', 'currency_id', 'order_backend',
                       'use_thumbnail_id', 'attributes', 'installments', 
                       'differential_pricing', 'inventory_id', 'variation_filters',
                       'variations_data', "shipping", "seller", "brand_id",
                       "brand_name", "brand_value_id", "brand_attribute_group_id",
                       "brand_attribute_group_name", "brand_value_struct", "brand_values",
                       "brand_source", "brand_value_type", "brand"]
        existing_columns = [col for col in df.columns if col in cols_delete]
        return df.drop(columns = existing_columns)

class SellerDataAcquisition(DataAcquisition):
        
        def __init__(self, path_data, country_site = "MCO", MAX_THREADS = 6) -> None:
            super().__init__(path_data, country_site)
            self.MAX_THREADS = MAX_THREADS
                               
        def get_seller(self, seller_id: str):
            url = f'https://api.mercadolibre.com/users/{seller_id}'       
            headers = {
                'Authorization': f'Bearer {self.access_token}'
                }
            payload = {}
            seller_data = requests.request("GET", url, headers=headers, data=payload)
            seller_data = seller_data.json()
            
            usertype_data = seller_data["user_type"]
            final_data = self.get_seller_reputation_data(seller_data["seller_reputation"])
            
            final_data["seller_id"] = seller_id
            final_data["user_type"] = usertype_data
            
            return final_data
        
        def get_seller_reputation_data(self, rep_data):
        
            # Get values from transactions
            rep_data['transactions_period'] = rep_data['transactions']['period']
            rep_data['transactions_total'] = rep_data['transactions']['total']

            # Delete data about transactions
            del rep_data['transactions']

            # Create dataframe from dictionary
            final_data = pd.DataFrame([rep_data])
            
            return final_data
        
        def get_all_sellers(self, sellers_id: list):
        
            sellers_data = pd.DataFrame()
            with ThreadPoolExecutor(max_workers=self.MAX_THREADS) as executor:
                sellers_data = executor.map(self.get_seller, sellers_id)
            sellers_data = pd.concat(list(sellers_data))
            
            self.save_data(sellers_data, self.path_data, "seller_data")
            
            return sellers_data
        
class RatingsDataAcquisition(DataAcquisition):
        
        def __init__(self, path_data, country_site = "MCO", MAX_THREADS = 6) -> None:
            super().__init__(path_data, country_site)
            self.MAX_THREADS = MAX_THREADS
        
        def get_all_ratings(self, items_id: list):
            
            ratings_data = pd.DataFrame()            
            with ThreadPoolExecutor(max_workers=self.MAX_THREADS) as executor:
                ratings_data = executor.map(self.get_rating, items_id)
            ratings_data = pd.concat(list(ratings_data))
            self.save_data(ratings_data, self.path_data, "ratings_data")
            
            return ratings_data
                            
        def get_rating(self, item_id: str):
            try:
                url = f'https://api.mercadolibre.com/reviews/item/{item_id}'       
                headers = {
                    'Authorization': f'Bearer {self.access_token}'
                    }
                payload = {}
                rating_data = requests.request("GET", url, headers=headers, data=payload)
                rating_data = rating_data.json()
                final_data = pd.DataFrame([rating_data["rating_levels"]])
                final_data["total_reviews"] = final_data.sum(axis = 1)
                final_data["rating_average"] = rating_data["rating_average"]
                
                final_data["id"] = item_id
                self.save_data(final_data, self.path_data, "ratings_data", item_id)
                
                return final_data
            except Exception as e:
                print('Request failed due to error:', e)
                print("|"+item_id+"|")
                return pd.DataFrame()
            
            # url = f'https://api.mercadolibre.com/reviews/item/{item_id}'       
            # headers = {
            #     'Authorization': f'Bearer {self.access_token}'
            #     }
            # payload = {}
            # rating_data = requests.request("GET", url, headers=headers, data=payload)
            # rating_data = rating_data.json()
            # final_data = pd.DataFrame([rating_data["rating_levels"]])
            # final_data["total_reviews"] = final_data.sum(axis = 1)
            # final_data["rating_average"] = rating_data["rating_average"]
            
            # final_data["id"] = item_id
            
            # return final_data