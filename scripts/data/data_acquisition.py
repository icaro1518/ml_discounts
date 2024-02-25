import requests
import pandas as pd
import numpy as np 
from itertools import product
from pathlib import Path
from scripts.data.api_access import call_access_token

class DataAcquisition:
    
    def __init__(self, path_data, country_site='MCO'):
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
    
    def get_all_items_cats(self, init_offset, final_offset):
        
        offset_range = np.arange(init_offset, final_offset+50, 50)
        for category, offset in product(self.cats['id'], offset_range):
            cat_data = self.items_by_category(category, offset)
            if cat_data.shape[0] == 0:
                continue
            # Saving extracted data
            self.save_items(self.path_data, category, offset, cat_data)
    
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
    def save_items(path, category, offset, items):
        items.to_csv(path / f'data_items_{category}_{offset}.csv', index=False)
        
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
    
    @staticmethod
    def compile_data(path):
        files = list(path.glob('*.csv'))
        data = pd.concat([pd.read_csv(file) for file in files], ignore_index=True)
        return data
    
