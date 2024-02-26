""" Data Acquisition classes. """
import requests
import pandas as pd
import numpy as np 
from itertools import product
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from scripts.data.api_access import call_access_token

class DataAcquisition:
    """
    This class is responsible for acquiring data from MercadoLibre API, its the base for items, 
    sellers and ratings.

    Args:
        path_data (Path): The location where the extracted data will be saved.
        country_site (str, optional): The country identifier to extract data from. Defaults to 'MCO'.
    """

    def __init__(self, path_data: Path, country_site: str="MCO") -> None:
        """
        Initializes a new instance of the DataAcquisition class.

        Args:
            path_data (Path): The location where the extracted data will be saved.
            country_site (str, optional): The country identifier to extract data from. Defaults to 'MCO'.
        """
        self.path_data = path_data
        self.country_site = country_site
        self.cats = self.get_categories()
        self.access_token = call_access_token()

    def get_categories(self) -> pd.DataFrame:
        """
        Retrieves the categories from the MercadoLibre API.

        Returns:
            pandas.DataFrame: The categories data.
        """
        cats = requests.get(f'https://api.mercadolibre.com/sites/{self.country_site}/categories')
        cats = pd.DataFrame(cats.json())
        cats = cats.sort_values(by="id").reset_index(drop=True)
        return cats

    @staticmethod
    def save_data(data: pd.DataFrame, path: Path, preffix: str, filename: str="") -> None:
        """
        Saves the data to a CSV file.

        Args:
            data (pandas.DataFrame): The data to be saved.
            path (Path): The location where the data will be saved.
            preffix (str): The prefix for the filename.
            filename (str, optional): The filename after the preffix. Defaults to "".
        """
        data.to_csv(path / f'{preffix}_{filename}.csv', index=False)

    @staticmethod
    def compile_data(preffix: str, path: Path) -> pd.DataFrame:
        """
        Compiles the data from multiple CSV files into a single DataFrame.

        Args:
            preffix (str): The prefix of the CSV files to be compiled.
            path (Path): The location of the CSV files.

        Returns:
            pandas.DataFrame: The compiled data.
        """
        files = list(path.glob(f'{preffix}*.csv'))
        data = pd.concat([pd.read_csv(file) for file in files], ignore_index=True)
        return data
    
class ItemDataAcquisition(DataAcquisition):
    """
    This class represents the data acquisition for items.
    It inherits from the DataAcquisition class.
    """

    def __init__(self, path_data, country_site="MCO") -> None:
        """
        Initializes an instance of the ItemDataAcquisition class.

        Parameters:
        - path_data (str): The path to the data.
        - country_site (str): The country site. Default is "MCO".
        """
        super().__init__(path_data, country_site)
    
    @staticmethod
    def get_only_one_attr(row: pd.Series, attribute: str) -> None:
        """
        Retrieves the attribute from a given row.

        Parameters:
        - row (pd.Series): The row containing attributes.
        - attribute (str): The attribute to retrieve.

        Returns:
        - dict: The attribute dictionary if found, None otherwise.
        """
        for i in range(len(row)):
            if row[i]["id"] == attribute:
                return row[i]
        return None
    
    def items_by_category(self, category: str, offset: int) -> pd.DataFrame:
        """
        Retrieves items by category and offset.
        Max offset for public API: 1000
        Max offset for registered APP in API: 4000
        
        See: https://developers.mercadolibre.com.ar/es_ar/items-y-busquedas

        Parameters:
        - category (str): The category of the items.
        - offset (int): The offset for pagination.

        Returns:
        - pandas.DataFrame: The items data.
        """
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
    
    def get_all_items_by_cats(self, init_offset: int, final_offset: int) -> None:
        """
        Retrieves all items by categories within the specified offset range.

        Parameters:
        - init_offset (int): The initial offset.
        - final_offset (int): The final offset.
        """
        offset_range = np.arange(init_offset, final_offset+50, 50)
        for category, offset in product(self.cats['id'], offset_range):
            cat_data = self.items_by_category(category, offset)
            if cat_data.shape[0] == 0:
                continue
            # Saving extracted data
            self.save_data(cat_data, self.path_data, "data_items", f"{category}_{offset}")
    
    def explode_data(self, df_items: pd.DataFrame) -> pd.DataFrame:
        """
        Explodes the data in the specified DataFrame.

        Parameters:
        - df_items (pandas.DataFrame): The DataFrame containing the items data.

        Returns:
        - pandas.DataFrame: The exploded data.
        """
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
    def clean_data_cols(df: pd.DataFrame) -> pd.DataFrame:
        """
        Cleans the specified DataFrame columns.

        Parameters:
        - df (pandas.DataFrame): The DataFrame to clean.

        Returns:
        - pandas.DataFrame: The cleaned DataFrame.
        """
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
    """
    This class represents the data acquisition for sellers.
    It inherits from the DataAcquisition class.
    """

    def __init__(self, path_data, country_site="MCO", MAX_THREADS=6) -> None:
        """
        Initializes a new instance of the SellerDataAcquisition class.

        Parameters:
        - path_data (str): The path to the data.
        - country_site (str): The country site. Default is "MCO".
        - MAX_THREADS (int): The maximum number of threads. Default is 6.
        """
        super().__init__(path_data, country_site)
        self.MAX_THREADS = MAX_THREADS

    def get_seller(self, seller_id: str) -> pd.DataFrame:
        """
        Retrieves data for a specific seller.

        Parameters:
        - seller_id (str): The ID of the seller.

        Returns:
        - final_data (pd.DataFrame): The seller data.
        """
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

    def get_seller_reputation_data(self, rep_data: dict) -> pd.DataFrame:
        """
        Retrieves the reputation data for a seller.

        Parameters:
        - rep_data (dict): The reputation data.

        Returns:
        - final_data (pd.DataFrame): The reputation data as a DataFrame.
        """
        # Get values from transactions
        rep_data['transactions_period'] = rep_data['transactions']['period']
        rep_data['transactions_total'] = rep_data['transactions']['total']

        # Delete data about transactions
        del rep_data['transactions']

        # Create dataframe from dictionary
        final_data = pd.DataFrame([rep_data])

        return final_data

    def get_all_sellers(self, sellers_id: list)-> pd.DataFrame:
        """
        Retrieves data for multiple sellers.

        Parameters:
        - sellers_id (list): The list of seller IDs.

        Returns:
        - sellers_data (pd.DataFrame): The data for all sellers.
        """
        sellers_data = pd.DataFrame()
        with ThreadPoolExecutor(max_workers=self.MAX_THREADS) as executor:
            sellers_data = executor.map(self.get_seller, sellers_id)
        sellers_data = pd.concat(list(sellers_data))

        self.save_data(sellers_data, self.path_data, "seller_data")

        return sellers_data
        
class RatingsDataAcquisition(DataAcquisition):
    """
    This class represents the data acquisition for ratings data.
    It inherits from the DataAcquisition class.
    """

    def __init__(self, path_data, country_site="MCO", MAX_THREADS=6) -> None:
        """
        Initializes a RatingsDataAcquisition object.

        Parameters:
        - path_data (str): The path to the data.
        - country_site (str): The country site (default is "MCO").
        - MAX_THREADS (int): The maximum number of threads to use (default is 6).
        """
        super().__init__(path_data, country_site)
        self.MAX_THREADS = MAX_THREADS

    def get_all_ratings(self, items_id: list) -> pd.DataFrame:
        """
        Retrieves all ratings data for the given list of item IDs.

        Parameters:
        - items_id (list): The list of item IDs.

        Returns:
        - ratings_data (DataFrame): The ratings data as a pandas DataFrame.
        """
        ratings_data = pd.DataFrame()
        with ThreadPoolExecutor(max_workers=self.MAX_THREADS) as executor:
            ratings_data = executor.map(self.get_rating, items_id)
        ratings_data = pd.concat(list(ratings_data))
        self.save_data(ratings_data, self.path_data, "ratings_data")

        return ratings_data

    def get_rating(self, item_id: str) -> pd.DataFrame:
        """
        Retrieves the rating data for a specific item ID.

        Parameters:
        - item_id (str): The item ID.

        Returns:
        - final_data (DataFrame): The rating data as a pandas DataFrame.
        """
        try:
            url = f'https://api.mercadolibre.com/reviews/item/{item_id}'
            headers = {
                'Authorization': f'Bearer {self.access_token}'
            }
            payload = {}
            rating_data = requests.request("GET", url, headers=headers, data=payload)
            rating_data = rating_data.json()
            final_data = pd.DataFrame([rating_data["rating_levels"]])
            final_data["total_reviews"] = final_data.sum(axis=1)
            final_data["rating_average"] = rating_data["rating_average"]

            final_data["id"] = item_id
            self.save_data(final_data, self.path_data, "ratings_data", item_id)

            return final_data
        except Exception as e:
            print('Request failed due to error:', e)
            print("|" + item_id + "|")
            return pd.DataFrame()
