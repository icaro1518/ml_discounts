""" PreprocessingData class. """
import pandas as pd
from typing import Optional
class PreprocessingDataUtils:
    """ Preprocess the data before training the model. """
    
    @staticmethod
    def fill_null_values_by_value(data: pd.DataFrame, value_dict: Optional[dict]) -> pd.DataFrame:
        """ Fill null values with a specific value.

        Args:
            data (pd.DataFrame): Data frame to fill the null values.
            value_dict (dict, optional): Values to fill for each column. Defaults to 0.

        Returns:
            pd.DataFrame: Data frame with the null values filled.
        """
        if value_dict is None:
            print("Value to fill the null values is not defined.")
            return data
        data = data.fillna(value_dict)
        return data
    
    @staticmethod
    def fill_null_values_by_mean(data: pd.DataFrame, columns: list) -> pd.DataFrame:
        """ Fill null values with the mean of the column.

        Args:
            data (pd.DataFrame): Data frame to fill the null values.
            columns (list): Columns to fill the null values.

        Returns:
            pd.DataFrame: Data frame with the null values filled.
        """
        data[columns] = data[columns].fillna(data[columns].mean())
        return data
    
    @staticmethod
    def fill_null_values_by_median(data: pd.DataFrame, columns: list) -> pd.DataFrame:
        """ Fill null values with the median of the column.

        Args:
            data (pd.DataFrame): Data frame to fill the null values.
            columns (list): Columns to fill the null values.

        Returns:
            pd.DataFrame: Data frame with the null values filled.
        """
        data[columns] = data[columns].fillna(data[columns].median())
        return data
    
    @staticmethod
    def detect_outliers(data: pd.DataFrame) -> pd.DataFrame:
        """ Detect outliers in the data.

        Args:
            data (pd.DataFrame): Data frame to detect the outliers.

        Returns:
            pd.DataFrame: Data frame with the outliers detected.
        """
        columns = data.select_dtypes(include='number').columns
        # detect outliers by the boxplot rule
        outliers = {}
        for column in columns:
            q1 = data[column].quantile(0.25)
            q3 = data[column].quantile(0.75)
            iqr = q3 - q1
            lower_bound = q1 - 1.5 * iqr
            upper_bound = q3 + 1.5 * iqr
            idx_column = (data[column].isnull())|((data[column] > lower_bound) & (data[column] < upper_bound))
            outliers[column] = ~idx_column
        return outliers

    @staticmethod
    def get_dummies(data: pd.DataFrame, columns: list) -> pd.DataFrame:
        """ Get dummies from the columns.

        Args:
            data (pd.DataFrame): Data frame to get the dummies.
            columns (list): Columns to get the dummies.

        Returns:
            pd.DataFrame: Data frame with the dummies.
        """
        data = pd.get_dummies(data.loc[:,columns], columns=columns).astype(int)
        return data
    @staticmethod
    def drop_null_columns(data: pd.DataFrame, threshold: float) -> pd.DataFrame:
        """ Drop columns with a percentage of null values higher than the threshold.

        Args:
            data (pd.DataFrame): Data frame to drop the columns.
            threshold (float): Threshold to drop the columns.

        Returns:
            pd.DataFrame: Data frame with the columns dropped.
        """
        null_values = data.isnull().sum()
        columns_to_drop = null_values[null_values/len(data) > threshold].index
        data = data.drop(columns=columns_to_drop)
        return data
    
    @staticmethod
    def drop_columns_constant_values(data: pd.DataFrame) -> pd.DataFrame:
        """ Drop columns with constant values.

        Args:
            data (pd.DataFrame): Data frame to drop the columns.

        Returns:
            pd.DataFrame: Data frame with the columns dropped.
        """
        columns_to_drop = data.columns[data.nunique() == 1]
        data = data.drop(columns=columns_to_drop)
        return data
    
    
    
    
    