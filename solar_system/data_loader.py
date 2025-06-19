import pandas as pd
import os
from datetime import datetime


class DataLoader:
    """
    Base class for loading and preprocessing data from CSV files or folders.
    It handles common operations like renaming columns, keeping specific columns,and converting data types.
    """
    def __init__(self, config: dict, global_config: dict):
        """
        Initializes the DataLoader with specific dataset configuration and global configuration.

        Args:
            config (dict): Configuration for the specific dataset (e.g., outbound_config).
            global_config (dict): Global column configurations (e.g., global_config).
        """
        self.config = config
        self.global_config = global_config
        self.df = None  # DataFrame to store the loaded data

    def _load_single_csv(self, file_path: str) -> pd.DataFrame:
        """
        Loads a single CSV file into a pandas DataFrame.

        Args:
            file_path (str): The full path to the CSV file.

        Returns:
            pd.DataFrame: The loaded DataFrame, or an empty DataFrame if an error occurs.
        """
        header_row = self.config.get("header_row", 0)
        try:
            # Using low_memory=False to avoid DtypeWarning for mixed types
            return pd.read_csv(file_path, header=header_row, low_memory=False)
        except FileNotFoundError:
            print(f"Error: File not found at {file_path}")
            return pd.DataFrame()
        except Exception as e:
            print(f"Error loading CSV {file_path}: {e}")
            return pd.DataFrame()

    def _load_folder_csvs(self, folder_path: str) -> pd.DataFrame:
        """
        Loads and combines all CSV files from a specified folder into a single DataFrame.

        Args:
            folder_path (str): The path to the folder containing CSV files.

        Returns:
            pd.DataFrame: A concatenated DataFrame of all CSVs in the folder,
                          or an empty DataFrame if no CSVs are found or an error occurs.
        """
        all_dfs = []
        if not os.path.isdir(folder_path):
            print(f"Error: Folder not found at {folder_path}")
            return pd.DataFrame()

        for file_name in os.listdir(folder_path):
            if file_name.endswith(".csv"):
                file_path = os.path.join(folder_path, file_name)
                df = self._load_single_csv(file_path)
                if not df.empty:
                    all_dfs.append(df)
        if all_dfs:
            return pd.concat(all_dfs, ignore_index=True)
        else:
            print(f"No CSV files found in {folder_path}")
            return pd.DataFrame()

    def load_data(self):
        """
        Loads data based on the configuration type (single file or folder).
        The loaded DataFrame is stored in self.df.
        """
        data_type = self.config.get("type")
        path = self.config.get("path")

        if not path:
            print("Error: 'path' not defined in configuration.")
            self.df = pd.DataFrame()
            return

        if data_type == "file":
            print(f"Loading single CSV file: {path}")
            self.df = self._load_single_csv(path)
        elif data_type == "folder":
            print(f"Loading CSVs from folder: {path}")
            self.df = self._load_folder_csvs(path)
        else:
            print(f"Error: Unknown data type '{data_type}' in configuration. Must be 'file' or 'folder'.")
            self.df = pd.DataFrame()

        if self.df.empty:
            print("Warning: No data loaded.")

    def _rename_columns(self):
        """
        Renames columns in the DataFrame based on the global configuration.
        This method is called internally after data loading.
        """
        if self.df is not None and not self.df.empty:
            rename_map = self.global_config.get("rename_columns", {})
            # Create a valid rename map for columns that actually exist in the DataFrame
            valid_rename_map = {old_name: new_name for old_name, new_name in rename_map.items()
                                if old_name in self.df.columns}
            if valid_rename_map:
                self.df.rename(columns=valid_rename_map, inplace=True)
            else:
                print("No columns to rename based on global config.")
        else:
            print("DataFrame is empty or not loaded, skipping column renaming.")

    def _keep_columns(self):
        """
        Keeps only the specified columns in the DataFrame based on the dataset's configuration.
        This method is called internally after column renaming.
        """
        if self.df is not None and not self.df.empty:
            columns_to_keep = self.config.get("keep_columns", [])
            if columns_to_keep:
                # Filter for columns that actually exist in the DataFrame after renaming
                existing_columns_to_keep = [col for col in columns_to_keep if col in self.df.columns]
                if existing_columns_to_keep:
                    self.df = self.df[existing_columns_to_keep]
                else:
                    print("No specified 'keep_columns' exist in the DataFrame after renaming.")
                    self.df = pd.DataFrame() # If no columns to keep, return an empty dataframe
            else:
                print("No 'keep_columns' specified in configuration. All columns will be kept.")
        else:
            print("DataFrame is empty or not loaded, skipping column keeping.")

    def _convert_column_types(self):
        """
        Converts specified columns to numeric and datetime types based on the global configuration.
        This method is called internally after keeping columns.
        It now includes rules to transform '-' to '0.0' and '(N)' to -N for numeric columns.
        """
        if self.df is not None and not self.df.empty:
            numeric_cols = self.global_config.get("numeric_columns", [])
            datetime_cols = self.global_config.get("datetime_columns", [])

            # Convert numeric columns
            for col in numeric_cols:
                if col in self.df.columns:
                    print(f"Converting column '{col}' to numeric.")
                    # Convert column to string type first to apply string operations
                    self.df[col] = self.df[col].astype(str)

                    # Rule 1: Transform '-' to '0.0'
                    self.df[col] = self.df[col].replace('-', '0.0')

                    # Rule 2: Transform '(N)' to -N
                    # Use regex to find patterns like (digits) and replace with -digits
                    self.df[col] = self.df[col].str.replace(r'\((\d+(\.\d+)?)\)', r'-\1', regex=True)

                    # Use errors='coerce' to turn unparseable values into NaN
                    self.df[col] = pd.to_numeric(self.df[col], errors='coerce')

            # Convert datetime columns
            for col in datetime_cols:
                if col in self.df.columns:
                    print(f"Converting column '{col}' to datetime.")
                    # Use errors='coerce' to turn unparseable dates into NaT
                    self.df[col] = pd.to_datetime(self.df[col], dayfirst=True, errors='coerce')
        else:
            print("DataFrame is empty or not loaded, skipping type conversion.")

    def process_data(self):
        """
        Orchestrates the data loading and transformation process.
        The order of operations is: load data -> rename columns -> keep columns -> convert column types.
        """
        self.load_data()
        if not self.df.empty:
            self._rename_columns()
            self._keep_columns()
            self._convert_column_types()
        return self.df

    def get_data(self) -> pd.DataFrame:
        """
        Returns the processed DataFrame.

        Returns:
            pd.DataFrame: The cleaned and transformed DataFrame.
        """
        return self.df
