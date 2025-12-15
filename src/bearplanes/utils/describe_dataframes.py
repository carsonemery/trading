import numpy as np
import pandas as pd

# Considers np.inf values as na
pd.options.mode.use_inf_as_na = True

@pd.api.extensions.register_dataframe_accessor("cust")
class DescribeDataframes:
    def __init__(self, pandas_obj):
        self._obj = pandas_obj

    def describe(
        self, 
        level='standard',
        ):
        """
        Main entry point to all descriptive methods on dataframes
            
        """
        if level == 'standard':
            self.basic_info()
            self.memory_usage()
            self.missing_analysis()
            self.exact_duplicates()
            self.analyze_columns()
        else:
            pass

    def basic_info(self):
        """
        Prints the dimensions of the dataframe (columns and rows)
        Prints all of the columns and their data types
        """
        # # Number of columns, rows, and column names and datatypes
        print("-" * 20)
        print(f"Dataframe has {len(self._obj)} rows and {self._obj.columns} columns ")
        print("-" * 20)
        print("Columns and Types:")
        print(self._obj.dtypes)
        print("-" * 20)

    def memory_usage(self):
        """
        Describes the memory usage of the dataframe
        """
        # Dataframe memory usage
        print("-" * 20)
        print(f"Dataframe size in memory: {self._obj.memory_usage(deep=True).sum() / (1024 * 1024)} Megabytes")
        print("-" * 20)

    def missing_analysis(self):
        """
        Show missing data patterns

        For each column, prints column +:
        - Nan/Na count and percentage
       """
        missing_count = self._obj.isna().sum()
        missing_df = pd.DataFrame({
            'column_name': missing_count.index,
            'missing_count': missing_count.values,
            'percent_missing': (missing_count.values / len(self._obj)) * 100
        })
        print("-" * 20)
        print(f"Information of Missing Values")
        with pd.option_context(
                'display.max_rows', None, 
                'display.max_columns', None, 
                'display.max_colwidth', None):
            print(missing_df)
        print("-" * 20)

    def exact_duplicates(self):
        """
        Finds exact duplicates
        prints,
            - entire dataframe containing duplicates
            - number of exact duplicates
            - percent of Dataframe
        """
        # Create a dataframe of exact duplicates
        duplicate_df = self._obj[self._obj.duplicated(keep=False)]

        print("-" * 20)
        print("Exact Duplicates:")
        with pd.option_context(
                'display.max_rows', None, 
                'display.max_columns', None, 
                'display.max_colwidth', None):
            print(duplicate_df)
        print()
        print(f"Percentage of duplicates: {len(duplicate_df) / len(self._obj) * 100:.2f}%")        
        print("-" * 20)

    def analyze_columns(
        self,
        return_category_dict=False, 
        return_numeric_dictionary=False):
        """
        For each column performs analysis:

            for each numeric type, min, max

            for each object/ string type, prints 

        Parameters:

            return_category_dictionary, returns a dictionary of format {str,list} 
            with each category column and the unique values of each

            return_numeric_dictionary, returns a dictionary of format {str, tuple}
        
        """

        # numeric_df = df.select_dtypes(include=['number'])
        pass

    def check_monotonic(
        self, 
        series_key: str):
        """
        Checks if the dates of every unique series are non increasing

        Parameters:

        self

        series_key:
            the key we will use to group rows into series, example could be ticker, or permno
            should mostly be unique.

        """
        pass