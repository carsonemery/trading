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
            self.check_monotonic() # want to put more stuff related to the validity of a time series here

    def basic_info(self):
        """
        Prints the dimensions of the dataframe (columns and rows)
        Prints all of the columns and their data types
        """
        # # Number of columns, rows, and column names and datatypes
        print("\n" + "=" * 60)
        print("BASIC INFORMATION")
        print("=" * 60)
        print(f"Dimensions: {len(self._obj):,} rows × {self._obj.shape[1]} columns")
        print("-" * 60)
        print("Column Data Types:")
        print(self._obj.dtypes)
        print("=" * 60)

    def memory_usage(self):
        """
        Describes the memory usage of the dataframe
        """
        # Dataframe memory usage
        print("\n" + "=" * 60)
        print("MEMORY USAGE")
        print("=" * 60)
        mb = self._obj.memory_usage(deep=True).sum() / (1024 * 1024)
        print(f"Total memory: {mb:.2f} MB")
        print("=" * 60)

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
        print("\n" + "=" * 60)
        print("MISSING VALUES ANALYSIS")
        print("=" * 60)
        with pd.option_context(
                'display.max_rows', None, 
                'display.max_columns', None, 
                'display.max_colwidth', None):
            print(missing_df.to_string(index=False))
        print("=" * 60)

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
        dup_count = len(duplicate_df)
        dup_percent = (dup_count / len(self._obj)) * 100 if len(self._obj) > 0 else 0
        
        print("\n" + "=" * 60)
        print("EXACT DUPLICATES")
        print("=" * 60)
        print(f"Found {dup_count:,} duplicate rows ({dup_percent:.2f}%)")
        print("-" * 60)
        if dup_count > 0:
            with pd.option_context(
                    'display.max_rows', None, 
                    'display.max_columns', None, 
                    'display.max_colwidth', None):
                print(duplicate_df)
        else:
            print("No duplicates found")
        print("=" * 60)

    def analyze_columns(
        self,
        return_category_dict=False, 
        return_numeric_dictionary=False):
        """
        For each column performs analysis:

            for each numeric type, min, max

            for each object/ string type column prints:
                - The total number of unique values
                - The number of Nan or null values based on isna()
                - Each canonical value up to the first 10 under each column


        Parameters:

            return_category_dictionary, returns a dictionary of format {str,list} 
            with each category column and the unique values of each

            return_numeric_dictionary, returns a dictionary of format {str, tuple}

            @TODO add dictionary creation and return logic
        
        """
        ## Numeric Column Processing ##
        numeric_df = self._obj.select_dtypes(include='number')
        
        if len(numeric_df.columns) > 0:
            print("\n" + "=" * 60)
            print("NUMERIC COLUMNS ANALYSIS")
            print("=" * 60)
            
            min_max_df = numeric_df.agg(['min', 'max'])
            min_max_df.loc['na_count'] = numeric_df.isna().sum()
            min_max_df.loc['na_percent'] = (numeric_df.isna().sum() / len(numeric_df)) * 100
            min_max_df = min_max_df.T
            
            print(min_max_df)
            print("=" * 60)
        
        ## String Column Processing ##
        object_df = self._obj.select_dtypes(include='object')
        
        if len(object_df.columns) > 0:
            print("\n" + "=" * 60)
            print("CATEGORICAL/STRING COLUMNS ANALYSIS")
            print("=" * 60)
            
            for col in object_df.columns:
                na_count = object_df[col].isna().sum()
                na_percent = (na_count / len(object_df)) * 100
                unique_values = object_df[col].nunique()
                
                print(f"\n[{col}]")  # Cleaner column name display
                print(f"  Missing: {na_count:,} ({na_percent:.2f}%)")
                print(f"  Unique values: {unique_values:,}")
                print(f"  Top 10 values:")
                top_values = object_df[col].value_counts(dropna=True).head(10)
                for value, count in top_values.items():
                    print(f"    {value}: {count:,}")
                print("-" * 60)
            
            print("=" * 60)

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