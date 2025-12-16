import numpy as np
import pandas as pd

@pd.api.extensions.register_dataframe_accessor("cust")
class DescribeDataframes:
    """Custom pandas DataFrame accessor for comprehensive data analysis.
    
    This accessor provides a suite of diagnostic and descriptive methods
    for analyzing DataFrames. Access via df.cust.method_name().
    
    Attributes:
        _obj: The pandas DataFrame object this accessor is bound to.
    
    Examples:
        >>> df.cust.describe()  # Run full standard analysis
        >>> df.cust.missing_analysis()  # Check missing values only
    """
    
    def __init__(
        self, 
        pandas_obj):
        self._obj = pandas_obj

    def describe(
        self, 
        level='standard',
        ):
        """Main entry point to all descriptive methods on dataframes.
        
        Args:
            level (str): Analysis level to perform. Options are:
                - 'standard': Runs basic_info, memory_usage, missing_analysis,
                  exact_duplicates, and analyze_columns
                - 'missing': Runs missing_check_security
                Default is 'standard'.
        """
        if level == 'standard':
            self.basic_info()
            self.memory_usage()
            self.missing_analysis()
            self.exact_duplicates()
            self.analyze_columns()
        else:
            pass # nothing for now, calling functions individually past standard

    def basic_info(
        self):
        """Print basic dataframe information.
        
        Displays the dimensions (rows × columns) and data types
        of all columns in the dataframe.
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

    def memory_usage(
        self):
        """Display memory usage of the dataframe.
        
        Prints the total memory consumption in megabytes (MB),
        including deep memory usage of object types.
        """
        # Dataframe memory usage
        print("\n" + "=" * 60)
        print("MEMORY USAGE")
        print("=" * 60)
        mb = self._obj.memory_usage(deep=True).sum() / (1024 * 1024)
        print(f"Total memory: {mb:.2f} MB")
        print("=" * 60)

    def missing_analysis(
        self):
        """Analyze and display missing data patterns.
        
        For each column, displays:
            - Missing value count
            - Percentage of missing values
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

    def exact_duplicates(
        self):
        """Find and display exact duplicate rows.
        
        Displays:
            - Count of duplicate rows
            - Percentage of dataframe that is duplicated
            - Full dataframe of duplicate rows (if any exist)
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
        """Analyze and display statistics for all columns by type.
        
        For numeric columns, displays:
            - Minimum value
            - Maximum value
            - NA count and percentage
        
        For categorical/string columns, displays:
            - Unique value count
            - NA count and percentage
            - Top 10 most frequent values with counts
        
        Args:
            return_category_dict (bool): If True, returns a dictionary of
                categorical columns mapped to their unique values.
                Default is False.
            return_numeric_dictionary (bool): If True, returns a dictionary of
                numeric columns mapped to their statistics tuple.
                Default is False.
        
        Returns:
            dict or None: Dictionary of results if return flags are True,
                otherwise None (prints only).
        
        Note:
            Dictionary return functionality is not yet implemented.
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
        object_df = self._obj.select_dtypes(include=['object','string','category'])
        
        if len(object_df.columns) > 0:
            print("\n" + "=" * 60)
            print("CATEGORICAL/STRING COLUMNS ANALYSIS")
            print("=" * 60)
            
            for col in object_df.columns:
                na_count = object_df[col].isna().sum()
                na_percent = (na_count / len(object_df)) * 100
                unique_values = object_df[col].nunique()
                
                print(f"\n[{col}]")
                print(f"  Missing: {na_count:,} ({na_percent:.2f}%)")
                print(f"  Unique values: {unique_values:,}")
                print(f"  Top 10 values:")
                top_values = object_df[col].value_counts(dropna=True).head(10)
                print(top_values)
                print("-" * 60)
            
            print("=" * 60)

    def missing_check_permno(
        self, 
        attribute=None):
        """Check for missing values by permno for a specific attribute.
        
        Analyzes missing data patterns by counting how many unique securities
        (permnos) have missing values for the specified attribute.
        
        Args:
            attribute (str, optional): Specific attribute/column to check for
                missing values. If None, prompts user to specify.
        
        Raises:
            ValueError: If attribute is None or doesn't exist in dataframe.
        """
        if attribute is None:
            raise ValueError("Must specify an attribute/column to check for missing values")
        
        if attribute not in self._obj.columns:
            raise ValueError(f"Column '{attribute}' not found in dataframe")
        
        # Calculate missing data statistics
        total_missing = self._obj[attribute].isna().sum()
        total_rows = len(self._obj)
        missing_percent = (total_missing / total_rows) * 100
        
        # Count unique permnos with missing values
        missing_permnos = self._obj[self._obj[attribute].isna()]['permno'].nunique()
        total_permnos = self._obj['permno'].nunique()
        permno_percent = (missing_permnos / total_permnos) * 100
        
        # Print results in established format
        print("\n" + "=" * 60)
        print(f"MISSING DATA ANALYSIS: {attribute}")
        print("=" * 60)
        print(f"Total missing values: {total_missing:,} ({missing_percent:.2f}%)")
        print(f"Missing across {missing_permnos:,} unique securities ({permno_percent:.2f}%)")
        print(f"Total securities: {total_permnos:,}")
        print("=" * 60)

    def missing_check_mktcap(
        self,
        attribute=None
        ):
        """Analyze market capitalization distribution for securities with missing attribute.
        
        Examines the market cap (dlycap) distribution of securities that have
        missing values for the specified attribute to understand if missing data
        is concentrated in certain market cap ranges.
        
        Args:
            attribute (str, optional): Attribute/column to check for missing values.
                If None, prompts user to specify.
        
        Raises:
            ValueError: If attribute is None or doesn't exist in dataframe.
            ValueError: If 'dlycap' column is not found in dataframe.
        """
        if attribute is None:
            raise ValueError("Must specify an attribute/column to check for missing values")
        
        if attribute not in self._obj.columns:
            raise ValueError(f"Column '{attribute}' not found in dataframe")
        
        if 'dlycap' not in self._obj.columns:
            raise ValueError("Column 'dlycap' not found in dataframe")
        
        # Get market caps for rows where attribute is missing
        missing_caps = self._obj[self._obj[attribute].isna()]['dlycap']
        
        # Calculate statistics
        caps_stats = missing_caps.describe()
        total_missing = len(missing_caps)
        missing_caps_na = missing_caps.isna().sum()
        
        # Print results in established format
        print("\n" + "=" * 60)
        print(f"MARKET CAP ANALYSIS FOR MISSING: {attribute}")
        print("=" * 60)
        print(f"Total records with missing {attribute}: {total_missing:,}")
        print(f"Records with missing market cap: {missing_caps_na:,}")
        print("-" * 60)
        print("Market Cap Distribution (for non-NA caps):")
        
        # Format the statistics without scientific notation
        with pd.option_context('display.float_format', '{:,.2f}'.format):
            print(caps_stats)
        
        print("=" * 60)

    def missing_check_timeseries(
        self,
        attribute=None,
        date_column='dlycaldt',
        permno_column='permno',
        status_column='tradingstatusflg',
        top_n=20,
        show_detailed=True
        ):
        """Analyze time series patterns of missing data for an attribute.
        
        Comprehensive analysis including temporal patterns, terminal observations,
        contiguity, and per-security breakdowns of missing values.
        
        Args:
            attribute (str, optional): Attribute/column to check for missing values.
                If None, prompts user to specify.
            date_column (str): Column name containing dates for grouping.
                Default is 'dlycaldt'.
            permno_column (str): Column name containing security identifiers.
                Default is 'permno'.
            status_column (str): Column name containing trading status.
                Default is 'tradingstatusflg'.
            top_n (int): Number of most recent dates to display. Default is 20.
            show_detailed (bool): If True, shows detailed non-terminal analysis.
                Default is True.
        
        Raises:
            ValueError: If attribute is None or doesn't exist in dataframe.
            ValueError: If required columns are not found in dataframe.
        """
        if attribute is None:
            raise ValueError("Must specify an attribute/column to check for missing values")
        
        # Validate columns exist
        required_cols = [attribute, date_column, permno_column]
        for col in required_cols:
            if col not in self._obj.columns:
                raise ValueError(f"Column '{col}' not found in dataframe")
        
        # Get missing data
        missing_df = self._obj[self._obj[attribute].isna()].copy()
        total_missing = len(missing_df)
        
        # === SECTION 1: TEMPORAL PATTERN ===
        missing_by_date = missing_df.groupby(date_column).size().sort_index()
        total_dates = self._obj[date_column].nunique()
        dates_with_missing = len(missing_by_date)
        
        print("\n" + "=" * 60)
        print(f"TIME SERIES MISSING DATA ANALYSIS: {attribute}")
        print("=" * 60)
        print(f"Total missing records: {total_missing:,}")
        print(f"Date range: {missing_by_date.index.min()} to {missing_by_date.index.max()}")
        print(f"Days with missing: {dates_with_missing:,} / {total_dates:,}")
        print(f"Average missing per affected day: {missing_by_date.mean():.2f}")
        print("-" * 60)
        print(f"Most recent {min(top_n, len(missing_by_date))} dates with missing data:")
        print(missing_by_date.tail(top_n))
        
        # === SECTION 2: TERMINAL OBSERVATION ANALYSIS ===
        # Sort and get last observation for each security
        df_sorted = self._obj.sort_values([permno_column, date_column])
        last_obs = df_sorted.groupby(permno_column).tail(1)[[permno_column, date_column, attribute]]
        last_obs.columns = [permno_column, 'final_date', 'final_value_missing']
        last_obs['final_value_missing'] = last_obs['final_value_missing'].isna()
        
        # Per-security missing data analysis
        per_security = missing_df.groupby(permno_column).agg({
            date_column: ['min', 'max', 'count'],
            status_column: lambda x: x.mode()[0] if len(x.mode()) > 0 else None
        })
        per_security.columns = ['first_missing_date', 'last_missing_date', 'missing_count', 'most_common_status']
        per_security = per_security.reset_index()
        
        unique_permnos = len(per_security)
        
        # Merge to check if missing values are terminal
        comparison = per_security.merge(last_obs, on=permno_column, how='left')
        comparison['is_terminal_missing'] = (
            (comparison['last_missing_date'] == comparison['final_date']) & 
            comparison['final_value_missing']
        )
        
        terminal_count = comparison['is_terminal_missing'].sum()
        terminal_pct = (terminal_count / unique_permnos * 100) if unique_permnos > 0 else 0
        
        print("\n" + "-" * 60)
        print("TERMINAL OBSERVATION ANALYSIS")
        print("-" * 60)
        print(f"Securities with missing data: {unique_permnos:,}")
        print(f"All missing values are terminal: {terminal_count:,} ({terminal_pct:.1f}%)")
        print(f"Non-terminal missing values: {unique_permnos - terminal_count:,}")
        
        # === SECTION 3: CONTIGUITY ANALYSIS ===
        # Calculate if missing values form contiguous blocks
        per_security['days_span'] = (
            pd.to_datetime(per_security['last_missing_date']) - 
            pd.to_datetime(per_security['first_missing_date'])
        ).dt.days + 1
        
        per_security['is_contiguous'] = per_security['missing_count'] == per_security['days_span']
        contiguous_count = per_security['is_contiguous'].sum()
        contiguous_pct = (contiguous_count / unique_permnos * 100) if unique_permnos > 0 else 0

        print("\n" + "-" * 60)
        print("CONTIGUITY ANALYSIS")
        print("-" * 60)
        print(f"Contiguous missing blocks: {contiguous_count:,} ({contiguous_pct:.1f}%)")
        print(f"Scattered missing values: {unique_permnos - contiguous_count:,}")
        
        # === SECTION 4: NON-TERMINAL MISSING VALUES (CRITICAL) ===
        if show_detailed:
            non_terminal = comparison[~comparison['is_terminal_missing']].copy()
            if len(non_terminal) > 0:
                print("\n" + "-" * 60)
                print("NON-TERMINAL MISSING VALUES (CRITICAL)")
                print("-" * 60)
                print(f"Count: {len(non_terminal):,}")
                
                # Calculate days before end
                non_terminal['days_before_end'] = (
                    pd.to_datetime(non_terminal['final_date']) - 
                    pd.to_datetime(non_terminal['last_missing_date'])
                ).dt.days
                
                print("\nDays between last missing and final observation:")
                with pd.option_context('display.float_format', '{:,.0f}'.format):
                    print(non_terminal['days_before_end'].describe())
                
                print(f"\nSample of problematic securities:")
                display_cols = [permno_column, 'first_missing_date', 'last_missing_date', 
                               'missing_count', 'final_date', 'most_common_status']
                # Filter to only existing columns
                display_cols = [col for col in display_cols if col in non_terminal.columns]
                print(non_terminal[display_cols].head(10).to_string(index=False))
        
        print("=" * 60)

    def missing_check_tradingstatus(
        self,
        attribute=None,
        status_column='tradingstatusflg'
        ):
        """Analyze trading status for records with missing attribute values.
        
        Examines the distribution of trading statuses for records that have
        missing values in the specified attribute. Particularly useful for
        identifying if missing data is concentrated in delisted securities.
        
        Args:
            attribute (str, optional): Attribute/column to check for missing values.
                If None, prompts user to specify.
            status_column (str): Column name containing trading status information.
                Default is 'tradingstatusflg'.
        
        Raises:
            ValueError: If attribute is None or doesn't exist in dataframe.
            ValueError: If status_column is not found in dataframe.
        """
        if attribute is None:
            raise ValueError("Must specify an attribute/column to check for missing values")
        
        if attribute not in self._obj.columns:
            raise ValueError(f"Column '{attribute}' not found in dataframe")
        
        if status_column not in self._obj.columns:
            raise ValueError(f"Trading status column '{status_column}' not found in dataframe")
        
        # Get records with missing attribute
        missing_df = self._obj[self._obj[attribute].isna()]
        total_missing = len(missing_df)
        
        # Trading status breakdown
        status_breakdown = missing_df[status_column].value_counts(dropna=False)
        
        # Calculate delisted percentage if 'D' status exists
        delisted_count = status_breakdown.get('D', 0)
        delisted_pct = (delisted_count / total_missing * 100) if total_missing > 0 else 0
        
        # Print results in established format
        print("\n" + "=" * 60)
        print(f"TRADING STATUS ANALYSIS FOR MISSING: {attribute}")
        print("=" * 60)
        print(f"Total records with missing {attribute}: {total_missing:,}")
        print("-" * 60)
        print("Trading Status Breakdown:")
        print(status_breakdown)
        print("-" * 60)
        print(f"Delisted ('D') Securities: {delisted_count:,} ({delisted_pct:.1f}%)")
        print("=" * 60)

    def missing_check_sharetype(
        self,
        attribute=None,
        sharetype_column='sharetype'
        ):
        """Analyze share types for records with missing attribute values.
        
        Examines the distribution of share types (sharetype) for records
        that have missing values in the specified attribute to understand
        if missing data is concentrated in certain share types.
        
        Args:
            attribute (str, optional): Attribute/column to check for missing values.
                If None, prompts user to specify.
            sharetype_column (str): Column name containing share type information.
                Default is 'sharetype'.
        
        Raises:
            ValueError: If attribute is None or doesn't exist in dataframe.
            ValueError: If sharetype_column is not found in dataframe.
        """
        if attribute is None:
            raise ValueError("Must specify an attribute/column to check for missing values")
        
        if attribute not in self._obj.columns:
            raise ValueError(f"Column '{attribute}' not found in dataframe")
        
        if sharetype_column not in self._obj.columns:
            raise ValueError(f"Share type column '{sharetype_column}' not found in dataframe")
        
        # Get records with missing attribute
        missing_df = self._obj[self._obj[attribute].isna()]
        total_missing = len(missing_df)
        
        # Share type breakdown
        sharetype_breakdown = missing_df[sharetype_column].value_counts(dropna=False)
        
        # Print results in established format
        print("\n" + "=" * 60)
        print(f"SHARE TYPE ANALYSIS FOR MISSING: {attribute}")
        print("=" * 60)
        print(f"Total records with missing {attribute}: {total_missing:,}")
        print("-" * 60)
        print("Share Type Breakdown:")
        print(sharetype_breakdown)
        print("=" * 60)