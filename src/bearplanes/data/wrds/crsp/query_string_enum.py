from enum import Enum

class CSRPQueryStrings(Enum):
    DAILY_DATA =  """
    SELECT *
    FROM crspq.dsf_v2
    WHERE YYYYMMDD >= '{year}0101'
        AND YYYYMMDD < '{next_year}0101'
    ORDER BY YYYYMMDD, permno
    """
    
    DISTRIBUTIONS = """
    SELECT *
    FROM crspq.stkdistributions
    WHERE disexdt >= '{year}-01-01'
        AND disexdt < '{next_year}-01-01'
    """

    INDICIES = """
    SELECT *
    FROM crspq.wrds_dailyindexret
    WHERE dlycaldt >= '{year}-01-01'
        AND dlycaldt < '{next_year}-01-01'
    ORDER BY dlycaldt
    """

    SHARES_OUTSTANDING = """
    SELECT *
    FROM crspq.stkshares
    WHERE shrstartdt >= '{year}-01-01'
        AND shrstartdt < '{next_year}-01-01'
    ORDER BY shrstartdt, permno
    """

    DELISTINGS = """
    SELECT *
    FROM crspq.stkdelists
    WHERE delistingdt >= '{year}-01-01'
        AND delistingdt < '{next_year}-01-01'
    ORDER BY delistingdt
    """

    SEC_INFO= """
    SELECT *
    FROM crspq.stksecurityinfohdr
    """
        
    NAMES = """
    SELECT *
    FROM crspq.wrds_names_query
    """