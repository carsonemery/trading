import pandas as pd
import numpy as np

""" Bolinger band features """

def bb_wdith_offset(
    df: pd.DataFrame,
    offset: int,
    lookback: int
    ) -> pd.DataFrame:
    """ Calculates bb width in a given lookback period using a given offset 

    Examples:
        bb_width_pct_20d_now = mean((upper - lower) / middle) over last 20 days
        bb_width_pct_20d_20d_ago = mean((upper - lower) / middle) for days -40 to -20
        bb_width_pct_20d_40d_ago = mean((upper - lower) / middle) for days -60 to -40
        bb_width_pct_20d_60d_ago = mean((upper - lower) / middle) for days -80 to -60
        bb_width_pct_20d_80d_ago = mean((upper - lower) / middle) for days -100 to -80

    """


    pass


def bb_price_position(
    df: pd.DataFrame,
    offset: int,
    lookback: int
    ) -> pd.DataFrame:
    """ calculates a ratio of where price traded within each bolinger band within a given lookback and offset

    Examples:

        bb_position_20d_now = mean((close - lower) / (upper - lower)) over last 20 days
        bb_position_20d_20d_ago = mean((close - lower) / (upper - lower)) for days -40 to -20
        bb_position_20d_40d_ago = ...
        bb_position_20d_60d_ago = ...
        bb_position_20d_80d_ago = ...

        0.5 = middle, 0.8 = upper band area, 0.2 = lower band area

    """