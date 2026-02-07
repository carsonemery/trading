import pandas as pd

import pytest

from bearplanes.data.wrds import WRDSClient
from bearplanes.utils import get_wrds_credentials


def test_load_environment():
    pass

def test_get_wrds_credentials():
    credentials_dict = get_wrds_credentials()
    username = credentials_dict['username']
    password = credentials_dict['password']

    assert username, "The wrds username did not init"
    assert password, "The wrds password did not init"

