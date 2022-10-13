"""
    test_util.py - test functionality in dl/Util.py
    To run the test everything simply do:
        pytest tests/test_util.py
  To test individual test methods verbose (-v):
    pytest tests/test_util.py::test_is_auth_token -v
    pytest tests/test_util.py::test_split_auth_token_parts -v
"""

__authors__ = 'Igor Sola<igor.suarez-sola@noirlab.edu>'
__version__ = '20221011'  # yyyymmdd
import os
import sys
import pytest

# position the script relative to the code
ROOT_PATH = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(ROOT_PATH, '..'))
import dl.Util as util


ANON_TOKEN = "anonymous.0.0.anon_access"
DEMO_TOKEN = "dldemo.99999.99999.demo_access"
TEST_TOKEN = "dltest.99998.99998.test_access"
DEMO00_OK_TOKEN = "demo00.1018.1018.$1$fdqe8g1I$jN1F2CZ8MhdbZNnuG9jAB/"
DEMO00_KO_TOKEN = "demo00.1018.1018.$2$fdqe8g1I$jN1F2CZ8MhdbZNnuG9jAB/"
DEMO00_TOO_SHORT = "demo00.1018.1018.$1$fdqe8g1I$jN1F2CZ8Mhdb"
SOME_USER = "some_user.3195.159.$1$3xf8E6xf$RL9XLqIyFCRsTOISOBTuL."


@pytest.mark.parametrize(
    "token, expected",
   [
       ("", False),
       (ANON_TOKEN, True),
       (DEMO_TOKEN, True),
       (TEST_TOKEN, True),
       (DEMO00_OK_TOKEN, True),
       (DEMO00_KO_TOKEN, False),
       (DEMO00_TOO_SHORT, False),

   ]
)
def test_is_auth_token(token, expected):
    res = util.is_auth_token(token)
    assert res == expected


@pytest.mark.parametrize(
    "token, expected",
    [
        (ANON_TOKEN, ["anonymous", "0", "0", "anon_access"]),
        (DEMO_TOKEN, ["dldemo", "99999", "99999", "demo_access"]),
        (TEST_TOKEN, ["dltest", "99998", "99998", "test_access"]),
        (DEMO00_OK_TOKEN, ["demo00", "1018", "1018", "$1$fdqe8g1I$jN1F2CZ8MhdbZNnuG9jAB/"]),
        (SOME_USER, ["some_user", "3195", "159", "$1$3xf8E6xf$RL9XLqIyFCRsTOISOBTuL."])
    ]
)
def test_split_auth_token_parts(token, expected):
    res = util.split_auth_token(token)
    for a, b in zip(res, expected):
        assert a == b
