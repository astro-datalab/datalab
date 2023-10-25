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
UNITTEST_OK_TOKEN = "unittest.666.666.$1$fdqasldur927JL97asldfj9279172B/"
UNITTEST_KO_TOKEN = "unittest.666.666.$2$fdqasldur927JL97asldfj9279172B/"
UNITTEST_TOO_SHORT = "unittest.666.666.$1$fdqasldur927JL97asldf"
SOME_USER = "some_user.3199.159.$1$3xf8E6xf$RL9XLqIyFCRsTOISOBTuL."


@pytest.mark.parametrize(
    "token, expected",
   [
       ("", False),
       (ANON_TOKEN, True),
       (DEMO_TOKEN, True),
       (TEST_TOKEN, True),
       (UNITTEST_OK_TOKEN, True),
       (UNITTEST_KO_TOKEN, False),
       (UNITTEST_TOO_SHORT, False),

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
        (UNITTEST_OK_TOKEN, ["unittest", "666", "666", "$1$fdqasldur927JL97asldfj9279172B/"]),
        (SOME_USER, ["some_user", "3199", "159", "$1$3xf8E6xf$RL9XLqIyFCRsTOISOBTuL."])
    ]
)
def test_split_auth_token_parts(token, expected):
    res = util.split_auth_token(token)
    for a, b in zip(res, expected):
        assert a == b


@pytest.mark.parametrize(
    "token, expected",
    [
        (ANON_TOKEN, ["anonymous", "0", "0", "anon_access"]),
        (DEMO_TOKEN, ["dldemo", "99999", "99999", "demo_access"]),
        (TEST_TOKEN, ["dltest", "99998", "99998", "test_access"]),
        (UNITTEST_OK_TOKEN, ["unittest", "666", "666", "$1$fdqasldur927JL97asldfj9279172B/"]),
        (SOME_USER, ["some_user", "3199", "159", "$1$3xf8E6xf$RL9XLqIyFCRsTOISOBTuL."]),
        (UNITTEST_KO_TOKEN, None)
    ]
)
def test_auth_token_to_dict(token, expected):
    if expected is not None:
        expected_dict = {k: v for k, v in zip(['username', 'uid', 'gid', 'hash'], expected)}
    else:
        expected_dict = None
    res = util.auth_token_to_dict(token)
    assert res == expected_dict
