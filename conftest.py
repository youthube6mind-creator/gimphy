import os
import pathlib
import pytest
import sys
from rtxn_description_cleaner import AppWorxEnum, get_apwx, parse_args


TEST_BASE_PATH = pathlib.Path(os.path.dirname(__file__))

# Script arguments for testing
SCRIPT_ARGUMENTS = {
    str(AppWorxEnum.TNS_SERVICE_NAME): "DNATST3",
    str(AppWorxEnum.CONFIG_FILE): str(TEST_BASE_PATH / "config.yaml"),
    str(AppWorxEnum.FULL_CLEAN_YN): "N",
    str(AppWorxEnum.DAYS_BACK): "3",
    str(AppWorxEnum.RPT_ONLY_YN): "Y",
    str(AppWorxEnum.OUTPUT_FILE_PATH): str(TEST_BASE_PATH),
    str(AppWorxEnum.OUTPUT_FILE_NAME): "test_rtxn_desc_cleaner_audit_log.csv",
}


def setup(script_args: dict):
    """Setup command line arguments to be passed to AppWorx library"""
    for k, v in script_args.items():
        sys.argv.append(f"{k}={v}")


def teardown(script_args: dict):
    """Cleanup command line arguments"""
    for _ in script_args:
        sys.argv.pop()


@pytest.fixture(scope="module")
def apwx():
    """Fixture to provide configured AppWorx object for testing"""
    setup(SCRIPT_ARGUMENTS)
    appworx = parse_args(get_apwx())
    teardown(SCRIPT_ARGUMENTS)
    return appworx


@pytest.fixture
def sample_ext_data():
    """Fixture providing sample external transaction data"""
    return [
        {
            'EXTRTXNDESCNBR': 1,
            'EXTRTXNDESCTEXT': 'Test Description with bad chars\x00\x01'
        },
        {
            'EXTRTXNDESCNBR': 2,
            'EXTRTXNDESCTEXT': 'Another description with unicode café'
        }
    ]


@pytest.fixture
def sample_int_data():
    """Fixture providing sample internal transaction data"""
    return [
        {
            'INTRTXNDESCNBR': 1,
            'INTRTXNDESCTEXT': 'Internal desc with special chars\x7f'
        },
        {
            'INTRTXNDESCNBR': 2,
            'INTRTXNDESCTEXT': 'Normal internal description'
        }
    ]


@pytest.fixture
def mock_config():
    """Fixture providing mock configuration"""
    return {
        'queries': {
            'get_ext_full': 'SELECT extrtxndescnbr, extrtxndesctext FROM extrtxndesc',
            'get_ext_incremental': 'SELECT extrtxndescnbr, extrtxndesctext FROM extrtxndesc WHERE datelastmaint > sysdate - :days_back',
            'get_int_full': 'SELECT intrtxndescnbr, intrtxndesctext FROM intrtxndesc',
            'get_int_incremental': 'SELECT intrtxndescnbr, intrtxndesctext FROM intrtxndesc WHERE datelastmaint > sysdate - :days_back',
            'update_ext': 'UPDATE extrtxndesc SET extrtxndesctext = :new_text WHERE extrtxndescnbr = :desc_nbr',
            'update_int': 'UPDATE intrtxndesc SET intrtxndesctext = :new_text WHERE intrtxndescnbr = :desc_nbr'
        }
    }