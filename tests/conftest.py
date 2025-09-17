import os
import pathlib
import pytest
import sys
from ..p2p_manual_reconcile_tool import AppWorxEnum, get_apwx, parse_args


TEST_BASE_PATH = pathlib.Path(os.path.dirname(__file__))

# Multiple structures such as this can be created to have fixtures
# with different behaviors
SCRIPT_ARGUMENTS = {
    str(AppWorxEnum.HOST): "FTFTST",
    str(AppWorxEnum.SID): "FTFTST",
    str(AppWorxEnum.P2P_SERVER): "test_p2p_server",
    str(AppWorxEnum.P2P_SCHEMA): "test_schema",
    str(AppWorxEnum.CONFIG_FILE): TEST_BASE_PATH.parent / "config.yaml",
    str(AppWorxEnum.INPUT_FILE): TEST_BASE_PATH / "test_input.xlsx",
    str(AppWorxEnum.OUTPUT_FILE_PATH): TEST_BASE_PATH,
    str(AppWorxEnum.RPT_ONLY): "N",
    str(AppWorxEnum.RECONCILE_DATE): "01-15-2024",
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
    setup(SCRIPT_ARGUMENTS)
    appworx = parse_args(get_apwx())
    teardown(SCRIPT_ARGUMENTS)
    return appworx