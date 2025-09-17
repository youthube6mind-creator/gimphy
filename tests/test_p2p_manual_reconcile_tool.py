import pytest
import datetime
from unittest.mock import Mock, patch, mock_open
from pathlib import Path

from ..p2p_manual_reconcile_tool import (
    run, 
    parse_recon_file, 
    update_p2p_recon_date,
    update_rtxn_recon_date,
    update_card_recon_date,
    create_output_file_path,
    execute_sql
)


def test_run(apwx):
    """Using the `apwx` fixture from conftest.py, execute the run function"""
    with patch('p2p_manual_reconcile_tool.initialize') as mock_init, \
         patch('p2p_manual_reconcile_tool.parse_recon_file') as mock_parse, \
         patch('p2p_manual_reconcile_tool.create_output_file_path') as mock_output_path, \
         patch('p2p_manual_reconcile_tool.update_reconcile_date') as mock_update, \
         patch('builtins.open', mock_open()) as mock_file:
        
        # Setup mocks
        mock_script_data = Mock()
        mock_script_data.p2p_dbh = Mock()
        mock_script_data.dna_dbh = Mock()
        mock_init.return_value = mock_script_data
        mock_parse.return_value = []
        mock_output_path.return_value = "/test/output.txt"
        
        result = run(apwx)
        assert result is True


def test_parse_recon_file(mocker):
    """Test parsing of Excel reconciliation file"""
    mock_config = {
        'valid_column_headers': [
            'DETAIL_RECORD_ID', 'PAYMENT_ID', 'ACCTNBR', 'RTXNNBR',
            'NETWORK_ID', 'TRAN_TYPE', 'UPDATE_DETAIL_RECORD',
            'UPDATE_PAYMENT', 'UPDATE_RTXN', 'UPDATE_VISA', 'UPDATE_MC'
        ]
    }
    
    # Mock openpyxl
    mock_workbook = Mock()
    mock_worksheet = Mock()
    mock_workbook.active = mock_worksheet
    mock_worksheet.max_row = 3
    
    # Mock cells
    def mock_cell(row, column):
        cell_mock = Mock()
        if row == 1:  # Header row
            headers = mock_config['valid_column_headers']
            cell_mock.value = headers[column - 1] if column <= len(headers) else None
        else:  # Data rows
            cell_mock.value = f"test_value_{row}_{column}"
        return cell_mock
    
    mock_worksheet.cell = mock_cell
    
    mocker.patch('openpyxl.load_workbook', return_value=mock_workbook)
    
    result = parse_recon_file("test.xlsx", mock_config)
    
    assert len(result) == 2  # Two data rows
    assert isinstance(result[0], dict)


def test_execute_sql_success(mocker):
    """Test successful SQL execution"""
    mock_conn = Mock()
    mock_cursor = Mock()
    mock_cursor.rowcount = 1
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    
    rows_affected, error = execute_sql(mock_conn, "SELECT * FROM test", ["param1"])
    
    assert rows_affected == 1
    assert error is None
    mock_cursor.execute.assert_called_once_with("SELECT * FROM test", ["param1"])


def test_execute_sql_exception(mocker):
    """Test SQL execution with exception"""
    mock_conn = Mock()
    mock_cursor = Mock()
    mock_cursor.execute.side_effect = Exception("SQL Error")
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    
    rows_affected, error = execute_sql(mock_conn, "SELECT * FROM test")
    
    assert rows_affected == 0
    assert error == "SQL Error"


def test_update_p2p_recon_date(mocker):
    """Test updating P2P reconciliation date"""
    mock_dbh = Mock()
    mock_config = {
        'sql_queries': {
            'update_payment': 'UPDATE Payment SET ReconcileDate = ? WHERE Id = ?',
            'update_detail_record': 'UPDATE DetailRecord SET ReconcileDate = ? WHERE Id = ?'
        }
    }
    
    mocker.patch('p2p_manual_reconcile_tool.execute_sql', return_value=(1, None))
    
    rows_affected, error = update_p2p_recon_date(
        mock_dbh, 'FTF', 123, '01/15/2024', mock_config
    )
    
    assert rows_affected == 1
    assert error is None


def test_update_rtxn_recon_date(mocker):
    """Test updating RTXN reconciliation date"""
    mock_dbh = Mock()
    mock_config = {
        'sql_queries': {
            'insert_rtxn_recon_date': 'INSERT INTO rtxnentityattrib VALUES (?, ?, ?, ?, ?)'
        }
    }
    
    mocker.patch('p2p_manual_reconcile_tool.execute_sql', return_value=(1, None))
    
    rows_affected, error = update_rtxn_recon_date(
        mock_dbh, 12345, 67890, '01/15/2024', mock_config
    )
    
    assert rows_affected == 1
    assert error is None


def test_update_card_recon_date_mc(mocker):
    """Test updating MC card reconciliation date"""
    mock_dbh = Mock()
    mock_config = {
        'sql_queries': {
            'update_mc_recon': 'UPDATE p2p_recon_mc_zeldly SET recon_date = ? WHERE p2p_Tran_Id = ?'
        }
    }
    
    mocker.patch('p2p_manual_reconcile_tool.execute_sql', return_value=(1, None))
    
    rows_affected, error = update_card_recon_date(
        mock_dbh, 'MC', 'tran123', None, '01/15/2024', mock_config
    )
    
    assert rows_affected == 1
    assert error is None


def test_update_card_recon_date_visa(mocker):
    """Test updating VISA card reconciliation date"""
    mock_dbh = Mock()
    mock_config = {
        'sql_queries': {
            'update_visa_recon': 'UPDATE p2p_recon_visa_rw3 SET recon_date = ? WHERE Tran_Id = ? AND Tran_Code = ?'
        }
    }
    
    mocker.patch('p2p_manual_reconcile_tool.execute_sql', return_value=(1, None))
    
    rows_affected, error = update_card_recon_date(
        mock_dbh, 'VISA', 'tran456', 'CREDIT', '01/15/2024', mock_config
    )
    
    assert rows_affected == 1
    assert error is None


def test_create_output_file_path():
    """Test creation of output file path"""
    test_dir = "/test/output"
    
    with patch('datetime.date') as mock_date:
        mock_date.today.return_value.strftime.return_value = "01-15-2024"
        
        result = create_output_file_path(test_dir)
        
        expected = f"/test/output{os.sep}P2P_RECON_MANUAL_UPDATE_01-15-2024.txt"
        assert result == expected


def test_parse_recon_file_invalid_headers(mocker):
    """Test parsing file with invalid headers raises exception"""
    mock_config = {
        'valid_column_headers': ['DETAIL_RECORD_ID', 'PAYMENT_ID']
    }
    
    mock_workbook = Mock()
    mock_worksheet = Mock()
    mock_workbook.active = mock_worksheet
    mock_worksheet.max_row = 2
    
    def mock_cell(row, column):
        cell_mock = Mock()
        if row == 1 and column == 1:
            cell_mock.value = "INVALID_HEADER"
        else:
            cell_mock.value = "test_value"
        return cell_mock
    
    mock_worksheet.cell = mock_cell
    mocker.patch('openpyxl.load_workbook', return_value=mock_workbook)
    
    with pytest.raises(ValueError) as excinfo:
        parse_recon_file("test.xlsx", mock_config)
    
    assert "is not a valid column name" in str(excinfo.value)