import pytest
from unittest.mock import Mock, patch, mock_open
from rtxn_description_cleaner import (
    run, get_data_ext, get_data_int, clean_data, clean_bad_chars, 
    unidecode_text, update_desc_ext, update_desc_int, write_report,
    ScriptData, AppWorxEnum
)


def test_run(apwx):
    """Using the `apwx` fixture from conftest.py, execute the run function"""
    with patch('rtxn_description_cleaner.initialize') as mock_init, \
         patch('rtxn_description_cleaner.get_data_ext') as mock_get_ext, \
         patch('rtxn_description_cleaner.get_data_int') as mock_get_int, \
         patch('rtxn_description_cleaner.clean_data') as mock_clean, \
         patch('rtxn_description_cleaner.update_desc_ext') as mock_update_ext, \
         patch('rtxn_description_cleaner.update_desc_int') as mock_update_int, \
         patch('rtxn_description_cleaner.write_report') as mock_report, \
         patch('rtxn_description_cleaner.log') as mock_log:
        
        # Setup mock data
        mock_script_data = Mock()
        mock_script_data.apwx.args.FULL_CLEAN_YN = 'N'
        mock_script_data.apwx.args.DAYS_BACK = 3
        mock_init.return_value = mock_script_data
        
        mock_get_ext.return_value = []
        mock_get_int.return_value = []
        
        result = run(apwx)
        
        assert result is True
        mock_init.assert_called_once_with(apwx)
        mock_clean.assert_called_once()
        mock_update_ext.assert_called_once()
        mock_update_int.assert_called_once()
        mock_report.assert_called_once()


def test_get_data_ext_full_clean(mocker):
    """Test getting external data with full clean"""
    mock_dbh = Mock()
    mock_config = {
        'queries': {
            'get_ext_full': 'SELECT * FROM extrtxndesc'
        }
    }
    expected_data = [{'EXTRTXNDESCNBR': 1, 'EXTRTXNDESCTEXT': 'Test Description'}]
    
    mocker.patch(
        'rtxn_description_cleaner.execute_sql',
        return_value=expected_data
    )
    
    result = get_data_ext(mock_dbh, mock_config, full_clean=True)
    
    assert result == expected_data


def test_get_data_ext_incremental(mocker):
    """Test getting external data with incremental clean"""
    mock_dbh = Mock()
    mock_config = {
        'queries': {
            'get_ext_incremental': 'SELECT * FROM extrtxndesc WHERE datelastmaint > sysdate - :days_back'
        }
    }
    expected_data = [{'EXTRTXNDESCNBR': 1, 'EXTRTXNDESCTEXT': 'Test Description'}]
    
    mocker.patch(
        'rtxn_description_cleaner.execute_sql',
        return_value=expected_data
    )
    
    result = get_data_ext(mock_dbh, mock_config, full_clean=False, days_back=5)
    
    assert result == expected_data


def test_get_data_int_full_clean(mocker):
    """Test getting internal data with full clean"""
    mock_dbh = Mock()
    mock_config = {
        'queries': {
            'get_int_full': 'SELECT * FROM intrtxndesc'
        }
    }
    expected_data = [{'INTRTXNDESCNBR': 1, 'INTRTXNDESCTEXT': 'Test Description'}]
    
    mocker.patch(
        'rtxn_description_cleaner.execute_sql',
        return_value=expected_data
    )
    
    result = get_data_int(mock_dbh, mock_config, full_clean=True)
    
    assert result == expected_data


def test_clean_bad_chars():
    """Test cleaning bad characters from text"""
    # Test with non-ASCII and non-printable characters
    test_text = "Hello\x00World\x7f\x80\x9fTest"
    expected = "HelloWorldTest"
    
    result = clean_bad_chars(test_text)
    
    assert result == expected


def test_clean_bad_chars_with_unicode():
    """Test cleaning bad characters with Unicode"""
    test_text = "Café\x00\x01\x02"
    expected = "Café"
    
    result = clean_bad_chars(test_text)
    
    # Should remove non-printable but keep printable Unicode
    assert len(result) > 0
    assert '\x00' not in result
    assert '\x01' not in result
    assert '\x02' not in result


def test_unidecode_text():
    """Test Unicode to ASCII conversion"""
    test_text = "Café résumé naïve"
    
    result = unidecode_text(test_text)
    
    # Should convert accented characters to ASCII equivalents
    assert 'é' not in result
    assert 'ï' not in result
    assert len(result) > 0


def test_clean_data():
    """Test cleaning data function"""
    mock_script_data = Mock()
    mock_script_data.data_ext = [
        {'EXTRTXNDESCTEXT': '  Test Description\x00  '}
    ]
    mock_script_data.data_int = [
        {'INTRTXNDESCTEXT': '  Internal Desc\x01  '}
    ]
    
    clean_data(mock_script_data)
    
    # Check that old values are preserved
    assert 'EXTRTXNDESCTEXT_OLD' in mock_script_data.data_ext[0]
    assert 'INTRTXNDESCTEXT_OLD' in mock_script_data.data_int[0]
    
    # Check that new values are created
    assert 'EXTRTXNDESCTEXT_NEW' in mock_script_data.data_ext[0]
    assert 'INTRTXNDESCTEXT_NEW' in mock_script_data.data_int[0]


def test_update_desc_ext(mocker):
    """Test updating external descriptions"""
    mock_script_data = Mock()
    mock_script_data.config = {
        'queries': {
            'update_ext': 'UPDATE extrtxndesc SET extrtxndesctext = :new_text WHERE extrtxndescnbr = :desc_nbr'
        }
    }
    mock_script_data.data_ext = [
        {
            'EXTRTXNDESCNBR': 1,
            'EXTRTXNDESCTEXT_NEW': 'Cleaned Text'
        }
    ]
    
    mock_post_trans = mocker.patch('rtxn_description_cleaner.post_trans')
    
    update_desc_ext(mock_script_data)
    
    mock_post_trans.assert_called_once()


def test_update_desc_int(mocker):
    """Test updating internal descriptions"""
    mock_script_data = Mock()
    mock_script_data.config = {
        'queries': {
            'update_int': 'UPDATE intrtxndesc SET intrtxndesctext = :new_text WHERE intrtxndescnbr = :desc_nbr'
        }
    }
    mock_script_data.data_int = [
        {
            'INTRTXNDESCNBR': 1,
            'INTRTXNDESCTEXT_NEW': 'Cleaned Text'
        }
    ]
    
    mock_post_trans = mocker.patch('rtxn_description_cleaner.post_trans')
    
    update_desc_int(mock_script_data)
    
    mock_post_trans.assert_called_once()


def test_write_report(mocker):
    """Test writing audit report"""
    mock_script_data = Mock()
    mock_script_data.output_file = 'test_output.csv'
    mock_script_data.apwx.args.RPT_ONLY_YN = 'Y'
    mock_script_data.data_ext = [
        {
            'EXTRTXNDESCNBR': 1,
            'EXTRTXNDESCTEXT_OLD': 'Old Text',
            'EXTRTXNDESCTEXT_NEW': 'New Text'
        }
    ]
    mock_script_data.data_int = [
        {
            'INTRTXNDESCNBR': 2,
            'INTRTXNDESCTEXT_OLD': 'Old Internal',
            'INTRTXNDESCTEXT_NEW': 'New Internal'
        }
    ]
    
    mock_file = mock_open()
    
    with patch('builtins.open', mock_file):
        write_report(mock_script_data)
    
    mock_file.assert_called_once_with('test_output.csv', 'w', newline='', encoding='utf-8')
    
    # Check that file was written to
    handle = mock_file()
    assert handle.write.called


def test_write_report_empty_data(mocker):
    """Test writing audit report with empty data"""
    mock_script_data = Mock()
    mock_script_data.output_file = 'test_output.csv'
    mock_script_data.apwx.args.RPT_ONLY_YN = 'N'
    mock_script_data.data_ext = []
    mock_script_data.data_int = []
    
    mock_file = mock_open()
    
    with patch('builtins.open', mock_file):
        write_report(mock_script_data)
    
    mock_file.assert_called_once()


def test_execute_sql_select_with_params(mocker):
    """Test execute_sql with SELECT and parameters"""
    from rtxn_description_cleaner import execute_sql
    
    mock_conn = Mock()
    mock_cursor = Mock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mock_cursor.description = [('COLUMN1',), ('COLUMN2',)]
    mock_cursor.fetchall.return_value = [('value1', 'value2')]
    
    sql = "SELECT * FROM table WHERE id = :id"
    params = {'id': 1}
    
    result = execute_sql(mock_conn, sql, 'SELECT', params)
    
    mock_cursor.execute.assert_called_once_with(sql, params)
    assert len(result) == 1


def test_execute_sql_select_no_params(mocker):
    """Test execute_sql with SELECT and no parameters"""
    from rtxn_description_cleaner import execute_sql
    
    mock_conn = Mock()
    mock_cursor = Mock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mock_cursor.description = [('COLUMN1',), ('COLUMN2',)]
    mock_cursor.fetchall.return_value = [('value1', 'value2')]
    
    sql = "SELECT * FROM table"
    
    result = execute_sql(mock_conn, sql, 'SELECT')
    
    mock_cursor.execute.assert_called_once_with(sql)
    assert len(result) == 1


def test_execute_sql_update(mocker):
    """Test execute_sql with UPDATE"""
    from rtxn_description_cleaner import execute_sql
    
    mock_conn = Mock()
    mock_cursor = Mock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mock_cursor.getbatcherrors.return_value = []
    
    sql = "UPDATE table SET col = :val WHERE id = :id"
    params = [{'val': 'new_value', 'id': 1}]
    
    execute_sql(mock_conn, sql, 'UPDATE', params)
    
    mock_cursor.executemany.assert_called_once_with(sql, params, batcherrors=True)


def test_execute_sql_update_with_errors(mocker):
    """Test execute_sql with UPDATE that has batch errors"""
    from rtxn_description_cleaner import execute_sql
    
    mock_conn = Mock()
    mock_cursor = Mock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    
    # Mock batch error
    mock_error = Mock()
    mock_error.offset = 0
    mock_error.message = "Test error"
    mock_cursor.getbatcherrors.return_value = [mock_error]
    
    sql = "UPDATE table SET col = :val WHERE id = :id"
    params = [{'val': 'new_value', 'id': 1}]
    
    with pytest.raises(Exception) as exc_info:
        execute_sql(mock_conn, sql, 'UPDATE', params)
    
    assert "One or more errors occurred during the UPDATE process" in str(exc_info.value)


def test_execute_sql_exception():
    """Test execute_sql with database exception"""
    from rtxn_description_cleaner import execute_sql
    
    mock_conn = Mock()
    mock_conn.cursor.side_effect = Exception("Database connection error")
    
    sql = "SELECT * FROM table"
    
    with pytest.raises(Exception) as exc_info:
        execute_sql(mock_conn, sql, 'SELECT')
    
    assert "SQL error" in str(exc_info.value)