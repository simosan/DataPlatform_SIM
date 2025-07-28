import pytest
import basedatetimeupdate


def test_success(mocker):
    ssm_mock = mocker.Mock()
    ssm_mock.get_parameter.return_value = {
        'Parameter': {'Value': 'test-bucket'}
    }

    test_csv = "base\n2025-07-25"
    body_mock = mocker.Mock()
    body_mock.read.return_value = test_csv.encode('utf-8')
    s3_mock = mocker.Mock()
    s3_mock.get_object.return_value = {'Body': body_mock}
    s3_mock.put_object.return_value = {}

    def client_side_effect(service):
        if service == 'ssm':
            return ssm_mock
        elif service == 's3':
            return s3_mock
        else:
            return mocker.Mock()
    mocker.patch('boto3.client', side_effect=client_side_effect)

    result = basedatetimeupdate.basedatetimeupdate({}, {})

    assert result['statusCode'] == 200
    assert result['message'] == 'success'
    assert s3_mock.put_object.called


def test_ssm_error(mocker):
    ssm_mock = mocker.Mock()
    ssm_mock.get_parameter.side_effect = Exception("SSM error")
    mocker.patch('boto3.client', return_value=ssm_mock)
    result = basedatetimeupdate.basedatetimeupdate({}, {})

    assert result['statusCode'] == 500
    assert "false!" in result['message']

def test_s3_read_error(mocker):
    ssm_mock = mocker.Mock()
    ssm_mock.get_parameter.return_value = {
        'Parameter': {'Value': 'test-bucket'}
    }
    s3_mock = mocker.Mock()
    s3_mock.get_object.side_effect = Exception("S3 read error")

    def client_side_effect(service):
        if service == 'ssm':
            return ssm_mock
        elif service == 's3':
            return s3_mock
        else:
            return mocker.Mock()
    mocker.patch('boto3.client', side_effect=client_side_effect)
    result = basedatetimeupdate.basedatetimeupdate({}, {})

    assert result['statusCode'] == 500
    assert "false!" in result['message']

def test_s3_write_error(mocker):
    ssm_mock = mocker.Mock()
    ssm_mock.get_parameter.return_value = {
        'Parameter': {'Value': 'test-bucket'}
    }
    s3_mock = mocker.Mock()
    test_csv = "base\n2025-07-25"
    body_mock = mocker.Mock()
    body_mock.read.return_value = test_csv.encode('utf-8')
    s3_mock.get_object.return_value = {'Body': body_mock}
    s3_mock.put_object.side_effect = Exception("S3 write error")
    
    def client_side_effect(service):
        if service == 'ssm':
            return ssm_mock
        elif service == 's3':
            return s3_mock
        else:
            return mocker.Mock()
    mocker.patch('boto3.client', side_effect=client_side_effect)
    result = basedatetimeupdate.basedatetimeupdate({}, {})

    assert result['statusCode'] == 500
    assert "false!" in result['message']