import os
from urllib import parse


class SyncConfig:
    # API账户，非真实账户
    # APIKey，非真实Key。请妥善保管，不能告知他人。
    api_user = os.environ.get('API_USER')
    api_key = os.environ.get('API_KEY')
    use_url = os.environ.get('API_URL')
    run_check = use_url + "/api/das/runinfo/add/"
    run_update = use_url + "/api/das/runinfo/status/"
    seqsample = use_url + "/api/das/seqsample/"
    fastq_path = ''  # store fastq file
    machine_dict = {"MN00302": 3, "M04840": 2}  # 测序仪字典
    request_result_path = os.path.join(os.path.dirname(__file__), 'static')
    synchead = {}


def forSyncAPI(request_data, data_sign, api_user):
    json = {
        'request_data': parse.quote(request_data),
        'data_sign': parse.quote(data_sign),
        'api_user': api_user
    }
    return json