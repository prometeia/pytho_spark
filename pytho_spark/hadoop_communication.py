import requests
import os
import uuid
from pytho_spark.configuration import Configuration
from pytho_spark.constants import build_rest_message_for_oozie_job


def hdfs_mkdir(folder_path, config=Configuration()):
    params = (
        ('op', 'MKDIRS'),
        ('noredirect', 'true'),
    )
    response = requests.put(f'http://{config.namenode_dns}:9870/webhdfs/v1' + folder_path,
                            params=params)
    return response


def create_file(hdfs_file_path, file_content, config=Configuration()):
    params = (
        ('op', 'CREATE'),
        ('noredirect', 'true'),
    )
    response = send_first_create_file_request(config, hdfs_file_path, params)
    url_datanode = response.json()['Location']

    tmp_local_file = '.' + uuid.uuid4().hex
    with open(tmp_local_file, 'w') as f:
        f.write(file_content)
    response = send_second_create_file_request(params, tmp_local_file, url_datanode)
    os.remove(tmp_local_file)

    return response


def send_second_create_file_request(params, tmp_local_file, url_datanode):
    return requests.put(url_datanode, params=params, data=open(tmp_local_file), timeout=3)


def send_first_create_file_request(config, hdfs_file_path, params):
    return requests.put(f'http://{config.namenode_dns}:9870/webhdfs/v1' + hdfs_file_path, params=params)


def submit_oozie_workflow(oozie_workflow_path=None, config=Configuration()):

    headers = {
        'Content-Type': 'application/xml;charset=UTF-8',
    }

    params = (
        ('action', 'start'),
    )

    tmp_local_file = '.' + uuid.uuid4().hex
    with open(tmp_local_file, 'w') as f:
        f.write(build_rest_message_for_oozie_job(oozie_workflow_path))
    response = send_oozie_request(config, headers, params, tmp_local_file)
    os.remove(tmp_local_file)

    return response


def send_oozie_request(config, headers, params, tmp_local_file):
    return requests.post(f'http://{config.oozie_master_dns}:11000/oozie/v2/jobs', headers=headers, params=params,
                         data=open(tmp_local_file))
