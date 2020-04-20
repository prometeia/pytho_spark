import datetime
from pytho_spark import hadoop_communication, constants
from pytho_spark.configuration import Configuration
import os


def submit_pyspark_code(code: str):
    oozie_workflow_xml = constants.build_oozie_xml_workflow()

    new_workflow_hdfs_path = os.path.join(Configuration().oozie_base_path,
                                          datetime.datetime.now().strftime('%Y%m%d%H%M%S'))

    hadoop_communication.hdfs_mkdir(new_workflow_hdfs_path)
    hadoop_communication.hdfs_mkdir(new_workflow_hdfs_path + '/lib')
    hadoop_communication.create_file(new_workflow_hdfs_path + '/workflow.xml', oozie_workflow_xml)
    hadoop_communication.create_file(new_workflow_hdfs_path + '/lib/macro.py', code)
    hadoop_communication.submit_oozie_workflow(new_workflow_hdfs_path)
