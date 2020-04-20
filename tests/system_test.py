from pytho_spark.workflow_submitter import submit_pyspark_code
import os


def main():
    os.environ['PYTHO_HADOOP_OOZIE_BASE_PATH'] = '/pytho/workflows/'
    os.environ['PYTHO_HADOOP_SPARK_ACTION_NAME'] = 'macro.py'
    os.environ['PYTHO_HADOOP_JOBTRACKER'] = 'dev-hdp-01.prometeia:8032'
    os.environ['PYTHO_HADOOP_NAMENODE'] = 'hdfs://dev-hdp-01.prometeia:8020'
    os.environ['PYTHO_HADOOP_NAMENODE_DNS'] = 'dev-hdp-01'
    os.environ['PYTHO_HADOOP_OOZIE_MASTER_DNS'] = 'dev-hdp-01'

    with open('./resources/macro.py') as f:
        code = f.read()
    submit_pyspark_code(code)


if __name__ == '__main__':
    main()
