import os

OOZIE_BASE_PATH = 'PYTHO_HADOOP_OOZIE_BASE_PATH'
SPARK_ACTION_NAME = 'PYTHO_HADOOP_SPARK_ACTION_NAME'
JOBTRACKER_FOR_OOZIE = 'PYTHO_HADOOP_JOBTRACKER'
NAMENODE_FOR_OOZIE = 'PYTHO_HADOOP_NAMENODE'
NAMENODE_DNS = 'PYTHO_HADOOP_NAMENODE_DNS'
OOZIE_MASTER_DNS = 'PYTHO_HADOOP_OOZIE_MASTER_DNS'


class Configuration:
    @property
    def namenode_for_oozie(self):
        return os.environ[NAMENODE_FOR_OOZIE]

    @property
    def jobtracker_for_oozie(self):
        return os.environ[JOBTRACKER_FOR_OOZIE]

    @property
    def oozie_base_path(self):
        return os.environ[OOZIE_BASE_PATH]

    @property
    def spark_action_name(self):
        return os.environ[SPARK_ACTION_NAME]

    @property
    def namenode_dns(self):
        return os.environ[NAMENODE_DNS]

    @property
    def oozie_master_dns(self):
        return os.environ[OOZIE_MASTER_DNS]

