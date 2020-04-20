import unittest
from unittest.mock import patch, PropertyMock
from unittest import mock

from pytho_spark.hadoop_communication import hdfs_mkdir


class TestHadoopCommunication(unittest.TestCase):
    @patch('pytho_spark.hadoop_communication.requests.put')
    def test_hdfs_mkdir(self, mock_put):
        with mock.patch('pytho_spark.configuration.Configuration.namenode_dns',
                        new_callable=PropertyMock) as mock_namenode:
            mock_put.return_value.ok = True
            mock_namenode.return_value = 'hadoop-01-master'
            response = hdfs_mkdir('/pytho/workflows/new_workflow')
            self.assertTrue(response.ok)

    @patch('pytho_spark.hadoop_communication.requests.put')
    def test_create_file(self, mock_put):
        with mock.patch('pytho_spark.configuration.Configuration.namenode_dns',
                        new_callable=PropertyMock) as mock_namenode:
            mock_put.return_value.ok = True
            mock_namenode.return_value = 'hadoop-01-master'
            response = hdfs_mkdir('/pytho/workflows/new_workflow')
            self.assertTrue(response.ok)
