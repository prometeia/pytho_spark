import unittest
from unittest import mock
import xml.etree.ElementTree as ET
import io
import re

from pytho_spark.constants import build_rest_message_for_oozie_job, build_oozie_xml_workflow


def _get_properties(xml_string):
    f = io.StringIO(xml_string)
    tree = ET.parse(f)
    config = {}
    for prop in tree.findall('property'):
        dontcare, name, value = list(prop.iter())
        config[name.text] = value.text
    return config


class TestConstants(unittest.TestCase):
    def test_build_rest_message_for_oozie_job(self):
        config = _get_properties(build_rest_message_for_oozie_job(namenode='hdfs://dev-hdp-01.prometeia:8020',
                                                                  jobtracker='dev-hdp-01.prometeia:8032',
                                                                  oozie_path='hdfs:///pytho/workflows/w1'))

        self.assertEqual(config['nameNode'], 'hdfs://dev-hdp-01.prometeia:8020')
        self.assertEqual(config['jobTracker'], 'dev-hdp-01.prometeia:8032')
        self.assertEqual(config['oozie.wf.application.path'], 'hdfs:///pytho/workflows/w1')

    def test_build_rest_message_for_oozie_job_default_values(self):
        mock_namenode_for_oozie = mock.patch('pytho_spark.configuration.Configuration.namenode_for_oozie',
                                             new_callable=mock.PropertyMock).start()
        mock_jobtracker_for_oozie = mock.patch('pytho_spark.configuration.Configuration.jobtracker_for_oozie',
                                               new_callable=mock.PropertyMock).start()
        mock_oozie_base_path = mock.patch('pytho_spark.configuration.Configuration.oozie_base_path',
                                          new_callable=mock.PropertyMock).start()
        mock_namenode_for_oozie.return_value = 'hdfs://dev-hdp-01.prometeia:8020'
        mock_jobtracker_for_oozie.return_value = 'dev-hdp-01.prometeia:8032'
        mock_oozie_base_path.return_value = 'hdfs:///pytho/workflows/'
        config = _get_properties(build_rest_message_for_oozie_job())

        self.assertEqual(config['nameNode'], 'hdfs://dev-hdp-01.prometeia:8020')
        self.assertEqual(config['jobTracker'], 'dev-hdp-01.prometeia:8032')
        self.assertTrue(re.match('hdfs:///pytho/workflows/.*', config['oozie.wf.application.path']))

        mock_namenode_for_oozie.close()
        mock_jobtracker_for_oozie.close()
        mock_oozie_base_path.close()

    def test_build_oozie_xml_workflow(self):
        oozie_workflow = build_oozie_xml_workflow(namenode='hdfs://dev-hdp-01.prometeia:8020',
                                                  jobtracker='dev-hdp-01.prometeia:8032',
                                                  spark_action_name='Test')

        f = io.StringIO(oozie_workflow)
        tree = ET.parse(f)

        self.assertEqual(list(tree.iter(tag='{uri:oozie:spark-action:0.1}name-node'))[0].text,
                         'hdfs://dev-hdp-01.prometeia:8020')
        self.assertEqual(list(tree.iter(tag='{uri:oozie:spark-action:0.1}job-tracker'))[0].text,
                         'dev-hdp-01.prometeia:8032')
        self.assertEqual(list(tree.iter(tag='{uri:oozie:spark-action:0.1}name'))[0].text,
                         'Test')

    def test_build_oozie_xml_workflow_default_values(self):
        mock_namenode_for_oozie = mock.patch('pytho_spark.configuration.Configuration.namenode_for_oozie',
                                             new_callable=mock.PropertyMock).start()
        mock_jobtracker_for_oozie = mock.patch('pytho_spark.configuration.Configuration.jobtracker_for_oozie',
                                               new_callable=mock.PropertyMock).start()
        mock_spark_action_name = mock.patch('pytho_spark.configuration.Configuration.spark_action_name',
                                               new_callable=mock.PropertyMock).start()

        mock_namenode_for_oozie.return_value = 'hdfs://dev-hdp-01.prometeia:8020'
        mock_jobtracker_for_oozie.return_value = 'dev-hdp-01.prometeia:8032'
        mock_spark_action_name.return_value = 'Test'

        oozie_workflow = build_oozie_xml_workflow()

        f = io.StringIO(oozie_workflow)
        tree = ET.parse(f)

        self.assertEqual(list(tree.iter(tag='{uri:oozie:spark-action:0.1}name-node'))[0].text,
                         'hdfs://dev-hdp-01.prometeia:8020')
        self.assertEqual(list(tree.iter(tag='{uri:oozie:spark-action:0.1}job-tracker'))[0].text,
                         'dev-hdp-01.prometeia:8032')
        self.assertEqual(list(tree.iter(tag='{uri:oozie:spark-action:0.1}name'))[0].text,
                         'Test')

        mock_namenode_for_oozie.stop()
        mock_jobtracker_for_oozie.stop()
        mock_spark_action_name.stop()
