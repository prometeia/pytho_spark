import os
import time

from pytho_spark.configuration import Configuration

_job_rest_message_template = """<configuration>
  <property>
    <name>nameNode</name>
    <value>{namenode}</value>
  </property>
  <property>
    <name>jobTracker</name>
    <value>{jobtracker}</value>
  </property>
  <property>
    <name>user.name</name>
    <value>hue</value>
  </property>
  <property>
    <name>oozie.wf.application.path</name>
    <value>{oozie_path}</value>
  </property>
  <property>
    <name>oozie.use.system.libpath</name>
    <value>true</value>
  </property>
</configuration>
"""

_oozie_workflow_xml = """
<workflow-app name="Pytho Oozie Workflow" xmlns="uri:oozie:workflow:0.5">
    <start to="SparkAction"/>

    <action name="SparkAction">
        <spark xmlns="uri:oozie:spark-action:0.1">
            <job-tracker>{jobtracker}</job-tracker>
            <name-node>{namenode}</name-node>
            <master>local[*]</master>
            <mode>client</mode>
            <name>{spark_action_name}</name>
            <jar>macro.py</jar>
        </spark>
        <ok to="End"/>
        <error to="Kill"/>
    </action>

    <kill name="Kill">
        <message>Action failed</message>
    </kill>
    <end name="End"/>
</workflow-app>
"""


def build_rest_message_for_oozie_job(namenode=None, jobtracker=None, oozie_path=None):
    return _job_rest_message_template.format(namenode=namenode or Configuration().namenode_for_oozie,
                                             jobtracker=jobtracker or Configuration().jobtracker_for_oozie,
                                             oozie_path=oozie_path or _create_oozie_path_for_new_workflow())


def _create_oozie_path_for_new_workflow():
    return os.path.join(Configuration().oozie_base_path, str(int(time.time())))


def build_oozie_xml_workflow(namenode=None, jobtracker=None, spark_action_name=None):
    return _oozie_workflow_xml.format(namenode=namenode or Configuration().namenode_for_oozie,
                                      jobtracker=jobtracker or Configuration().jobtracker_for_oozie,
                                      spark_action_name=spark_action_name or Configuration().spark_action_name)
