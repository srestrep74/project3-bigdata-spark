import pandas as pd
from sodapy import Socrata
import boto3


MyAppToken = "######################"


client = Socrata("www.datos.gov.co",
                  MyAppToken,
                  username="######################",
                  password="######################")


results = client.get("gt2j-8ykr", limit=1000000)
results_df = pd.DataFrame.from_records(results)


credentials = {
    'key': '######################',
    'secret': '######################',
    'token': '######################'
}

session = boto3.Session(
    aws_access_key_id='######################',
    aws_secret_access_key='######################',
    aws_session_token='######################',
    region_name='us-east-1'
)
client = session.client('emr')
results_df.to_csv('s3://proyecto3-covid/RAW/covid_data.csv', storage_options=credentials, index=False)

response = client.run_job_flow(
    Name="2024-II",
    ReleaseLabel='emr-7.3.0',
    LogUri='s3://proyecto3-covid/EMR/logs/',
    Instances={
        'KeepJobFlowAliveWhenNoSteps': False,
        'TerminationProtected': False,
        'InstanceGroups': [
            {
                'Name': 'Master',
                'Market': 'ON_DEMAND',
                'InstanceRole': 'MASTER',
                'InstanceType': 'm5.xlarge',
                'InstanceCount': 1
            },
            {
                'Name': 'Core',
                'Market': 'ON_DEMAND',
                'InstanceRole': 'CORE',
                'InstanceType': 'm5.xlarge',
                'InstanceCount': 2
            },
            {
                'Name': 'Task',
                'Market': 'ON_DEMAND',
                'InstanceRole': 'TASK',
                'InstanceType': 'm5.xlarge',
                'InstanceCount': 1
            }
        ],
        'Ec2KeyName': 'emr-cluster'
    },
    Applications=[
        {'Name': 'Spark'},
        {'Name': 'Hadoop'},
        {'Name': 'Hive'},
        {'Name': 'JupyterHub'},
        {'Name': 'Livy'},
        {'Name': 'Zeppelin'},
        {'Name': 'TensorFlow'},
        {'Name': 'Hue'},
        {'Name': 'Tez'},
        {'Name': 'Zookeeper'}
    ],
    Steps=[
        {
            'Name': 'Install Dependencies',
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': [
                    'pip3', 'install', 'pandas', 'sodapy', 'boto3', 's3fs', 'fsspec', 'mysql-connector-python'
                ]
            }
        },
        {
            'Name': 'Load DB',
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': [
                    'bash', '-c', 
                    'aws s3 cp s3://proyecto3-covid/Scripts/load_s3.py /home/hadoop/load_s3.py && python3 /home/hadoop/load_s3.py'
                ]
            }
        },
        {
            'Name': 'ETL',
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': [
                    'bash', '-c', 
                    'aws s3 cp s3://proyecto3-covid/Scripts/ETL.py /home/hadoop/ETL.py && spark-submit /home/hadoop/ETL.py'
                ]
            }
        },
        {
            'Name': 'DF',
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': [
                    'bash', '-c', 
                    'aws s3 cp s3://proyecto3-covid/Scripts/dataframes.py /home/hadoop/dataframes.py && spark-submit /home/hadoop/dataframes.py'
                ]
            }
        },
        {
            'Name': 'SparkSQL',
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': [
                    'bash', '-c', 
                    'aws s3 cp s3://proyecto3-covid/Scripts/Sparksql.py /home/hadoop/Sparksql.py && spark-submit /home/hadoop/Sparksql.py'
                ]
            }
        },
        {
            'Name': 'Crawler',
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': [
                    'bash', '-c', 
                    'aws s3 cp s3://proyecto3-covid/Scripts/Crawler.py /home/hadoop/Crawler.py && python /home/hadoop/Crawler.py'
                ]
            }
        },
    ],
    VisibleToAllUsers=True,
    ServiceRole='EMR_DefaultRole',
    JobFlowRole='EMR_EC2_DefaultRole',
    AutoScalingRole='EMR_AutoScaling_DefaultRole'
)

print("Cluster creado con éxito:", response['JobFlowId'])