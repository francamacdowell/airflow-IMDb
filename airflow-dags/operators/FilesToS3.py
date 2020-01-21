# airflow related
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
# other packages
from datetime import datetime, timedelta
from os import listdir
from utils import s3_API

class FilesToS3Operator(BaseOperator):
    """
    Extract data from dir data source to S3
    """
@apply_defaults
def __init__(self, bucket_name, dir_path, s3_object_name=None, 
            *args, **kwargs):

    super(FilesToS3Operator, self).__init__(*args, **kwargs)
    self.bucket_name = bucket_name
    self.dir_path = dir_path
    self.s3_object_name =  object_name

def __datasource_to_s3(self, execution_date):
    file_name_list = os.listdir(self.dir_path)

    for filename in file_name_list:
        s3_API.upload_file(self.dir_path + '/' + filename, self.bucket_name)

def execute(self, context):
    self.__datasource_to_s3(self, execution_date)