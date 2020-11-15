"""Custom airflow operator to check data quality"""
import os
import glob
import boto3
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults

class LocalToS3Operator(BaseOperator):
    """
    Allows for local file uploads to S3.
    """

    @apply_defaults
    def __init__(
            self,
            path="",
            extention="",
            folder="",
            s3_bucket="",
            *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.path = path
        self.extention = extention
        self.folder = folder
        self.s3_bucket = s3_bucket

    def execute(self, context):

        data_dir = f"{self.path}/"
        folder = f"{self.folder}/"
        file_type = f"*.{self.extention}"
        data_path = os.path.join(data_dir, folder, file_type)
        print(data_path)
        list_of_files = glob.glob(data_path)

        s3 = boto3.resource('s3')

        for each in list_of_files:
            obj_name = each.replace(os.path.join(data_dir, folder), "")
            s3.meta.client.upload_file(each, self.s3_bucket, f"{folder}{obj_name}")
