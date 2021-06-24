from ruigi import Task, inherit_list, Parameter
from ruigi.storage.aws import S3Storage
import os

bucket_name = 'my-bucket'
parent_folder = 'folder-to-save-in-bucket'

aws_access_key_id = os.environ.get('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY')

# This will define this Storage for all tasks.
Task._storage = S3Storage(bucket_name=bucket_name, aws_access_key_id=aws_access_key_id,
                          aws_secret_access_key=aws_secret_access_key, parent_folder=parent_folder)


class MyTaskWithParameters(Task):
    a = Parameter(default='nothing')

    def easy_run(self, inputs):
        return f"My Task now prints {self.a}"


@inherit_list(
    (MyTaskWithParameters, dict(a='SOMETHING')),
    (MyTaskWithParameters, dict(a='SOMETHINGS')),
    (MyTaskWithParameters, dict(a='A LOT OF SOMETHINGS'))
)
class InterestingPipeline(Task):
    def easy_run(self, inputs):
        for i in inputs:
            print(i)
        return 'Done!'


InterestingPipeline().buildme(local_scheduler=True)
