from ruigi import Task, inherit_list, Parameter
from ruigi.backends import StorageGCS

bucket_name='my-bucket'
parent_folder= 'folder-to-save-in-bucket'
project = 'my-project'
service_account_path='creds.json'
Task.cloud_target_backend = StorageGCS(
    project=project, bucket_name=bucket_name, parent_folder=parent_folder, 
    service_account_path=service_account_path, )


class MyTaskWithParameters(Task):
    a = Parameter(default='nothing')

    def easy_run(self, inputs):
        return f"My Task now prints {self.a}"


@inherit_list((MyTaskWithParameters, dict(a='SOMETHING')),
              (MyTaskWithParameters, dict(a='SOMETHINGS')),
              (MyTaskWithParameters, dict(a='A LOT OF SOMETHINGS'))
              )
class InterestingPipeline(Task):
    def easy_run(self, inputs):
        for i in inputs:
            print(i)
        return 'Done!'


InterestingPipeline().buildme(local_scheduler=True)
