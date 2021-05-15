"""
Ruigi's Pipeline
================

This module defines a framework for building and managing data pipelines. It was
inspired in Luigi's architecture and has it as a backend for the
implementation of the pipeline execution.

In submodule targets, luigi targets classes are implemented. Some of these
targets, make use of Carol Data Storage and are a key feature for running the
pipeline in the cloud.

In submodule tasks, we found Ruigi's extension to luigi.Task class. This new
class, combined with some decorators, allows a more efficient description of
the pipeline. It is also in this submodule, that we implement the notebook
task feature, which allows to write task definitions directly in jupyter
notebooks.

In submodule tools, we have tools to interact with the pipeline as a whole.
This inserts an abstraction layer over luigi and provides some features that
are not supported in luigi.

An app pipeline is mainly described using classes from submodules tasks and
targets. Finally, the top pipeline tasks together with the pipeline
parameters definitions are used to instantiate an object of class Pipe,
from submodule tools. This object should be used to make all interactions
with the pipeline, like running, removing targets and related actions.

Finally, submodule viewer implements a dash web app to visualize an object Pipe
and, thus, visualize a given pipeline. This web app has also a jupyter
version to be used inside jupyter lab.

Example
-------

This is a very simplified case when we will create a task that will fetch data from carol in one task and process in
a second task.

.. code:: python

    import pandas as pd
    from ruigi import inherit_list, Task
    import luigi

    @inherit_list(
    )
    class Task1(Task):

        range_values = luigi.IntParameter()

        def easy_run(self, inputs):
            d = {'col1': list(range(self.range_values)), 'col2': list(range(self.range_values))}
            df = pd.DataFrame(data=d)
            return df


    @inherit_list(
        Task1,
    )
    class DataProcess(Task):

        def easy_run(self, inputs):
            df = inputs[0] #since there is only one requirement.
            df['new_col'] = df['col1']  * df['col2'] 
            return df

    task = [DataProcess(range_values=3)]
    luigi.build(task, local_scheduler=True)

Notice that we can start the pipeline with luigi.build.
If  a single task needs to be build, one can call, from the example above,

.. code:: python

    task = [DataProcess(range_values=3)]
    task.buildme(local_scheduler=True,)

It will have exactly the same behavior.

"""
__version__ = "0.0.1"

from luigi import (
    Parameter, OptionalParameter, DateParameter, MonthParameter, YearParameter, 
    DateHourParameter, DateMinuteParameter, DateSecondParameter, IntParameter,
    FloatParameter, BoolParameter, DateIntervalParameter, TimeDeltaParameter, TaskParameter,
    EnumParameter, DictParameter, ListParameter, TupleParameter, NumericalParameter,
    ChoiceParameter,     
    )

from .task import (
    Task,
    WrapperTask,
    inherit_list,
    inherit_dict
)

from .targets import (
    CloudTarget,
    PickleTarget,
    KerasTarget,
    DummyTarget,
    JsonTarget,
    PytorchTarget,
    ParquetTarget,
    FileTarget,
    LocalTarget
)

from .tools import (
    Pipe,
)
