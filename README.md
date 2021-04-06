# Ruigi

Ruiji in japanese means similar: Ruigi ~ Luigi

The name encapsulates the essence of this package, being very similar to Luigi, but not exactly.

A pipeline that is:
1. Very easy to start with
2. Easy to configure and reuse recipes
3. Makes every programmer a Data Pipeline pro

Just like keras was made for tensorflow, ruigi was made for luigi. (if you don't know what that is, never mind, keep scrolling)

## Installation

```
pip install ruigi
```

## Quickstart

Pipelines are made of tasks that must be executed in a specific order.

1. Create your first Task
```
from ruigi import Task

class MyFirstAwesomeTask(Task):
    def easy_run(self, inputs):
        return "First Hello World!"
```

2. Create a Task with a dependency of another Task
```
@inherit_list(MyFirstAwesomeTask)
class MySecondAwesomeTask(Task):
    def easy_run(self, inputs):
        print(f'I got the {inputs[0]}')
        return "Hello World, again!"
```

3. Build and run your pipeline
```
MySecondAwesomeTask().buildme()
```

4. Add some parameters
```
from ruigi import Parameter

class MyTaskWithParameters(Task):
    a = Parameter(default='nothing')

    def easy_run(self, inputs):
        return f"My Task now prints {self.a}"
```

5. Create some dynamic pipelines
```
@inherit_list((MyTaskWithParameters, dict(a='SOMETHING')),
              (MyTaskWithParameters, dict(a='SOMETHINGS')),
              (MyTaskWithParameters, dict(a='A LOT OF SOMETHINGS'))
)
class InterestingPipeline(Task):
    def easy_run(self, inputs):
        for i in inputs:
            print(i)
        return 'Done!'
```

6. Stop playing around and start working with data
```
from ruigi.targets import ParquetTarget
import pandas as pd

class InterestingDataTask(Task):
    target_type = ParquetTarget # Wow, is that easy to change how the return from easy_run should be saved and loaded?

    def easy_run(self, inputs):
        df = pd.DataFrame()
        # Do Data stuff here
        return df
```

Read our [docs]() for a lot of examples of more interesting things you can do.

### Summary: 
With ruigi's Task, you can create tasks of a pipeline.  
With ruigi's Target, you can define how the outputs of these tasks are saved (and loaded later).  
With ruigi's Parameter, you can make your pipeline dynamic.

## Become a PRO

Never think again of pipelines, learn how to program them naturally with ruigi.

1. Understand the building blocks (read our [docs](), it is very cool)
2. Get familiar with his father Luigi (read their [docs](https://luigi.readthedocs.io/en/stable/), it is very friendly)
3. Create and share pipelines, or targets. Also, share the good news to the world and let us know.*

* If your post reaches a lot of people, we'll give you some small gift. If it doesn't, we might give as well.
Create a pull request with the name "I_DID_IT" with your post and how we can contact you (like a linkedin profile or st.).

## Why not use Airflow or other pipeline tool?

There are only two types of people in the world: those who create pipelines with ruigi
and those that don't. We know who have fun programming pipelines.

Just kidding, we don't believe in bulletproofs tools, there are always pros and cons,
but at least give it a try yourself using ruigi in one of your projects. There are some
cases where Airflow + Ruigi makes sense as well.


## Why not use pure Luigi?
No one developing with keras will say not to use tensorflow. We are built on top of luigi, so you can
always use luigi when you feel to. It's just easier with ruigi.


## Contributing
Life is not always rainbows and butterflies. Help us deal with this fact.
Pull Requests are more than welcome. Just get familiar with [standards]() before doing so,
it will help A LOT. Let's build this together :)
