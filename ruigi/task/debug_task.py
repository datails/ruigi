from luigi import LuigiStatusCode, interface


def task_execution_debug(task, parameters: dict = None, worker_scheduler_factory=None, **env_params):
    """ Executes a pipeline and store execution information for debugging purposes
    
    :param task: Task Class to be executed
    :param parameters: Parameters to be used in the Task
    
    :return: dict with the following items:
            success: bool - Checks if the execution succeeded or not,
            worker: worker object,
            task: instance task
            task_history: pipeline's worker _add_task_history output
            history_has(task, status, ignore_parameters=False): whether task history has or not that task and status.
                Obs. If you want to ignore parameter, make sure task is a class and not an instance.
    """
    if parameters is None:
        parameters = {}

    if "no_lock" not in env_params:
        env_params["no_lock"] = True

    if "local_scheduler" not in env_params:
        env_params["local_scheduler"] = True

    out = dict()
    # TODO Get only parameters that are used in task_instance. Similar to self.clone
    task_instance = task(**parameters)
    out['task'] = task_instance
    exec_out = interface._schedule_and_run([task_instance], worker_scheduler_factory,
                                                 override_defaults=env_params)
    # TODO: Check luigi version
    # if luigi.__version__
    out.update({'success': exec_out.status == LuigiStatusCode.SUCCESS})
    task_history = exec_out.worker._add_task_history
    out.update({'task_history': task_history})

    # TODO Get execution stacktrace
    return out


def history_has(task_history, task, status=None, ignore_parameters=True):
    """ Check if execution history contains a specific task and, optionally, with a specific status

    E.g.:
        history_has(MyTask, LuigiStatusCode.SUCCESS)

    Args:
        task: Task Instance
        status: LuigiStatusCode
        ignore_parameters:

    Returns:

    """
    if not ignore_parameters:
        for t, s, _ in task_history:
            if task == t and status == s:
                return True
        return False
    else:
        for t, s, _ in task_history:
            if task.__name__ == t.__class__.__name__ and status == s:
                return True
        return False