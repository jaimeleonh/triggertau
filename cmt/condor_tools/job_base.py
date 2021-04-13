from law.job.base import JobArguments


class JobArgumentsExt(JobArguments):

    def __init__(self, task_cls, task_params, branches, auto_retry=False, dashboard_data=None,
            args=[]):
        super(JobArgumentsExt, self).__init__(task_cls, task_params, branches, auto_retry=False,
            dashboard_data=None)

        self.additional_args = args

    def get_args(self):
        """
        Returns the list of encoded job arguments. The order of this list corresponds to the
        arguments expected by the job wrapper script.
        """
        return [
            self.task_cls.__module__,
            self.task_cls.__name__,
            self.encode_string(self.task_params),
            self.encode_list(self.branches),
            self.encode_bool(self.auto_retry),
            self.encode_list(self.dashboard_data),
        ] + self.additional_args 