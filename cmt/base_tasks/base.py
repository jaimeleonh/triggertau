# coding: utf-8

"""
Base tasks.
"""

__all__ = [
    "Task", "ConfigTask", "ConfigTaskWithCategory", "DatasetTask", "DatasetTaskWithCategory",
    "DatasetWrapperTask", "HTCondorWorkflow", "InputData",
]


import os
import math
from collections import OrderedDict

import luigi
import law

from law.util import merge_dicts
from law.contrib.htcondor.job import HTCondorJobFileFactory
from cmt.condor_tools.htcondor import HTCondorWorkflowExt

law.contrib.load("cms", "git", "htcondor", "root", "tasks", "telegram", "tensorflow", "wlcg")


class Task(law.Task):

    version = luigi.Parameter(description="version of outputs to produce")
    notify = law.telegram.NotifyTelegramParameter()

    default_store = "$CMT_STORE"
    default_wlcg_fs = "wlcg_fs"

    # law related configs
    exclude_params_req = {"notify"}
    exclude_params_branch = {"notify"}
    exclude_params_workflow = {"notify"}
    exclude_params_repr = {"notify"}
    output_collection_cls = law.SiblingFileCollection
    workflow_run_decorators = [law.decorator.notify]
    local_workflow_require_branches = False
    message_cache_size = 20

    @classmethod
    def vreq(cls, inst, **kwargs):
        # try to insert cls version when not already set
        if "version" not in kwargs:
            config = getattr(inst, "config", None)
            if config:
                version = config.versions.get(cls.task_family)
                if version:
                    kwargs["version"] = version

        # ensure that values from the cli always have precedence
        _prefer_cli = law.util.make_list(kwargs.get("_prefer_cli", []))
        if "version" not in _prefer_cli:
            _prefer_cli.append("version")
        kwargs["_prefer_cli"] = _prefer_cli

        return cls.req(inst, **kwargs)

    def store_parts(self):
        parts = OrderedDict()
        parts["task_family"] = self.task_family
        return parts

    def store_parts_ext(self):
        parts = OrderedDict()
        if self.version is not None:
            parts["version"] = self.version
        return parts

    def local_path(self, *path, **kwargs):
        store = kwargs.get("store") or self.default_store
        store = os.path.expandvars(os.path.expanduser(store))
        parts = tuple(self.store_parts().values()) + tuple(self.store_parts_ext().values()) + path
        return os.path.join(store, *[str(p) for p in parts])

    def local_target(self, *args, **kwargs):
        cls = law.LocalDirectoryTarget if kwargs.pop("dir", False) else law.LocalFileTarget
        return cls(self.local_path(*args, store=kwargs.pop("store", None)), **kwargs)

    def wlcg_path(self, *path, **kwargs):
        if kwargs.pop("avoid_store", False):
            parts = list(path)[0]
        else:
            parts = tuple(self.store_parts().values()) + tuple(self.store_parts_ext().values()) + path
        return "/".join([parts])

    def wlcg_target(self, *args, **kwargs):
        kwargs.setdefault("fs", self.default_wlcg_fs)
        cls = law.wlcg.WLCGDirectoryTarget if kwargs.pop("dir", False) else law.wlcg.WLCGFileTarget
        path = self.wlcg_path(*args, **kwargs)
        kwargs.pop("avoid_store", False)
        return cls(path, **kwargs)

    def dynamic_target(self, *args, **kwargs):
        if os.getenv("CMT_REMOTE_JOB", "0") == "1":
            return self.wlcg_target(*args, **kwargs)
        else:
            return self.local_target(*args, **kwargs)


class ConfigTask(Task):

    config_name = luigi.Parameter(default="base_2018", description="name of the config file to "
        "load, default: base_2018")

    def __init__(self, *args, **kwargs):
        super(ConfigTask, self).__init__(*args, **kwargs)

        # load the config
        cmt = __import__("cmt.config." + self.config_name)
        self.config = getattr(cmt.config, self.config_name).config

    def store_parts(self):
        parts = super(ConfigTask, self).store_parts()
        parts["config_name"] = self.config_name
        return parts


class ConfigTaskWithCategory(ConfigTask):

    category_name = luigi.Parameter(default="baseline_even", description="the name of a category "
        "whose selection rules are applied, default: baseline_even")
    region_name = luigi.Parameter(default=law.NO_STR, description="an optional name of a region "
        "to apply live, default: empty")
    # use_base_category = luigi.BoolParameter(default=False, description="use the base category of "
    #     "the requested category for requirements, apply the selection on-the-fly, default: False")
    use_base_category = False  # currently disabled

    allow_composite_category = False

    def __init__(self, *args, **kwargs):
        super(ConfigTaskWithCategory, self).__init__(*args, **kwargs)

        self.category = self.config.categories.get(self.category_name)

        if self.category.subcategories and not self.allow_composite_category:
            raise Exception("category '{}' is composite, prohibited by task {}".format(
                self.category.name, self))

        self.region = None
        if self.region_name and self.region_name != law.NO_STR:
            self.region = self.config.regions.get(self.region_name)

    def store_parts(self):
        parts = super(ConfigTaskWithCategory, self).store_parts()
        parts["category_name"] = "cat_" + self.category_name
        return parts

    def expand_category(self):
        if self.category.subcategories:
            return self.category.subcategories
        else:
            return [self.category]

    def get_data_category(self, category=None):
        if category is None:
            category = self.category

        # if self.use_base_category and category.x.base_category:
            # return self.config.categories.get(category.x.base_category)
        # else:
            # return category
        return category


class DatasetTask(ConfigTask):

    dataset_name = luigi.Parameter(default="hh_ggf", description="name of the dataset to process, "
        "default: hh_ggf")

    def __init__(self, *args, **kwargs):
        super(DatasetTask, self).__init__(*args, **kwargs)

        # store a reference to the dataset object
        self.dataset = self.config.datasets.get(self.dataset_name)

        # store a reference to the main process
        self.process = self.dataset.process

    def store_parts(self):
        parts = super(DatasetTask, self).store_parts()
        parts["dataset_name"] = self.dataset_name
        return parts


class DatasetTaskWithCategory(ConfigTaskWithCategory, DatasetTask):

    def __init__(self, *args, **kwargs):
        super(DatasetTaskWithCategory, self).__init__(*args, **kwargs)

        # if self.dataset.merging:
            # self.n_files_after_merging = self.dataset.merging.get(self.category.name, 1)


class DatasetWrapperTask(ConfigTask):

    dataset_names = law.CSVParameter(default=(), description="names or name "
        "patterns of datasets to use, uses all datasets when empty, default: ()")
    dataset_tags = law.CSVParameter(default=(), description="list of tags for "
        "filtering datasets selected via dataset_names, default: ()")
    skip_dataset_names = law.CSVParameter(default=(), description="names or name pattern of "
        "datasets to skip, default: ()")
    skip_dataset_tags = law.CSVParameter(default=(), description="list of tags of datasets to "
        "skip, default: ()")

    def _find_datasets(self, names, tags):
        datasets = []
        for pattern in names:
            for dataset in self.config.datasets:
                if law.util.multi_match(dataset.name, pattern):
                    datasets.append(dataset)
        for tag in tags:
            for dataset in self.config.datasets:
                if dataset.has_tag(tag) and dataset not in datasets:
                    datasets.append(dataset)
        return datasets

    def __init__(self, *args, **kwargs):
        super(DatasetWrapperTask, self).__init__(*args, **kwargs)

        # first get datasets to skip
        skip_datasets = self._find_datasets(self.skip_dataset_names, self.skip_dataset_tags)

        # then get actual datasets and filter
        dataset_names = self.dataset_names
        if not dataset_names and not self.dataset_tags:
            dataset_names = self.get_default_dataset_names()
        self.datasets = [
            dataset for dataset in self._find_datasets(dataset_names, self.dataset_tags)
            if dataset not in skip_datasets
        ]

    def get_default_dataset_names(self):
        return list(self.config.datasets.names())


class HTCondorWorkflow(HTCondorWorkflowExt):

    only_missing = luigi.BoolParameter(default=True, significant=False, description="skip tasks "
        "that are considered complete, default: True")
    max_runtime = law.DurationParameter(default=2.0, unit="h", significant=False,
        description="maximum runtime, default unit is hours, default: 2")
    htcondor_central_scheduler = luigi.BoolParameter(default=False, significant=False,
        description="whether or not remote tasks should connect to the central scheduler, default: "
        "False")

    exclude_params_branch = {"max_runtime", "htcondor_central_scheduler"}

    def htcondor_output_directory(self):
        # the directory where submission meta data should be stored
        # at the CERN HTCondor system, this cannot be eos so force using the local store
        return law.LocalDirectoryTarget(self.local_path(store="$CMT_STORE_LOCAL"))

    def htcondor_bootstrap_file(self):
        # each job can define a bootstrap file that is executed prior to the actual job
        # in order to setup software and environment variables
        return os.path.expandvars("$CMT_BASE/cmt/files/cern_htcondor_bootstrap.sh")

    def htcondor_job_config(self, config, job_num, branches):
        # render variables
        config.render_variables["cmt_base"] = os.environ["CMT_BASE"]
        config.render_variables["cmt_env_path"] = os.environ["PATH"]

        # custom job file content
        config.custom_content.append(("requirements", "(OpSysAndVer =?= \"CentOS7\")"))
        config.custom_content.append(("getenv", "true"))
        config.custom_content.append(("log", "/dev/null"))
        config.custom_content.append(("+MaxRuntime", int(math.floor(self.max_runtime * 3600)) - 1))

        # print "{}/x509up".format(os.getenv("HOME"))
        config.custom_content.append(("Proxy_path", "{}/x509up".format(os.getenv("HOME"))))
        #config.custom_content.append(("arguments", "$(Proxy_path)"))
        
        return config
    
    def htcondor_create_job_file_factory(self, **kwargs):
        # job file fectory config priority: kwargs > class defaults
        kwargs = merge_dicts(self.htcondor_job_file_factory_defaults, kwargs)
        
        return HTCondorJobFileFactory(**kwargs)

    def htcondor_use_local_scheduler(self):
        return not self.htcondor_central_scheduler


class InputData(DatasetTask, law.ExternalTask):

    file_index = luigi.IntParameter(default=law.NO_INT, description="index of the external file to "
        "refer to, points to the collection of all files when empty, default: empty")

    version = None

    def complete(self):
        return True

    def output(self):
        if self.file_index != law.NO_INT:
            return self.dynamic_target(self.dataset.get_files()[self.file_index], avoid_store=True)
        else:
            cls = law.SiblingFileCollection
            return cls([self.dynamic_target(file_path, avoid_store=True) for file_path in self.dataset.get_files()])


class RemoteInputData(InputData):

    default_wlcg_fs = "wlcg_xrootd"
    os.environ["CMT_REMOTE_JOB"] = "1"
