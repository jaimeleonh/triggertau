# coding: utf-8

"""
Preprocessing tasks.
"""

__all__ = []


import abc
import contextlib
import itertools
from collections import OrderedDict, defaultdict
import os

import law
import luigi

from cmt.base_tasks.base import ( 
    DatasetTaskWithCategory, DatasetWrapperTask, HTCondorWorkflow, RemoteInputData, ConfigTaskWithCategory
)


class DatasetCategoryWrapperTask(DatasetWrapperTask, law.WrapperTask):

    category_names = law.CSVParameter(default=("baseline_even",), description="names of categories "
        "to run, default: (baseline_even,)")

    exclude_index = True

    def __init__(self, *args, **kwargs):
        super(DatasetCategoryWrapperTask, self).__init__(*args, **kwargs)

        # tasks wrapped by this class do not allow composite categories, so split them here
        self.categories = []
        for name in self.category_names:
            category = self.config.categories.get(name)
            if category.subcategories:
                self.categories.extend(category.subcategories)
            else:
                self.categories.append(category)

    @abc.abstractmethod
    def atomic_requires(self, dataset, category):
        return None

    def requires(self):
        return OrderedDict(
            ((dataset.name, category.name), self.atomic_requires(dataset, category))
            for dataset, category in itertools.product(self.datasets, self.categories)
        )


class Preprocess(DatasetTaskWithCategory, law.LocalWorkflow, HTCondorWorkflow):

    modules = luigi.DictParameter(default=None)
    modules_file = luigi.Parameter(description="filename with modules to run on nanoAOD tools",
        default=None)

    # regions not supported
    region_name = None

    default_store = "$CMT_STORE_EOS_CATEGORIZATION"
    default_wlcg_fs = "wlcg_fs_categorization"

    def create_branch_map(self):
        return len(self.dataset.get_files())

    def workflow_requires(self):
        return {"data": RemoteInputData.req(self)}

    def requires(self):
        return {"data": RemoteInputData.req(self, file_index=self.branch)}

    def output(self):
        return self.local_target("{}".format(self.input()["data"].path.split("/")[-1]))
        # return self.local_target("{}".format(self.input()["data"].split("/")[-1]))
    
    def get_modules(self):
        module_params = None
        if self.modules_file:
            import yaml
            with open(os.path.expandvars("$CMT_BASE/cmt/config/{}.yaml".format(self.modules_file))) as f:
                module_params = yaml.load(f, Loader=yaml.FullLoader)
        else:
            return []
        
        def _args(*_nargs, **_kwargs):
            return _nargs, _kwargs
        
        modules = []
        for name in module_params.keys():
            parameter_str = ""
            for param, value in module_params[name]["parameters"].items():
                if isinstance(value, str): 
                    if "self" in value:
                        value = eval(value)
                if isinstance(value, str):
                    parameter_str += param + " = '{}', ".format(value)
                else:
                    parameter_str += param + " = {}, ".format(value)
            mod = module_params[name]["path"]
            mod = __import__(mod, fromlist=[name])
            nargs, kwargs = eval('_args(%s)' % parameter_str)
            modules.append(getattr(mod, name)(**kwargs)())      
        return modules

    @law.decorator.notify
    @law.decorator.localize(input=False)
    def run(self):
        from analysis_tools.utils import join_root_selection as jrs
        from shutil import move
        # from PhysicsTools.NanoAODTools.postprocessing.modules.jme.jetmetHelperRun2 import (
            # createJMECorrector
        # )
        from PhysicsTools.NanoAODTools.postprocessing.framework.postprocessor import PostProcessor

        # prepare inputs and outputs
        # inp = self.input()["data"]
        inp = self.input()["data"].path
        outp = self.output().path

        # build the full selection
        selection = self.category.selection
        dataset_selection = self.dataset.get_aux("selection")
        if dataset_selection and dataset_selection != "1":
            selection = jrs(dataset_selection, selection, op="and")
        selection = "Jet_pt > 1000" # hard-coded to reduce the number of events on purpose
        modules = self.get_modules()
        p = PostProcessor(".", [inp],
                      cut=selection,
                      modules=modules,
                      postfix="")
        p.run()
        move("./{}".format(inp.split("/")[-1]), outp)


class PreprocessWrapper(DatasetCategoryWrapperTask):

    def atomic_requires(self, dataset, category):
        return Preprocess.req(self, dataset_name=dataset.name, category_name=category.name)
