# coding: utf-8

"""
Preprocessing tasks.
"""

__all__ = []


import abc
import contextlib
import itertools
from collections import OrderedDict, defaultdict

import law
import luigi

from cmt.base_tasks.base import ( 
    DatasetTaskWithCategory, DatasetWrapperTask, HTCondorWorkflow, InputData, ConfigTaskWithCategory
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

    # regions not supported
    region_name = None

    default_store = "$CMT_STORE_EOS_CATEGORIZATION"
    default_wlcg_fs = "wlcg_fs_categorization"

    def create_branch_map(self):
        return len(self.dataset.get_files())

    def workflow_requires(self):
        return {"data": InputData.req(self)}

    def requires(self):
        return {"data": InputData.req(self, file_index=self.branch)}

    def output(self):
        return self.local_target("{}".format(self.input()["data"].path.split("/")[-1]))
        # return self.local_target("{}".format(self.input()["data"].split("/")[-1]))
    
    def get_modules(self):
        from importlib import import_module
        import sys
        modules = []
        if self.modules:
            for mod, names in self.modules:
                import_module(mod)
                obj = sys.modules[mod]
                selnames = names.split(",")
                mods = dir(obj)
                for name in selnames:
                    if name in mods:
                        print("Loading %s from %s " % (name, mod))
                        if type(getattr(obj, name)) == list:
                            for mod in getattr(obj, name):
                                modules.append(mod())
                        else:
                            modules.append(getattr(obj, name)())
        return modules        


    @law.decorator.notify
    @law.decorator.localize(input=False)
    def run(self):
        from analysis_tools.utils import join_root_selection as jrs
        from shutil import move
        from PhysicsTools.NanoAODTools.postprocessing.modules.jme.jetmetHelperRun2 import createJMECorrector
        from PhysicsTools.NanoAODTools.postprocessing.framework.postprocessor import PostProcessor

        # prepare inputs and outputs
        # inp = self.input()["data"]
        inp = self.input()["data"].path
        print inp
        outp = self.output().path

        # build the full selection
        selection = self.category.selection
        dataset_selection = self.dataset.get_aux("selection")
        if dataset_selection and dataset_selection != "1":
            selection = jrs(dataset_selection, selection, op="and")
        selection = "Jet_pt>500"

        jmeCorrections = createJMECorrector(
            True, self.config.year, "B", "All", "AK4PFchs", False, splitJER=True)

        modules = self.get_modules()
        p = PostProcessor(".", [inp],
                      cut=selection,
                      modules=[jmeCorrections()],
                      postfix="")
        p.run()
        move("./{}".format(inp.split("/")[-1]), outp)


class PreprocessWrapper(DatasetCategoryWrapperTask):

    def atomic_requires(self, dataset, category):
        return Preprocess.req(self, dataset_name=dataset.name, category_name=category.name)
