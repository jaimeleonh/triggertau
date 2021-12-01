import os
import law
import luigi
import json
import math
import itertools
from copy import deepcopy
from analysis_tools.utils import (
    import_root, create_file_dir, join_root_selection
)

from plotting_tools.root.labels import get_labels

from cmt.base_tasks.base import ( 
    DatasetTaskWithCategory, ConfigTaskWithCategory, 
)
from cmt.tasks.trigger import (
    AddTrigger, Skim
)

class L1TauOfflineJetPlotMaker(AddTrigger):
    xx_range = (32, 40)
    yy_range = (20, 33)
    zz_range = (20, 80)

    def workflow_requires(self):
        return {"data": Skim.vreq(self, _prefer_cli=["workflow"])}

    def requires(self):
        return {"data": Skim.vreq(self, _prefer_cli=["workflow"], branch=self.branch)}

    def output(self):
        return self.local_target("{}".format(self.input()["data"].path.split("/")[-1]))

    def get_new_dataframe(self, input_file, output_file):
        ROOT = import_root()

        ROOT.gSystem.Load("%s" % os.path.expandvars("$CMT_BASE/cmt/tasks/./TotalTrigger_C.so"))

        run = ROOT.TotalTrigger(input_file, self.output().path, self.tree_name,
            self.xx_range[0], self.xx_range[1], self.yy_range[0], self.yy_range[1],
            self.zz_range[0], self.zz_range[1], -1, -1,
            -1, -1, -1
        )
        run.L1TauOfflineJetLoop()

    @law.decorator.notify
    @law.decorator.localize(input=False)
    def run(self):
        inp = self.input()["data"].path
        self.get_new_dataframe(inp, None)


class L1TauOfflineJetPlotter(DatasetTaskWithCategory, ConfigTaskWithCategory):
    yy_range = (20, 33)

    def requires(self):
        return L1TauOfflineJetPlotMaker.vreq(self)

    def output(self):
        output = {
            "turnon": self.local_target("turnon.pdf"),
            "eff": self.local_target("efficiency.pdf"),
        }
        for yy in range(*self.yy_range):
            output["eff_%s" % yy] = self.local_target("eff_%s.pdf" % yy)
        return output

    def run(self):
        ROOT = import_root()
        histos = {}

        histo_names_2D = ["eff"]
        histo_names = ["turnon"]
        for yy in range(*self.yy_range):
            histo_names.append("eff_%s" % yy)

        inp = self.input()
        output = self.output()

        for elem in inp.collection.targets.values():
            rootfile = ROOT.TFile.Open(elem.path)

            for name in histo_names:
                for tag in ["passed", "total"]:
                    name_tag = "%s_%s" % (name, tag)
                    if name_tag in histos:
                        histos[name_tag].Add(rootfile.Get(name_tag))
                    else:
                        histos[name_tag] = deepcopy(rootfile.Get(name_tag))

            for name in histo_names_2D:
                for tag in ["passed", "total"]:
                    name_tag = "%s_%s" % (name, tag)
                    if name_tag in histos:
                        histos[name_tag].Add(rootfile.Get(name_tag))
                    else:
                        histos[name_tag] = deepcopy(rootfile.Get(name_tag))
        
        for name in histo_names + histo_names_2D:
            c = ROOT.TCanvas()
            eff = ROOT.TEfficiency(histos["%s_passed" % name], histos["%s_total" % name])
            if name in histo_names:
                eff.Draw()
            else:
                eff.Draw("colz")
            c.SaveAs(create_file_dir(output[name].path))


















