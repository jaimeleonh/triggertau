# coding: utf-8

import law
import luigi

from cmt.base_tasks.base import ( 
    DatasetTaskWithCategory, DatasetWrapperTask, HTCondorWorkflow, InputData, ConfigTaskWithCategory
)

from cmt.tasks.trigger import (
    AddTrigger, AddOffline
)


class PlotAcceptance(DatasetTaskWithCategory, law.LocalWorkflow, HTCondorWorkflow):

    #xx_range = (20, 40)
    #yy_range = (20, 40)
    #zz_range = (60, 140)
    xx_range = (20, 40)
    yy_range = (20, 40)
    zz_range = (60, 70)

    # regions not supported
    region_name = None
    tree_name = "Events"

    def create_branch_map(self):
        return len(self.dataset.get_files())

    def workflow_requires(self):
        return {
            "trigger": AddTrigger.req(self, _prefer_cli=["workflow"]),
            "offline": AddOffline.req(self, _prefer_cli=["workflow"])
        }

    def requires(self):
        return {
            "trigger": AddTrigger.req(self, branch=self.branch, _prefer_cli=["workflow"]),
            "offline": AddOffline.req(self, branch=self.branch, _prefer_cli=["workflow"])
        }

    def output(self):
        return {
            "stats": self.local_target("{}".format(self.input()["trigger"].path.split("/")[-1]
                ).replace(".root", ".json")),
            "root": self.local_target("{}".format(self.input()["trigger"].path.split("/")[-1]))
        }

    @law.decorator.notify
    @law.decorator.localize(input=False)
    def run(self):
        from analysis_tools.utils import (
            import_root, create_file_dir, join_root_selection
        )
        import itertools
        import json
        ROOT = import_root()

        inp_trigger = self.input()["trigger"].path
        inp_offline = self.input()["offline"].path
        tchain = ROOT.TChain("trigger")
        tchain.Add("{}/{}".format(inp_trigger, self.tree_name))
        offline_tchain = ROOT.TChain("offline")
        offline_tchain.Add("{}/{}".format(inp_offline, self.tree_name))
        tchain.AddFriend(offline_tchain, "offline")
        df = ROOT.RDataFrame(tchain)

        histos = {}
        hmodel = ("", "", 1, 1, 2)
        for xx, yy, zz in itertools.product(
                range(*self.xx_range), range(*self.yy_range), range(*self.zz_range)):
            name = "{}_{}_{}".format(xx, yy, zz)
            histos[(xx, yy, zz)] = df.Define(
                    "dum",
                    "offline.DoubleIsoTau{0}er2p1Jet{1}dR0p5".format(yy + 10, zz + 10)
                ).Define(
                    "pass",
                    "(DoubleIsoTau{0}er2p1 && offline.DoubleIsoTau{3}er2p1) "
                        "|| (DoubleIsoTau{1}er2p1Jet{2}dR0p5 "                        
                            "&& dum)".format(
                        xx, yy, zz, xx + 10)
                            # "&& offline.DoubleIsoTau{4}er2p1Jet{5}dR0p5)".format(
                        # xx, yy, zz, xx + 10, yy + 10, zz + 10)
                ).Histo1D(hmodel, "pass")
        histo_den = df.Define("den", "(DoubleIsoTau32er2p1 && offline.DoubleIsoTau40er2p1)"
            ).Histo1D(hmodel, "den")

        nbinsX = self.xx_range[1] - self.xx_range[0]
        nbinsY = self.yy_range[1] - self.yy_range[0]
        nbinsZ = self.zz_range[1] - self.zz_range[0]
        hmodel = ("histo", "; XX; YY; ZZ",
            nbinsX, self.xx_range[0], self.xx_range[1],
            nbinsY, self.yy_range[0], self.yy_range[1],
            nbinsZ, self.zz_range[0], self.zz_range[1],
        )
        num = ROOT.TH3F(*hmodel)
        for xx, yy, zz in itertools.product(
                range(*self.xx_range), range(*self.yy_range), range(*self.zz_range)):
            num.Fill(xx, yy, zz, histos[(xx, yy, zz)].Integral())

        
        outp = self.output()["root"].path
        f = ROOT.TFile(create_file_dir(outp), "RECREATE")
        num.Write()
        f.Close()

        f = ROOT.TFile.Open(inp_trigger)
        tree = f.Get(self.tree_name)
        stats = {
            "nevents": tree.GetEntries(),
            "den": histo_den.Integral()
        }
        f.Close()

        stats_path = self.output()["stats"].path
        with open(create_file_dir(stats_path), "w") as json_f:
            json.dump(stats, json_f, indent=4)

