# coding: utf-8

import law
import luigi
import itertools
from analysis_tools.utils import (
    import_root, create_file_dir, join_root_selection
)

from plotting_tools.root.labels import get_labels

from cmt.base_tasks.base import ( 
    DatasetTaskWithCategory, DatasetWrapperTask, HTCondorWorkflow, InputData, ConfigTaskWithCategory
)
from cmt.tasks.trigger import (
    AddTrigger, AddOffline, AddDiJetTrigger, AddDiJetOffline, ComputeRate, ComputeDiJetRate
)
from cmt.tasks.plotting import PlotAcceptance, Plot2D, PlotRate


class PlotTotalAcceptance(PlotAcceptance):

    yy_range = AddTrigger.yy_range
    zz_range = AddTrigger.zz_range
    yyp_range = AddDiJetTrigger.yy_range
    zzp_range = AddDiJetTrigger.zz_range

    # regions not supported
    region_name = None
    tree_name = "Events"
    
    def __init__(self, *args, **kwargs):
        super(PlotTotalAcceptance, self).__init__(*args, **kwargs)

    #def create_branch_map(self):
        #return len(self.dataset.get_files())

    def workflow_requires(self):
        return {
            "trigger": AddTrigger.vreq(self, _prefer_cli=["workflow"]),
            "offline": AddOffline.vreq(self, _prefer_cli=["workflow"]),
            "dijettrigger": AddDiJetTrigger.vreq(self, _prefer_cli=["workflow"]),
            "dijetoffline": AddDiJetOffline.vreq(self, _prefer_cli=["workflow"])
        }

    def requires(self):
        return {
            "trigger": AddTrigger.vreq(self, branch=self.branch, _prefer_cli=["workflow"]),
            "offline": AddOffline.vreq(self, branch=self.branch, _prefer_cli=["workflow"]),
            "dijettrigger": AddDiJetTrigger.vreq(self, branch=self.branch, _prefer_cli=["workflow"]),
            "dijetoffline": AddDiJetOffline.vreq(self, branch=self.branch, _prefer_cli=["workflow"])
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
        import itertools
        import json
        ROOT = import_root()

        inp_trigger = self.input()["trigger"].path
        inp_offline = self.input()["offline"].path
        inp_dijettrigger = self.input()["dijettrigger"].path
        inp_dijetoffline = self.input()["dijetoffline"].path

        tchain = ROOT.TChain("trigger")
        tchain.Add("{}/{}".format(inp_trigger, self.tree_name))

        offline_tchain = ROOT.TChain("offline")
        offline_tchain.Add("{}/{}".format(inp_offline, self.tree_name))

        dijet_trigger_tchain = ROOT.TChain("dijettrigger")
        dijet_trigger_tchain.Add("{}/{}".format(inp_dijettrigger, self.tree_name))

        dijet_offline_tchain = ROOT.TChain("dijetoffline")
        dijet_offline_tchain.Add("{}/{}".format(inp_dijetoffline, self.tree_name))

        tchain.AddFriend(offline_tchain, "offline")
        tchain.AddFriend(dijet_trigger_tchain, "dijettrigger")
        tchain.AddFriend(dijet_offline_tchain, "dijetoffline")
        df = ROOT.RDataFrame(tchain)

        histos = {}
        hmodel = ("", "", 1, 1, 2)
        for xx, yy, zz, yyp, zzp in itertools.product(
                range(*self.xx_range), range(*self.yy_range), range(*self.zz_range),
                range(*self.yyp_range), range(*self.zzp_range)):
            histos[(xx, yy, zz, yyp, zzp)] = df.Define(
                    "offl",
                    "offline.DoubleIsoTau{0}er2p1Jet{1}dR0p5".format(yy, zz)
                ).Define(
                    "dijettrig",
                    "dijettrigger.DoubleIsoTau{0}er2p1Jet{1}dR0p5Jet{1}dR0p5".format(yyp, zzp)
                ).Define(
                    "dijetoffl",
                    "dijetoffline.DoubleIsoTau{0}er2p1Jet{1}dR0p5Jet{1}dR0p5".format(yyp, zzp)
                ).Define(
                    "DoubleIsoTau", "DoubleIsoTau{0}er2p1".format(xx)
                ).Define(
                    "pass",
                    "(DoubleIsoTau && offline.DoubleIsoTau{0}er2p1) "
                        "|| (DoubleIsoTau{1}er2p1Jet{2}dR0p5 && offl)"
                        "|| (dijettrig && dijetoffl)".format(xx, yy, zz)
                ).Histo1D(hmodel, "pass")

        histo_den = df.Define(
                "DoubleIsoTau", "DoubleIsoTau32er2p1"
            ).Define(
                "den", "(DoubleIsoTau && offline.DoubleIsoTau32er2p1)"
            ).Histo1D(hmodel, "den")

        outp = self.output()["root"].path
        f = ROOT.TFile(create_file_dir(outp), "RECREATE")

        nbinsYp = self.yyp_range[1] - self.yyp_range[0]
        nbinsZp = self.zzp_range[1] - self.zzp_range[0]

        for xx, yy, zz in itertools.product(self.xx_range, self.yy_range, self.zz_range):
            hmodel = ("histo_{}_{}_{}".format(xx, yy, zz), "; YYP; ZZP",
                nbinsYp, self.yyp_range[0], self.yyp_range[1],
                nbinsZp, self.zzp_range[0], self.zzp_range[1],
            )
            num = ROOT.TH2F(*hmodel)
            for yyp, zzp in itertools.product(range(*self.yyp_range), range(*self.zzp_range)):
                num.Fill(yyp, zzp, histos[(xx, yy, zz, yyp, zzp)].Integral())
                del histos[(xx, yy, zz, yyp, zzp)]

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


class PlotTotal2D(Plot2D):

    def __init__(self, *args, **kwargs):
        super(PlotTotal2D, self).__init__(*args, **kwargs)
        self.labels = ["xx", "yy", "zz", "yyp", "zzp"]   
        self.ranges = [self.xx_range, PlotTotalAcceptance.yy_range, PlotTotalAcceptance.zz_range,
            PlotTotalAcceptance.yyp_range, PlotTotalAcceptance.zzp_range]

    def requires(self):
        reqs = {}
        for xx in range(*self.xx_range):
            reqs[xx] = PlotTotalAcceptance.req(self, version="{}_{}".format(self.version, xx),
                xx_range=(xx, xx + 1))
        return reqs

    def output(self):
        output = {}
        for val in itertools.product(range(*self.ranges[0]), range(*self.ranges[1]),
                range(*self.ranges[2])):
            output[val] = self.local_target("plot2D_xx{}_yy{}_zz{}.pdf".format(*val))
        return output

    def run(self):
        from copy import deepcopy
        import json
        ROOT = import_root()
        ROOT.gStyle.SetOptStat(0)
        ROOT.gStyle.SetPaintTextFormat("3.2f")
        inputs = self.input()
        output = self.output()
        den = 0

        nbinsYp = self.ranges[3][1] - self.ranges[3][0]
        nbinsZp = self.ranges[4][1] - self.ranges[4][0]

        histos = {}
        for val in itertools.product(range(*self.ranges[0]), range(*self.ranges[1]), range(*self.ranges[2])):
            hmodel = ("histo_{}_{}_{}".format(*val), "; YYp; ZZp",
                nbinsYp, self.ranges[3][0], self.ranges[3][1],
                nbinsZp, self.ranges[4][0], self.ranges[4][1],
            )
            histos[val] = ROOT.TH2F(*hmodel)
        for i, inp in enumerate(inputs.values()):
            for elem in inp.collection.targets.values():
                rootfile = ROOT.TFile.Open(elem["root"].path)
                for val in itertools.product(range(*self.ranges[0]), range(*self.ranges[1]),
                        range(*self.ranges[2])):
                    histo[val].Add(rootfile.Get("histo_{}_{}_{}".format(*val)).Clone())
                rootfile.Clone()
                jsonfile = elem["stats"].path
                if i == 0:
                    with open(jsonfile) as f:
                        d = json.load(f)
                    den += d["den"]

        for val in itertools.product(range(*self.ranges[0]), range(*self.ranges[1]),
                range(*self.ranges[2])):
            c = ROOT.TCanvas("", "", 800, 800)
            histo[val].Draw("colz, text")
            texts = get_labels(
                upper_right="           {} Simulation (13 TeV)".format(self.config.year),
                inner_text=[self.dataset.process.label, "(xx, yy, zz)=({}, {}, {})".format(*val)]
            )
            for text in texts:
                text.Draw("same")
            c.SaveAs(create_file_dir(output[val].path))
            del histo[val], c


class PlotTotalRate(PlotRate):
    yy_range = ComputeRate.yy_range
    zz_range = ComputeRate.zz_range
    yyp_range = ComputeDiJetRate.yy_range
    zzp_range = ComputeDiJetRate.zz_range

    def workflow_requires(self):
        return {
            "trigger": ComputeRate.vreq(self, _prefer_cli=["workflow"]),
            "dijetrigger": ComputeDiJetRate.vreq(self, _prefer_cli=["workflow"]),
        }

    def requires(self):
        return {
            "trigger": ComputeRate.vreq(self, branch=self.branch, _prefer_cli=["workflow"]),
            "dijettrigger": ComputeRate.vreq(self, branch=self.branch, _prefer_cli=["workflow"])
        }

    @law.decorator.notify
    @law.decorator.localize(input=False)
    def run(self):
        import itertools
        import json
        ROOT = import_root()

        inp_trigger = self.input()["trigger"].path
        
        tchain = ROOT.TChain("trigger")
        tchain.Add("{}/{}".format(inp_trigger, self.tree_name))

        dijet_trigger_tchain = ROOT.TChain("dijettrigger")
        dijet_trigger_tchain.Add("{}/{}".format(inp_dijettrigger, self.tree_name))

        tchain.AddFriend(dijet_trigger_tchain, "dijettrigger")
        df = ROOT.RDataFrame(tchain)

        histos_ditaujet_or = {}
        hmodel = ("", "", 1, 1, 2)

        # filling plots for DoubleTau + jet w/o and w/ overlap removal
        for xx, yy, zz, yyp, zzp in itertools.product(
                range(*self.xx_range), range(*self.yy_range), range(*self.zz_range),
                range(*self.yyp_range), range(*self.zzp_range)):
            histos_ditaujet_or[(xx, yy, zz, yyp, zzp)] = df.Define(
                    "dijet",
                    "dijettrigger.DoubleIsoTau{0}er2p1Jet{1}dR0p5Jet{1}dR0p5".format(yyp, zzp)
                ).Define(
                    "pass",
                    "DoubleIsoTau{0}er2p1 || DoubleIsoTau{1}er2p1Jet{2}dR0p5 || dijet".format(xx, yy, zz)
                ).Histo1D(hmodel, "pass")

        # filling summary plots
        nbinsX = self.xx_range[1] - self.xx_range[0]
        nbinsYp = self.yyp_range[1] - self.yyp_range[0]
        nbinsZp = self.zzp_range[1] - self.zzp_range[0]
       
        histos = {}
        
        # saving output plots in root files
        outp = self.output()["root"].path
        f = ROOT.TFile(create_file_dir(outp), "RECREATE")
        
        # - fill (and save) the 2D plots for the DoubleTau + jet triggers
        for xx, yy, zz in itertools.product(
                range(*self.xx_range), range(*self.yy_range), range(*self.zz_range)):
            hmodel_ditaujet_or = ("histo_{}_{}_{}".format(xx, yy, zz), "; YYp; ZZp",
                nbinsYp, self.yyp_range[0], self.yyp_range[1],
                nbinsZp, self.zzp_range[0], self.zzp_range[1],
            )    
            histos[(xx, yy, zz)] = ROOT.TH2F(*hmodel_ditaujet_or)
            for yyp, zzp in itertools.product(range(*self.yyp_range), range(*self.zzp_range)):
                histos[(xx, yy, zz)].Fill(yyp, zzp, histos_ditaujet_or[(xx, yy, zz, yyp, zzp)].Integral())
            histos[(xx, yy, zz)].Write()
        f.Close()

        f = ROOT.TFile.Open(inp_trigger)
        tree = f.Get(self.tree_name)
        stats = {
            "nevents": tree.GetEntries(),
        }
        f.Close()

        stats_path = self.output()["stats"].path
        with open(create_file_dir(stats_path), "w") as json_f:
            json.dump(stats, json_f, indent=4)


class PlotTotal2DRate(PlotTotal2D):

    def __init__(self, *args, **kwargs):
        super(PlotTotal2D, self).__init__(*args, **kwargs)
        self.ranges = [self.xx_range, PlotTotalRate.yy_range, PlotTotalRate.zz_range,
            PlotTotalRate.yyp_range, PlotTotalRate.zzp_range]

    def requires(self):
        reqs = {}
        for xx in range(*self.xx_range):
            reqs[xx] = PlotTotalRate.req(self, version="{}_{}".format(self.version, xx),
                xx_range=(xx, xx + 1))
        return reqs

    def output(self):
        output = {}
        for val in itertools.product(range(*self.ranges[0]), range(*self.ranges[1]),
                range(*self.ranges[2])):
            output[val] = self.local_target("rateplot2D_xx{}_yy{}_zz{}.pdf".format(*val))
        output["ditaujet_or_stats"] = self.local_target("rate_ditaujet_or.json")
        return output

    def run(self):
        from copy import deepcopy
        import json
        ROOT = import_root()
        ROOT.gStyle.SetOptStat(0)
        ROOT.gStyle.SetPaintTextFormat("3.2f")
        inputs = self.input()
        output = self.output()
        den = 0

        # Create histos for ditau or ditau_jet or ditau_dijet
        nbinsYp = self.ranges[3][1] - self.ranges[3][0]
        nbinsZp = self.ranges[4][1] - self.ranges[4][0]
        histos = {}
        for val in itertools.product(range(*self.ranges[0]), range(*self.ranges[1]), range(*self.ranges[2])):
            hmodel = ("histo_{}_{}_{}".format(*val), "; YYp; ZZp",
                nbinsYp, self.ranges[3][0], self.ranges[3][1],
                nbinsZp, self.ranges[4][0], self.ranges[4][1],
            )
            histos[val] = ROOT.TH2F(*hmodel)
        for i, inp in enumerate(inputs.values()):
            for elem in inp.collection.targets.values():
                rootfile = ROOT.TFile.Open(elem["root"].path)
                for val in itertools.product(range(*self.ranges[0]), range(*self.ranges[1]),
                        range(*self.ranges[2])):
                    histos[val].Add(rootfile.Get("histo_{}_{}_{}".format(*val)).Clone())
                rootfile.Clone()
                jsonfile = elem["stats"].path
                if i == 0:
                    with open(jsonfile) as f:
                        d = json.load(f)
                    nevents += d["nevents"]

        dijet_rate_dict = {}
        scaling = self.dataset.get_aux("rate_scaling")

        for val in itertools.product(range(*self.ranges[0]), range(*self.ranges[1]),
                range(*self.ranges[2])):
            histo[val].Scale((scaling * 2760. * 11246.) / (1000 * nevents))
            for (yyp, zzp) in itertools.product(range(*self.ranges[3]), range(*self.ranges[4])):
                rate_dict["{}, {}, {}, {}, {}".format(val[0], val[1], val[2], yyp, zzp)] = histo[val].GetBinContent(
                    yyp - self.ranges[3][0], zzp - self.ranges[4][0])
            c = ROOT.TCanvas("", "", 800, 800)
            histo[val].Draw("colz, text")
            texts = get_labels(
                upper_right="           {} Simulation (13 TeV)".format(self.config.year),
                inner_text=[self.dataset.process.label, "(xx, yy, zz)=({}, {}, {})".format(*val)]
            )
            for text in texts:
                text.Draw("same")
            c.SaveAs(create_file_dir(output[val].path))
            del c
        
        stats_path = self.output()["ditaujet_or_stats"].path
        with open(create_file_dir(stats_path), "w") as json_f:
            json.dump(rate_dict, json_f, indent=4)