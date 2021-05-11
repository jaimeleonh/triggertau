# coding: utf-8

import law
import luigi
import itertools
from analysis_tools.utils import (
    import_root, create_file_dir, join_root_selection
)

from plottint_tools.root.labels import get_labels

from cmt.base_tasks.base import ( 
    DatasetTaskWithCategory, DatasetWrapperTask, HTCondorWorkflow, InputData, ConfigTaskWithCategory
)
from cmt.tasks.trigger import (
    AddTrigger, AddOffline, ComputeRate
)


class PlotAcceptance(DatasetTaskWithCategory, law.LocalWorkflow, HTCondorWorkflow):

    xx_range = law.CSVParameter(default=("32", "33"))
    yy_range = (20, 40)
    zz_range = (20, 160)    
    #xx_range = (20, 21)
    #yy_range = (20, 21)
    #zz_range = (20, 21)

    # regions not supported
    region_name = None
    tree_name = "Events"
    
    def __init__(self, *args, **kwargs):
        super(PlotAcceptance, self).__init__(*args, **kwargs)
        self.xx_range = [int(xx) for xx in self.xx_range]

    def create_branch_map(self):
        return len(self.dataset.get_files())

    def workflow_requires(self):
        return {
            "trigger": AddTrigger.vreq(self, _prefer_cli=["workflow"]),
            "offline": AddOffline.vreq(self, _prefer_cli=["workflow"])
        }

    def requires(self):
        return {
            "trigger": AddTrigger.vreq(self, branch=self.branch, _prefer_cli=["workflow"]),
            "offline": AddOffline.vreq(self, branch=self.branch, _prefer_cli=["workflow"])
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
            histos[(xx, yy, zz)] = df.Define(
                    "dum",
                    "offline.DoubleIsoTau{0}er2p1Jet{1}dR0p5".format(yy, zz)
                ).Define(
                    "DoubleIsoTau", "DoubleIsoTau{0}er2p1".format(xx)
                ).Define(
                    "pass",
                    "(DoubleIsoTau && offline.DoubleIsoTau{0}er2p1) "
                        "|| (DoubleIsoTau{1}er2p1Jet{2}dR0p5 && dum)".format(xx, yy, zz)
                ).Histo1D(hmodel, "pass")
        
        histos_ditau = {}
        for xx in range(*self.xx_range):
            histos_ditau[xx] = df.Define(
                    "DoubleIsoTau", "DoubleIsoTau{0}er2p1".format(xx)
                ).Define(
                    "OfflineDoubleIsoTau", "offline.DoubleIsoTau{0}er2p1".format(xx)
                ).Define(
                    "pass", "DoubleIsoTau && OfflineDoubleIsoTau"
                ).Histo1D(hmodel, "pass")

        histos_ditaujet = {}
        for xx, zz in itertools.product(
                range(*self.xx_range), range(*self.zz_range)):
            histos_ditaujet[(xx, zz)] = df.Define(
                    "DoubleIsoTauJet",
                    "DoubleIsoTau{0}er2p1Jet{1}dR0p5".format(xx, zz)
                ).Define(
                    "OfflineDoubleIsoTauJet",
                    "offline.DoubleIsoTau{0}er2p1Jet{1}dR0p5".format(xx, zz)
                ).Define(
                    "pass", "DoubleIsoTauJet && OfflineDoubleIsoTauJet"
                ).Histo1D(hmodel, "pass")

        histo_den = df.Define(
                "DoubleIsoTau", "DoubleIsoTau32er2p1"
            ).Define("den", "(DoubleIsoTau && offline.DoubleIsoTau32er2p1)"
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

        hmodel = ("ditau", "; XX; Events; ",
            nbinsX, self.xx_range[0], self.xx_range[1]
        )
        ditau = ROOT.TH1F(*hmodel)
        for xx in range(*self.xx_range):
            ditau.Fill(xx, histos_ditau[xx].Integral())

        hmodel = ("ditaujet", "; XX; ZZ",
            nbinsX, self.xx_range[0], self.xx_range[1],
            nbinsZ, self.zz_range[0], self.zz_range[1],
        )
        ditaujet = ROOT.TH2F(*hmodel)
        for xx, zz in itertools.product(
                range(*self.xx_range), range(*self.zz_range)):
            ditaujet.Fill(xx, zz, histos_ditaujet[(xx, zz)].Integral())

        outp = self.output()["root"].path
        f = ROOT.TFile(create_file_dir(outp), "RECREATE")
        num.Write()
        ditau.Write()
        ditaujet.Write()
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


class Plot2D(DatasetTaskWithCategory):
    
    xx_range = law.CSVParameter(default=("32", "40"))

    def __init__(self, *args, **kwargs):
        super(Plot2D, self).__init__(*args, **kwargs)
        self.xx_range = [int(xx) for xx in self.xx_range]
        self.labels = ["xx", "yy", "zz"]   
        self.ranges = [self.xx_range, PlotAcceptance.yy_range, PlotAcceptance.zz_range]

    def requires(self):
        reqs = {}
        for xx in range(*self.xx_range):
            reqs[xx] = PlotAcceptance.req(self, version="{}_{}".format(self.version, xx),
                xx_range=(xx, xx + 1))
        return reqs

    def output(self):
        output = {}
        for label, var_range in zip(self.labels, self.ranges):
            for val in range(*var_range):
                output[(label, val)] = self.local_target("plot2D_{}{}.pdf".format(label, val))
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

        nbinsX = self.ranges[0][1] - self.ranges[0][0]
        nbinsY = self.ranges[1][1] - self.ranges[1][0]
        nbinsZ = self.ranges[2][1] - self.ranges[2][0]
        hmodel = ("histo", "; XX; YY; ZZ",
            nbinsX, self.ranges[0][0], self.ranges[0][1],
            nbinsY, self.ranges[1][0], self.ranges[1][1],
            nbinsZ, self.ranges[2][0], self.ranges[2][1],
        )
        histo = ROOT.TH3F(*hmodel)
        for i, inp in enumerate(inputs.values()):
            for elem in inp.collection.targets.values():
                rootfile = ROOT.TFile.Open(elem["root"].path)
                histo.Add(rootfile.Get("histo").Clone())
                rootfile.Clone()
                jsonfile = elem["stats"].path
                if i == 0:
                    with open(jsonfile) as f:
                        d = json.load(f)
                    den += d["den"]

        for i, (label, var_range) in enumerate(zip(self.labels, self.ranges)):
            other_labels = deepcopy(self.labels)
            other_ranges = deepcopy(self.ranges)

            other_labels.remove(label)
            other_ranges.remove(var_range)

            x_binning = (other_ranges[0][1] - other_ranges[0][0],
                other_ranges[0][0], other_ranges[0][1])

            y_binning = (other_ranges[1][1] - other_ranges[1][0],
                other_ranges[1][0], other_ranges[1][1])

            for val in range(*var_range):
                #histo2D = ROOT.TH1F("histo{}{}".format(label, val), " ;" + "; ".join(other_labels), *x_binning)
                histo2D = ROOT.TH2F("histo{}{}".format(label, val), " ;" + "; ".join(other_labels),
                    x_binning[0], x_binning[1], x_binning[2],
                    y_binning[0], y_binning[1], y_binning[2])

                for (xbin, ybin) in itertools.product(
                        range(*other_ranges[0]), range(*other_ranges[1])):
                    xbin -= other_ranges[0][0]
                    ybin -= other_ranges[1][0]
                    otherbin = val - var_range[0]
                    if i == 0:
                        (x, y, z) = (otherbin + 1, xbin + 1, ybin + 1)
                    elif i == 1:
                        (x, y, z) = (xbin + 1, otherbin + 1, ybin + 1)
                    elif i == 2:
                        (x, y, z) = (xbin + 1, ybin + 1, otherbin + 1)
                    # print i
                    # print x, y, z, histo.GetBinContent(x, y, z), float(den)
                    # a = raw_input()
                    # if a == "":
                        # continue
                    histo2D.SetBinContent(xbin + 1, ybin + 1,
                        histo.GetBinContent(x, y, z) / float(den))
                c = ROOT.TCanvas("", "", 800, 800)
                histo2D.Draw("colz, text")
                texts = get_labels(upper_right="           2018 Simulation (13 TeV)",
                    inner_text=[self.dataset.process.label, "{}={}".format(label, val)])
                for text in texts:
                    text.Draw("same")
                c.SaveAs(create_file_dir(output[(label, val)].path))
                del histo2D, c


class PlotRate(PlotAcceptance):

    tree_name = "l1UpgradeEmuTree/L1UpgradeTree"

    def workflow_requires(self):
        return {
            "trigger": ComputeRate.vreq(self, _prefer_cli=["workflow"])
        }

    def requires(self):
        return {
            "trigger": ComputeRate.vreq(self, branch=self.branch, _prefer_cli=["workflow"])
        }

    @law.decorator.notify
    @law.decorator.localize(input=False)
    def run(self):
        import itertools
        import json
        ROOT = import_root()

        inp_trigger = self.input()["trigger"].path
        df = ROOT.RDataFrame(self.tree_name, inp_trigger)

        histos_ditau = {}
        histos_ditaujet = {}
        histos_ditaujet_or = {}
        hmodel = ("", "", 1, 1, 2)

        # filling plots for DoubleTau trigger
        for xx in range(*self.xx_range):
            histos_ditau[xx] = df.Define(
                "pass", "DoubleIsoTau{0}er2p1".format(xx)
            ).Histo1D(hmodel, "pass")

        # filling plots for DoubleTau + jet w/o and w/ overlap removal
        for xx, yy, zz in itertools.product(
                range(*self.xx_range), range(*self.yy_range), range(*self.zz_range)):
            histos_ditaujet[(xx, yy, zz)] = df.Define(
                    "pass",
                    "DoubleIsoTau{0}er2p1 || DoubleIsoTau{1}er2p1Jet{2}".format(xx, yy, zz)
                ).Histo1D(hmodel, "pass")
            histos_ditaujet_or[(xx, yy, zz)] = df.Define(
                    "pass",
                    "DoubleIsoTau{0}er2p1 || DoubleIsoTau{1}er2p1Jet{2}dR0p5".format(xx, yy, zz)
                ).Histo1D(hmodel, "pass")

        # filling summary plots
        nbinsX = self.xx_range[1] - self.xx_range[0]
        nbinsY = self.yy_range[1] - self.yy_range[0]
        nbinsZ = self.zz_range[1] - self.zz_range[0]

        # - fill the 1D plot for the DoubleTau trigger
        hmodel_ditau = ("histo_ditau", "; XX; ", nbinsX, self.xx_range[0], self.xx_range[1]) 
        histo_ditau = ROOT.TH1F(*hmodel_ditau)
        for xx in range(*self.xx_range):
            histo_ditau.Fill(xx, histos_ditau[xx].Integral())

        # - fill the 3D plots for the DoubleTau + jet triggers
        hmodel_ditaujet = ("histo_ditaujet", "; XX; YY; ZZ",
            nbinsX, self.xx_range[0], self.xx_range[1],
            nbinsY, self.yy_range[0], self.yy_range[1],
            nbinsZ, self.zz_range[0], self.zz_range[1],
        )
        hmodel_ditaujet_or = ("histo_ditaujet_or", "; XX; YY; ZZ",
            nbinsX, self.xx_range[0], self.xx_range[1],
            nbinsY, self.yy_range[0], self.yy_range[1],
            nbinsZ, self.zz_range[0], self.zz_range[1],
        )
        histo_ditaujet = ROOT.TH3F(*hmodel_ditaujet)
        histo_ditaujet_or = ROOT.TH3F(*hmodel_ditaujet_or)
        for xx, yy, zz in itertools.product(
                range(*self.xx_range), range(*self.yy_range), range(*self.zz_range)):
            histo_ditaujet.Fill(xx, yy, zz, histos_ditaujet[(xx, yy, zz)].Integral())
            histo_ditaujet_or.Fill(xx, yy, zz, histos_ditaujet_or[(xx, yy, zz)].Integral())

        # saving output plots in root files
        outp = self.output()["root"].path
        f = ROOT.TFile(create_file_dir(outp), "RECREATE")
        histo_ditau.Write()
        histo_ditaujet.Write()
        histo_ditaujet_or.Write()
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


class Plot2DRate(DatasetTaskWithCategory):
    
    xx_range = law.CSVParameter(default=("32", "40"))

    def __init__(self, *args, **kwargs):
        super(Plot2DRate, self).__init__(*args, **kwargs)
        self.xx_range = [int(xx) for xx in self.xx_range]
        self.labels = ["xx", "yy", "zz"]   
        self.ranges = [self.xx_range, PlotAcceptance.yy_range, PlotAcceptance.zz_range]

    def requires(self):
        reqs = {}
        for xx in range(*self.xx_range):
            reqs[xx] = PlotRate.req(self, version="{}_{}".format(self.version, xx),
                xx_range=(xx, xx + 1))
        return reqs

    def output(self):
        output = {
            "ditaujet": {},
            "ditaujet_or": {}
        }

        output["ditau"] = self.local_target("rate_ditau.pdf")
        for label, var_range in zip(self.labels, self.ranges):
            for val in range(*var_range):
                output["ditaujet"][(label, val)] = self.local_target(
                    "rate_ditaujet_{}{}.pdf".format(label, val))
                output["ditaujet_or"][(label, val)] = self.local_target(
                    "rate_ditaujet_or_{}{}.pdf".format(label, val))

        output["ditau_stats"] = self.local_target("rate_ditau.json")
        output["ditaujet_stats"] = self.local_target("rate_ditaujet.json")
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
        nevents = 0

        # summing summary plots
        nbinsX = self.ranges[0][1] - self.ranges[0][0]
        nbinsY = self.ranges[1][1] - self.ranges[1][0]
        nbinsZ = self.ranges[2][1] - self.ranges[2][0]

        # - summing the 1D plot for the DoubleTau trigger
        hmodel_ditau = ("histo_ditau", "; XX; Rate (kHz)", nbinsX, self.ranges[0][0], self.ranges[0][1])
        histo_ditau = ROOT.TH1F(*hmodel_ditau)

        # - summing the 3D plots for the DoubleTau + jet triggers
        hmodel_ditaujet = ("histo_ditaujet", "; XX; YY; ZZ",
            nbinsX, self.ranges[0][0], self.ranges[0][1],
            nbinsY, self.ranges[1][0], self.ranges[1][1],
            nbinsZ, self.ranges[2][0], self.ranges[2][1],
        )
        hmodel_ditaujet_or = ("histo_ditaujet_or", "; XX; YY; ZZ",
            nbinsX, self.ranges[0][0], self.ranges[0][1],
            nbinsY, self.ranges[1][0], self.ranges[1][1],
            nbinsZ, self.ranges[2][0], self.ranges[2][1],
        )
        histo_ditaujet = ROOT.TH3F(*hmodel_ditaujet)
        histo_ditaujet_or = ROOT.TH3F(*hmodel_ditaujet_or)

        for i, inp in enumerate(inputs.values()):
            for elem in inp.collection.targets.values():
                rootfile = ROOT.TFile.Open(elem["root"].path)
                histo_ditau.Add(rootfile.Get("histo_ditau").Clone())
                histo_ditaujet.Add(rootfile.Get("histo_ditaujet").Clone())
                histo_ditaujet_or.Add(rootfile.Get("histo_ditaujet_or").Clone())
                rootfile.Clone()
                if i == 0:
                    jsonfile = elem["stats"].path
                    with open(jsonfile) as f:
                        d = json.load(f)
                    nevents += d["nevents"]

        histo_ditau.Scale((2760. * 11246.) / (1000 * nevents))
        c = ROOT.TCanvas("", "", 800, 800)
        histo_ditau.Draw("")
        texts = get_labels(upper_right="           2018 Simulation (13 TeV)",
            inner_text=[self.dataset.process.label])
        for text in texts:
            text.Draw("same")
        c.SetLogy()
        c.SaveAs(create_file_dir(output["ditau"].path))
        del c
        d = {}
        for xx in range(self.ranges[0][0], self.ranges[0][1]):
            d[str(xx)] = histo_ditau.GetBinContent(xx - self.ranges[0][0] + 1)
        with open(create_file_dir(output["ditau_stats"].path), "w") as f:
            json.dump(d, f, indent=4)

        for (histo, histoname) in [(histo_ditaujet, "ditaujet"),
                (histo_ditaujet_or, "ditaujet_or")]:
            d = {}
            for xx, yy, zz in itertools.product(
                    range(self.ranges[0][0], self.ranges[0][1]),
                    range(self.ranges[1][0], self.ranges[1][1]),
                    range(self.ranges[2][0], self.ranges[2][1])):
                d["{}, {}, {}".format(xx, yy, zz)] = histo.GetBinContent(
                    xx - self.ranges[0][0] + 1,
                    yy - self.ranges[1][0] + 1,
                    zz - self.ranges[2][0] + 1) / (1000 * nevents) * (2760 * 11246)

            with open(create_file_dir(output["{}_stats".format(histoname)].path), "w") as f:
                json.dump(d, f, indent=4)

            for i, (label, var_range) in enumerate(zip(self.labels, self.ranges)):
                other_labels = deepcopy(self.labels)
                other_ranges = deepcopy(self.ranges)

                other_labels.remove(label)
                other_ranges.remove(var_range)

                x_binning = (other_ranges[0][1] - other_ranges[0][0],
                    other_ranges[0][0], other_ranges[0][1])

                y_binning = (other_ranges[1][1] - other_ranges[1][0],
                    other_ranges[1][0], other_ranges[1][1])

                for val in range(*var_range):
                    histo2D = ROOT.TH2F("{}_{}{}".format(histoname, label, val),
                        " ;" + "; ".join(other_labels),
                        x_binning[0], x_binning[1], x_binning[2],
                        y_binning[0], y_binning[1], y_binning[2])

                    for (xbin, ybin) in itertools.product(
                            range(*other_ranges[0]), range(*other_ranges[1])):
                        xbin -= other_ranges[0][0]
                        ybin -= other_ranges[1][0]
                        otherbin = val - var_range[0]
                        if i == 0:
                            (x, y, z) = (otherbin + 1, xbin + 1, ybin + 1)
                        elif i == 1:
                            (x, y, z) = (xbin + 1, otherbin + 1, ybin + 1)
                        elif i == 2:
                            (x, y, z) = (xbin + 1, ybin + 1, otherbin + 1)
                        histo2D.SetBinContent(xbin + 1, ybin + 1,
                            histo.GetBinContent(x, y, z))
                    histo2D.Scale((2760. * 11246.) / (1000 * nevents))
                    c = ROOT.TCanvas("", "", 800, 800)
                    histo2D.Draw("colz, text")
                    texts = get_labels(upper_right="           2018 Simulation (13 TeV)",
                        inner_text=[self.dataset.process.label, "{}={}".format(label, val)])
                    for text in texts:
                        text.Draw("same")
                    c.SaveAs(create_file_dir(output[histoname][(label, val)].path))
                    del histo2D, c


class Plot2DLimitRate(Plot2D):
    rate_version = luigi.Parameter(description="version of outputs to produce")
    rate_dataset_name = luigi.Parameter(description="dataset name used for rate studies",
        default="nu")
    rate_category_name = luigi.Parameter(description="category name used for rate studies",
        default="base")
    rate_threshold = luigi.FloatParameter(default=18., description="maximum rate threshold "
        "default: 18.")
    rate_low_percentage = luigi.FloatParameter(default=0.05, description="min allowed rate "
        "default: 0.05")

    def requires(self):
        reqs = {}
        reqs["acceptance"] = {}
        for xx in range(*self.xx_range):
            reqs["acceptance"][xx] = PlotAcceptance.req(self,
                version="{}_{}".format(self.version, xx), xx_range=(xx, xx + 1))
        reqs["rate"] = Plot2DRate.req(self, version=self.rate_version,
            dataset_name=self.rate_dataset_name, category_name=self.rate_category_name)
        return reqs

    def output(self):
        outputs = {
            "plot": self.local_target("plot2D_{}.pdf".format(str(self.rate_threshold).replace(
                ".", "_"))),
            "rateplot": self.local_target("rate_plot2D_{}.pdf".format(str(self.rate_threshold).replace(
                ".", "_"))),
            "json": self.local_target("plot2D_{}.json".format(str(self.rate_threshold).replace(
                ".", "_"))),
        }
        return outputs

    @law.decorator.notify
    def run(self):
        from copy import deepcopy
        import json
        from collections import OrderedDict
        ROOT = import_root()
        ROOT.gStyle.SetOptStat(0)
        ROOT.gStyle.SetPaintTextFormat("3.2f")
        inputs = self.input()
        output = self.output()
        den = 0

        nbinsX = self.ranges[0][1] - self.ranges[0][0]
        nbinsY = self.ranges[1][1] - self.ranges[1][0]
        nbinsZ = self.ranges[2][1] - self.ranges[2][0]
        hmodel = ("histo3D", "; XX; YY; ZZ",
            nbinsX, self.ranges[0][0], self.ranges[0][1],
            nbinsY, self.ranges[1][0], self.ranges[1][1],
            nbinsZ, self.ranges[2][0], self.ranges[2][1],
        )
        histo = ROOT.TH3F(*hmodel)
        for i, inp in enumerate(inputs["acceptance"].values()):
            for elem in inp.collection.targets.values():
                rootfile = ROOT.TFile.Open(elem["root"].path)
                histo.Add(rootfile.Get("histo").Clone())
                rootfile.Clone()
                jsonfile = elem["stats"].path
                if i == 0:
                    with open(jsonfile) as f:
                        d = json.load(f)
                    den += d["den"]

        with open(inputs["rate"]["ditaujet_or_stats"].path) as f: 
            rates = json.load(f)

        histo2D = ROOT.TH2F("histo", "; XX; YY", 
            nbinsX, self.ranges[0][0], self.ranges[0][1],
            nbinsY, self.ranges[1][0], self.ranges[1][1])
        ratehisto2D = ROOT.TH2F("ratehisto", "; XX; YY; ZZ", 
            nbinsX, self.ranges[0][0], self.ranges[0][1],
            nbinsY, self.ranges[1][0], self.ranges[1][1])
        dict_to_output = OrderedDict()
        for xx, yy in itertools.product(range(*self.ranges[0]), range(*self.ranges[1])):
            x = xx - self.ranges[0][0]
            y = yy - self.ranges[1][0]
            z = -1
            for zz in range(*self.ranges[2]):
                if rates["{}, {}, {}".format(xx, yy, zz)] < self.rate_threshold:
                    if ((self.rate_threshold - rates["{}, {}, {}".format(xx, yy, zz)]) 
                            / self.rate_threshold > self.rate_low_percentage):
                        print ("For triplet ({}, {}, {}) rate is too small ({} << {})".format(
                            xx, yy, zz, rates["{}, {}, {}".format(xx, yy, zz)], self.rate_threshold))
                        break
                    zz_to_use = zz
                    z = zz - self.ranges[2][0]
                    break
            if z == -1:
                # raise ValueError("No zz was found for pair ({}, {}) such as rate "
                #     "is smaller than {}".format(xx, yy, self.rate_threshold))
                print ("No zz was found for pair ({}, {}) such as rate "
                    "is smaller than {}".format(xx, yy, self.rate_threshold))
            else:
                histo2D.SetBinContent(x + 1, y + 1,
                    histo.GetBinContent(x + 1, y + 1, z + 1) / float(den))
                ratehisto2D.SetBinContent(x + 1, y + 1, zz_to_use)
                dict_to_output["{}, {}, {}".format(xx, yy, zz_to_use)] = (rates["{}, {}, {}".format(xx, yy, zz_to_use)],
                    histo.GetBinContent(x + 1, y + 1, z + 1) / float(den))

        c = ROOT.TCanvas("", "", 800, 800)
        histo2D.GetZaxis().SetRangeUser(0.8, 1.2)
        histo2D.Draw("text, colz")
        texts = get_labels(upper_right="           2018 Simulation (13 TeV)",
            inner_text=[self.dataset.process.label])
        for text in texts:
            text.Draw("same")
        c.SaveAs(create_file_dir(output["plot"].path))
        
        c.SetLeftMargin(0.1)
        c.SetRightMargin(0.15)
        ratehisto2D.GetZaxis().SetTitleOffset(1.5)
        ratehisto2D.Draw("text, colz")
        texts = get_labels(upper_right="           2018 Simulation (13 TeV)",
            inner_text=[self.dataset.process.label,
                "Rate#leq{}kHz".format(self.rate_threshold)
            ])
        for text in texts:
            text.Draw("same")
        c.SaveAs(create_file_dir(output["rateplot"].path))

        del ratehisto2D, histo2D, c

        with open(create_file_dir(output["json"].path), "w") as f:
            json.dump(dict_to_output, f, indent=4)


class DecoAcceptance(Plot2D):
    
    xx_range = law.CSVParameter(default=("2", "40"))

    def output(self):
        output = {}
        for xx, yy in itertools.product(range(*self.ranges[0]), range(*self.ranges[1])):
            output[(xx, yy)] = self.local_target("plot_xx{}_yy{}.pdf".format(xx, yy))
        return output








