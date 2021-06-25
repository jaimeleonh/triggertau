# coding: utf-8

import law
import luigi
import itertools
from analysis_tools.utils import (
    import_root, create_file_dir, join_root_selection
)

from plotting_tools.root.labels import get_labels

from cmt.base_tasks.base import ( 
    DatasetTaskWithCategory, DatasetWrapperTask, HTCondorWorkflow, InputData,
    ConfigTask, ConfigTaskWithCategory, 
)
from cmt.tasks.trigger import (
    AddTrigger, AddOffline, ComputeRate, ComputeAsymRate
)


class PlotAcceptance(DatasetTaskWithCategory, law.LocalWorkflow, HTCondorWorkflow):

    xx_range = law.CSVParameter(default=("32", "33"))
    yy_range = (20, 33)
    zz_range = (20, 160)    
    #xx_range = (20, 21)
    #yy_range = (20, 21)
    #zz_range = (20, 21)

    # regions not supported
    region_name = None
    tree_name = "Events"

    default_store = "$CMT_STORE_EOS_CATEGORIZATION"
    
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

        histo_den = df.Define(
                "DoubleIsoTau", "DoubleIsoTau32er2p1"
            ).Define(
                "den", "(DoubleIsoTau && offline.DoubleIsoTau32er2p1)"
            ).Histo1D(
                hmodel, "den"
            )

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

        outp = self.output()["root"].path
        f = ROOT.TFile(create_file_dir(outp), "RECREATE")
        num.Write()
        ditau.Write()
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
                texts = get_labels(upper_right="           {} Simulation (13 TeV)".format(
                        self.config.year),
                    inner_text=[self.dataset.process.label, "{}={}".format(label, val)])
                for text in texts:
                    text.Draw("same")
                c.SaveAs(create_file_dir(output[(label, val)].path))
                del histo2D, c


class PlotRate(PlotAcceptance):

    tree_name = "l1UpgradeTree/L1UpgradeTree"
    #tree_name = "l1UpgradeEmuTree/L1UpgradeTree"

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
        
        scaling = self.dataset.get_aux("rate_scaling")

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

        histo_ditau.Scale((scaling * 2760. * 11246.) / (1000 * nevents))
        c = ROOT.TCanvas("", "", 800, 800)
        histo_ditau.Draw("")
        texts = get_labels(upper_right=11 * " " + "{} Simulation (13 TeV)".format(
                self.config.year),
            inner_text=[self.dataset.process.label])
        for text in texts:
            text.Draw("same")
        #c.SetLogy()
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
                    zz - self.ranges[2][0] + 1) * (2760 * 11246 * scaling) / (1000 * nevents)

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
                    histo2D.Scale((scaling * 2760. * 11246.) / (1000 * nevents))
                    c = ROOT.TCanvas("", "", 800, 800)
                    histo2D.Draw("colz, text")
                    texts = get_labels(upper_right=11 * " " + "{} Simulation (13 TeV)".format(
                            self.config.year),
                        inner_text=[self.dataset.process.label, "{}={}".format(label, val)])
                    for text in texts:
                        text.Draw("same")
                    c.SaveAs(create_file_dir(output[histoname][(label, val)].path))
                    del histo2D, c


class RateTask():
    rate_version = luigi.Parameter(description="version of outputs to produce")
    rate_dataset_name = luigi.Parameter(description="dataset name used for rate studies",
        default="nu")
    rate_category_name = luigi.Parameter(description="category name used for rate studies",
        default="base")


class Plot2DLimitRate(Plot2D, RateTask):
    rate_threshold = luigi.FloatParameter(default=18., description="maximum rate threshold "
        "default: 18.")
    rate_low_percentage = luigi.FloatParameter(default=0.05, description="min allowed rate "
        "default: 0.05")

    rate_stats_name = "ditaujet_or_stats"

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

        with open(inputs["rate"][self.rate_stats_name].path) as f: 
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
        histo2D.GetZaxis().SetRangeUser(0.7, 1.2)
        histo2D.Draw("text, colz")
        texts = get_labels(upper_right="           {} Simulation (13 TeV)".format(
                self.config.year),
            inner_text=[self.dataset.process.label])
        for text in texts:
            text.Draw("same")
        c.SaveAs(create_file_dir(output["plot"].path))
        
        c.SetLeftMargin(0.1)
        c.SetRightMargin(0.15)
        ratehisto2D.GetZaxis().SetTitleOffset(1.5)
        ratehisto2D.Draw("text, colz")
        texts = get_labels(upper_right="           {} Simulation (13 TeV)".format(
                self.config.year),
            inner_text=[self.dataset.process.label,
                "Rate#leq{}kHz".format(self.rate_threshold)
            ])
        for text in texts:
            text.Draw("same")
        c.SaveAs(create_file_dir(output["rateplot"].path))

        del ratehisto2D, histo2D, c

        with open(create_file_dir(output["json"].path), "w") as f:
            json.dump(dict_to_output, f, indent=4)


class MapAcceptance(RateTask, DatasetWrapperTask):
    min_rate = luigi.FloatParameter(default=0., description="min allowed rate "
        "default: 0")
    max_rate = luigi.FloatParameter(default=20., description="max allowed rate "
        "default: 20")
    acceptance_ranges = law.CSVParameter(default=(1.,1.), description="allowed acceptance ranges, "
        "default: (1.,1.)")
    category_names = law.CSVParameter(default=(), description="names or name "
        "patterns of categories to use, uses all datasets when empty, default: ()")
    acceptance_version = luigi.Parameter(default="version", description="acceptance version, "
        "default: version")
    only_available_branches = luigi.BoolParameter(default=False, description="whether to run only "
        "using the available branche sinstead of producing all the required ones, default: False")
    xx_fixed = luigi.FloatParameter(default=-1., description="whether to show results "
        "only for 1 specific xx value, default: -1")
    yy_fixed = luigi.FloatParameter(default=-1., description="whether to show results "
        "only for 1 specific yy value, default: -1")
    zz_fixed = luigi.FloatParameter(default=-1., description="whether to show results "
        "only for 1 specific zz value, default: -1")
    npoints = luigi.IntParameter(default=-1, description="how many points to show in the plots "
        "default: -1 (All)")

    xx_range = Plot2D.xx_range
    
    rate_title = "DoubleIsoTauXX OR DoubleIsoTauYYJetZZ Rate (kHz)"
    acceptance_title = "Acceptance gain"

    rate_stats_name = "ditaujet_or_stats"
    histo_name = "histo"

    def __init__(self, *args, **kwargs):
        super(MapAcceptance, self).__init__(*args, **kwargs)
        assert len(self.acceptance_ranges) % 2 == 0
        self.acceptance_ranges = [
            [float(self.acceptance_ranges[i]), float(self.acceptance_ranges[i + 1])]
            for i in range(0, len(self.acceptance_ranges), 2)]
        assert len(self.acceptance_ranges) == len(self.datasets) or len(self.acceptance_ranges) == 1
        if len(self.acceptance_ranges) == 1:
            if self.acceptance_ranges[0][0] == 1 and self.acceptance_ranges[0][1] == 1:
                self.acceptance_ranges[0][0] = None
                self.acceptance_ranges[0][1] = None
            self.acceptance_ranges = [self.acceptance_ranges[0]
                for i in range(len(self.datasets))]

        self.categories = [self.config.categories.get(cat_name) for cat_name in self.category_names]
        assert len(self.categories) == len(self.datasets) or len(self.categories) == 1
        if len(self.categories) == 1:
            self.categories = [self.categories[0] for i in range(len(self.datasets))]

        self.xx_range = [int(xx) for xx in self.xx_range]
        self.ranges = [self.xx_range, PlotAcceptance.yy_range, PlotAcceptance.zz_range]

    def requires(self):
        reqs = {}
        for dataset, category in zip(self.datasets, self.categories):
            postfix = "{}_{}".format(dataset.name, category.name)
            reqs["acceptance_%s" % postfix] = {}
            available_branches = len(dataset.get_files())
            if self.only_available_branches:
                branches = []
                for i in range(available_branches):
                    ok = True
                    for xx in range(*self.xx_range):
                        if not PlotAcceptance.req(self,
                                version="{}_{}".format(self.acceptance_version, xx), dataset_name=dataset.name,
                                category_name=category.name, xx_range=(xx, xx + 1), branch=i).complete():
                            ok = False
                    if ok:
                        branches.append(i)
                for xx in range(*self.xx_range):
                    reqs["acceptance_%s" % postfix][xx] = PlotAcceptance.req(self,
                        version="{}_{}".format(self.acceptance_version, xx), dataset_name=dataset.name,
                        category_name=category.name, xx_range=(xx, xx + 1), branches=branches)
            else:
                for xx in range(*self.xx_range):
                    reqs["acceptance_%s" % postfix][xx] = PlotAcceptance.req(self,
                        version="{}_{}".format(self.acceptance_version, xx), dataset_name=dataset.name,
                        category_name=category.name, xx_range=(xx, xx + 1))
        reqs["rate"] = Plot2DRate.req(self, version=self.rate_version,
            dataset_name=self.rate_dataset_name, category_name=self.rate_category_name)
        return reqs

    def get_postfix(self, postfix):
        save_postfix = postfix
        if self.xx_fixed != -1:
            save_postfix += "_xx_" + str(self.xx_fixed)
        if self.yy_fixed != -1:
            save_postfix += "_yy_" + str(self.yy_fixed)
        if self.zz_fixed != -1:
            save_postfix += "_zz_" + str(self.zz_fixed)
        return save_postfix

    def output(self):
        outputs = {}
        for dataset, category in zip(self.datasets, self.categories):
            postfix = "{}_{}".format(dataset.name, category.name)
            save_postfix = self.get_postfix(postfix)
            outputs["plot_rate_%s" % postfix] = self.local_target("rate_vs_%s.pdf" % save_postfix)
            outputs["json_rate_%s" % postfix] = self.local_target("rate_vs_%s.json" % save_postfix)

        for i in range(len(self.datasets) - 1):
            for j in range(i + 1, len(self.datasets)):
                postfix = "{}_{}_{}_{}".format(self.datasets[i].name, self.categories[i].name,
                    self.datasets[j].name, self.categories[j].name)
                outputs["plot_%s" % postfix] = self.local_target("acceptance_%s.pdf" % save_postfix)
                outputs["json_%s" % postfix] = self.local_target("acceptance_%s.json" % save_postfix)
            
        return outputs

#    def complete(self):
#        return ConfigTask

    def plot(self, xaxis, yaxis, parameters, x_title, y_title, min_x, max_x, min_y, max_y, save_path):
        import matplotlib
        matplotlib.use("Agg")
        from matplotlib import pyplot as plt
        ax = plt.subplot()
        plt.plot(xaxis, yaxis, 'bo')
        for (x, y, label) in zip(xaxis, yaxis, parameters):
            plt.annotate(label, # this is the text
                 (x, y), # this is the point to label
                 textcoords="offset points", # how to position the text
                 xytext=(0, 10), # distance from text to points (x,y)
                 ha='center', # horizontal alignment can be left, right or center
                 size=5)
        plt.xlabel(x_title)
        plt.ylabel(y_title)

        x_text=0.05
        y_text=0.9
        plt.text(x_text, 1.02, "CMS", fontsize='large', fontweight='bold',
            transform=ax.transAxes)
        upper_text = "private work"
        plt.text(x_text + 0.1, 1.02, upper_text, transform=ax.transAxes)
        # text = [self.dataset.process.label.latex, self.category.label.latex]
        # for t in text:
            # plt.text(x_text, y_text, t, transform=ax.transAxes)
            # y_text -= 0.05

        if min_x and max_x:
            ax.set_xlim(min_x, max_x)
        if min_y and max_y:
            ax.set_ylim(min_y, max_y)

        plt.savefig(create_file_dir(save_path))
        plt.close('all')

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
        den = {}
        histos = {}

        nbinsX = self.ranges[0][1] - self.ranges[0][0]
        nbinsY = self.ranges[1][1] - self.ranges[1][0]
        nbinsZ = self.ranges[2][1] - self.ranges[2][0]

        for dataset, category in zip(self.datasets, self.categories):
            postfix = "{}_{}".format(dataset.name, category.name)
            den[(dataset, category)] = 0
            hmodel = ("histo3D_%s" % postfix, "; XX; YY; ZZ",
                nbinsX, self.ranges[0][0], self.ranges[0][1],
                nbinsY, self.ranges[1][0], self.ranges[1][1],
                nbinsZ, self.ranges[2][0], self.ranges[2][1],
            )
            histos[(dataset, category)] = deepcopy(ROOT.TH3F(*hmodel))
            
            for i, inp in enumerate(inputs["acceptance_%s" % postfix].values()):
                for elem in inp.collection.targets.values():
                    rootfile = ROOT.TFile.Open(elem["root"].path)
                    histos[(dataset, category)].Add(rootfile.Get(self.histo_name).Clone())
                    rootfile.Clone()
                    jsonfile = elem["stats"].path
                    if i == 0:
                        with open(jsonfile) as f:
                            d = json.load(f)
                        den[(dataset, category)] += d["den"]

        with open(inputs["rate"][self.rate_stats_name].path) as f: 
            rates = json.load(f)

        import matplotlib
        matplotlib.use("Agg") 
        from matplotlib import pyplot as plt

        acceptances_to_plot = OrderedDict()
        for dataset, category in zip(self.datasets, self.categories):
            acceptances_to_plot[(dataset, category)] = []

        for xx, yy, zz in itertools.product(range(*self.ranges[0]),
                range(*self.ranges[1]), range(*self.ranges[2])):
            if self.xx_fixed != -1 and self.xx_fixed != xx:
                continue
            if self.yy_fixed != -1 and self.yy_fixed != yy:
                continue
            if self.zz_fixed != -1 and self.zz_fixed != zz:
                continue

            if (rates["{}, {}, {}".format(xx, yy, zz)] > self.max_rate
                    or rates["{}, {}, {}".format(xx, yy, zz)] < self.min_rate):
                continue
            rate = rates["{}, {}, {}".format(xx, yy, zz)]
            parameters = "{}, {}, {}".format(xx, yy, zz)
            for dataset, category in zip(self.datasets, self.categories):
                acceptance = histos[(dataset, category)].GetBinContent(
                    xx - self.ranges[0][0] + 1,
                    yy - self.ranges[1][0] + 1,
                    zz - self.ranges[2][0] + 1
                ) / float(den[(dataset, category)])

                # if acceptance < self.min_acceptance or acceptance > self.max_acceptance:
                #     continue
                acceptances_to_plot[(dataset, category)].append((parameters, rate, acceptance))

        print "\n***********************************\n"

        for dataset, category, ranges in zip(self.datasets, self.categories,
                self.acceptance_ranges):
            # order by acceptance
            acceptances_sorted = deepcopy(acceptances_to_plot[(dataset, category)])
            acceptances_sorted.sort(key=lambda x:x[2], reverse=True)

            parameters = [x[0] for x in acceptances_sorted]
            rates = [x[1] for x in acceptances_sorted]
            acceptances = [x[2] for x in acceptances_sorted]

            bigger = len([acc for acc in acceptances if acc > ranges[1]])
            smaller = len([acc for acc in acceptances if acc < ranges[0]])

            print "({}, {}) -> >{}:{}, <{}:{}".format(dataset.name, category.name,
                ranges[1], bigger, ranges[0], smaller)

            postfix = "{}_{}".format(dataset.name, category.name)
            with open(create_file_dir(output["json_rate_%s" % postfix].path), "w") as f:
                json.dump(dict(zip(parameters, zip(rates, acceptances))), f, indent=4)
            if self.npoints != -1:
                parameters = parameters[:self.npoints]
                rates = rates[:self.npoints]
                acceptances = acceptances[:self.npoints]

            self.plot(rates, acceptances, parameters,
                self.rate_title, self.acceptance_title + " ({}, {})".format(
                    dataset.process.label.latex, category.label.latex),
                None, None, ranges[0], ranges[1], output["plot_rate_%s" % postfix].path)

        print "\n***********************************\n"

        for i in range(len(self.datasets) - 1):
            for j in range(i + 1, len(self.datasets)):
                (dataset_1, category_1, ranges_1) = (self.datasets[i],
                    self.categories[i], self.acceptance_ranges[i])
                (dataset_2, category_2, ranges_2) = (self.datasets[j],
                    self.categories[j], self.acceptance_ranges[j])
                postfix = "{}_{}_{}_{}".format(dataset_1.name, category_1.name,
                    dataset_2.name, category_2.name)

                acceptances_sorted = [(x[0], x[1], x[2], y[2]) for x, y in zip(
                    acceptances_to_plot[(dataset_1, category_1)],
                    acceptances_to_plot[(dataset_2, category_2)])]
                acceptances_sorted.sort(key=lambda x:x[2] + x[3], reverse=True)
                acceptances_1 = [x[2] for x in acceptances_sorted]
                acceptances_2 = [x[3] for x in acceptances_sorted]
                parameters = [x[0] for x in acceptances_sorted]

                with open(create_file_dir(output["json_%s" % postfix].path), "w") as f:
                    json.dump(dict(zip(parameters, zip(acceptances_1, acceptances_2))), f, indent=4)
                if self.npoints != -1:
                    acceptances_1 = acceptances_1[:self.npoints]
                    acceptances_2 = acceptances_2[:self.npoints]
                    acceptances_parameters = parameters[:self.npoints]

                self.plot(acceptances_1, acceptances_2, parameters,
                    self.acceptance_title + " ({}, {})".format(
                        dataset_1.process.label.latex, category_1.label.latex),
                    self.acceptance_title + " ({}, {})".format(
                        dataset_2.process.label.latex, category_2.label.latex),
                    ranges_1[0], ranges_1[1], ranges_2[0], ranges_2[1], output["plot_%s" % postfix].path)


class DecoAcceptance(Plot2D):
    
    xx_range = law.CSVParameter(default=("2", "40"))

    def output(self):
        output = {}
        output["ditau"] = self.local_target("plot_ditau_xx.pdf")
        for xx in range(*self.ranges[0]):
            output[("ditaujet", xx)] = self.local_target("plot_ditaujet_xx{}.pdf".format(xx))
        return output

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
        # hmodel = ("histo3D", "; XX; YY; ZZ",
            # nbinsX, self.ranges[0][0], self.ranges[0][1],
            # nbinsY, self.ranges[1][0], self.ranges[1][1],
            # nbinsZ, self.ranges[2][0], self.ranges[2][1],
        # )
        # histo = ROOT.TH3F(*hmodel)
        
        hmodel = ("ditau", "; XX; Acceptance; ",
            nbinsX, self.ranges[0][0], self.ranges[0][1]
        )
        ditau = ROOT.TH1F(*hmodel)

        hmodel = ("ditaujet", "; XX; ZZ",
            nbinsX, self.ranges[0][0], self.ranges[0][1],
            nbinsZ, self.ranges[2][0], self.ranges[2][1],
        )
        ditaujet = ROOT.TH2F(*hmodel)

        for i, inp in enumerate(inputs.values()):
            for elem in inp.collection.targets.values():
                rootfile = ROOT.TFile.Open(elem["root"].path)
                #histo.Add(rootfile.Get("histo").Clone())
                ditau.Add(rootfile.Get("ditau").Clone())
                ditaujet.Add(rootfile.Get("ditaujet").Clone())
                rootfile.Clone()
                jsonfile = elem["stats"].path
                if i == 0:
                    with open(jsonfile) as f:
                        d = json.load(f)
                    den += d["den"]

        c = ROOT.TCanvas("", "", 800, 800)
        ditau.Scale(1. / den)
        ditau.Draw()
        texts = get_labels(upper_right="           {} Simulation (13 TeV)".format(
                self.config.year),
            inner_text=[self.config.datasets.get(self.rate_dataset_name).process.label])
        for text in texts:
            text.Draw("same")
        c.SaveAs(create_file_dir(output["ditau"].path))
        del c, ditau

        hmodel = ("ditau", "; ZZ; Acceptance; ",
            nbinsZ, self.ranges[2][0], self.ranges[2][1]
        )
        for xx in range(*self.ranges[0]):
            x = xx - self.ranges[0][0]
            ditaujetxx = ROOT.TH1F(*hmodel)
            for zz in range(self.ranges[2][0], self.ranges[2][1]):
                z = zz - self.ranges[0][0]
                ditaujetxx.SetBinContent(z + 1, ditaujet.GetBinContent(x + 1, z + 1))
            ditaujetxx.Scale(1. / den)
            c = ROOT.TCanvas("", "", 800, 800)
            ditaujetxx.Draw()
            for text in texts:
                text.Draw("same")
            c.SaveAs(create_file_dir(output[("ditaujet", xx)].path))
            del ditaujetxx, c


class PlotNanoAODStuff(DatasetTaskWithCategory):

    tree_name = "Events"

    def requires(self):
        return InputData.req(self)

    def output(self):
        output = {}
        output["tau_pt"] = self.local_target("tau_pt.pdf")
        output["jet_pt"] = self.local_target("jet_pt.pdf")
        output["jet_pt_er2p5"] = self.local_target("jet_pt_er2p5.pdf")
        output["jet_eta"] = self.local_target("jet_eta.pdf")
        return output

    def add_to_root(self, root):
        return AddTrigger.add_to_root(root)

    def add_dataframe_definitions(self, df):
        df = AddTrigger.add_dataframe_definitions(df)
        restriction = ("L1Obj_type == 0 "
                    "&& maskDeltaR("
                        "L1Obj_eta, "
                        "L1Obj_phi, "
                        "lead_sublead_goodl1tau_eta, "
                        "lead_sublead_goodl1tau_phi, "
                        "0.5)")
        df = df.Define("lead_sublead_goodl1jet_eta",
            "lead_sublead("
                "L1Obj_pt[{0}],"
                "L1Obj_eta[{0}],"
                "L1Obj_phi[{0}],"
                "L1Obj_pt[{0}]"
            ")[1]".format(restriction))
        restriction = ("L1Obj_type == 0 "
                    "&& abs(L1Obj_eta) <= 2.5"
                    "&& maskDeltaR("
                        "L1Obj_eta, "
                        "L1Obj_phi, "
                        "lead_sublead_goodl1tau_eta, "
                        "lead_sublead_goodl1tau_phi, "
                        "0.5)")
        df = df.Define("lead_sublead_goodl1jet_pt_er2p5",
            "lead_sublead("
                "L1Obj_pt[{0}],"
                "L1Obj_eta[{0}],"
                "L1Obj_phi[{0}],"
                "L1Obj_pt[{0}]"
            ")[0]".format(restriction))
        return df

    def run(self):
        from copy import deepcopy
        import json
        ROOT = import_root()
        ROOT.gStyle.SetOptStat(0)
        ROOT.ROOT.EnableImplicitMT()
        inputs = self.input()
        output = self.output()

        dataframe_files = ROOT.vector(str)()
        for elem in inputs.targets:
            dataframe_files.push_back(elem.path)
        df = ROOT.RDataFrame(self.tree_name, dataframe_files)

        ROOT = self.add_to_root(ROOT)
        df = self.add_dataframe_definitions(df)

        hmodel = ("jet_pt", "; pt [GeV]; Events / 4 GeV", 20, 20, 100)

        leading_tau_pt = df.Define("leading_tau_pt",
            "lead_sublead_goodl1tau_pt[0]").Histo1D(hmodel, "leading_tau_pt")
        subleading_tau_pt = df.Define("subleading_tau_pt",
            "lead_sublead_goodl1tau_pt[1]").Histo1D(hmodel, "subleading_tau_pt")

        leading_pt = df.Define("leading_pt",
            "lead_sublead_goodl1jet_pt[0]").Histo1D(hmodel, "leading_pt")
        subleading_pt = df.Define("subleading_pt",
            "lead_sublead_goodl1jet_pt[1]").Histo1D(hmodel, "subleading_pt")

        leading_pt_er2p5 = df.Define("leading_pt",
            "lead_sublead_goodl1jet_pt_er2p5[0]").Histo1D(hmodel, "leading_pt")
        subleading_pt_er2p5 = df.Define("subleading_pt",
            "lead_sublead_goodl1jet_pt_er2p5[1]").Histo1D(hmodel, "subleading_pt")

        hmodel = ("jet_eta", "; #eta ; Events / 0.2", 50, -5, 5)
        leading_eta = df.Define("leading_eta", "lead_sublead_goodl1jet_eta[0]").Histo1D(hmodel, "leading_eta")
        subleading_eta = df.Define("subleading_eta", "lead_sublead_goodl1jet_eta[1]").Histo1D(hmodel, "subleading_eta")
        
        name_plots = [
            ("tau_pt", (leading_tau_pt, subleading_tau_pt)),
            ("jet_pt", (leading_pt, subleading_pt)),
            ("jet_pt_er2p5", (leading_pt_er2p5, subleading_pt_er2p5)),
            ("jet_eta", (leading_eta, subleading_eta))
        ]
        
        for name, plots in name_plots:
            c = ROOT.TCanvas("", "", 800, 800)
            leg = ROOT.TLegend(0.7, 0.7, 0.9, 0.9)
            leading = plots[0].Clone()
            subleading = plots[1].Clone()
            leading.SetLineColor(ROOT.kBlue)
            subleading.SetLineColor(ROOT.kRed)
            leg.AddEntry(leading, "Leading L1 " + ("jet" if "jet" in name else "#tau"), "l")
            leg.AddEntry(subleading, "Subleading L1 " + ("jet" if "jet" in name else "#tau"), "l")

            if leading_pt.GetMaximum() < subleading_pt.GetMaximum():
                subleading.Draw()
                leading.Draw("same")
            else:
                leading.Draw()
                subleading.Draw("same")

            texts = get_labels(upper_right="           {} Simulation (13 TeV)".format(
                    self.config.year),
                inner_text=[self.config.datasets.get(self.dataset_name).process.label])
            for text in texts:
                text.Draw("same")

            leg.Draw("same")

            c.SaveAs(create_file_dir(output[name].path))
            del c, leading, subleading


class PlotL1TStuff(PlotNanoAODStuff):
    tree_name = "l1UpgradeEmuTree/L1UpgradeTree"
    
    def add_to_root(self, root):
        return ComputeRate.add_to_root(root)

    def add_dataframe_definitions(self, df):
        df = ComputeRate.add_dataframe_definitions(df)
        restriction = (
            "maskDeltaR("
                "jetEta, "
                "jetPhi, "
                "lead_sublead_goodl1tau_eta, "
                "lead_sublead_goodl1tau_phi, "
                "0.5)")
        df = df.Define("lead_sublead_goodl1jet_eta",
            "lead_sublead("
                "jetEt[{0}], "
                "jetEta[{0}], "
                "jetPhi[{0}], "
                "jetEt[{0}]"
            ")[1]".format(restriction))
        restriction = (
            "abs(jetEta) <= 2.5"
            "&& maskDeltaR("
                "jetEta, "
                "jetPhi, "
                "lead_sublead_goodl1tau_eta, "
                "lead_sublead_goodl1tau_phi, "
                "0.5)")
        df = df.Define("lead_sublead_goodl1jet_pt_er2p5",
            "lead_sublead("
                "jetEt[{0}], "
                "jetEta[{0}], "
                "jetPhi[{0}], "
                "jetEt[{0}]"
            ")[0]".format(restriction))
        return df

