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
    AddDiJetTrigger, AddDiJetOffline, ComputeDiJetRate
)

from cmt.tasks.plotting import (
    PlotAcceptance, Plot2D, Plot2DLimitRate, MapAcceptance
)


class PlotDiJetAcceptance(DatasetTaskWithCategory, law.LocalWorkflow, HTCondorWorkflow):

    xx_range = law.CSVParameter(default=("32", "33"))
    yy_range = (20, 33)
    zz_range = (20, 100)

    # regions not supported
    region_name = None
    tree_name = "Events"

    default_store = "$CMT_STORE_EOS_CATEGORIZATION"
    
    def __init__(self, *args, **kwargs):
        super(PlotDiJetAcceptance, self).__init__(*args, **kwargs)
        self.xx_range = [int(xx) for xx in self.xx_range]

    def create_branch_map(self):
        return len(self.dataset.get_files())

    def workflow_requires(self):
        return {
            "trigger": AddDiJetTrigger.vreq(self, _prefer_cli=["workflow"]),
            "offline": AddDiJetOffline.vreq(self, _prefer_cli=["workflow"])
        }

    def requires(self):
        return {
            "trigger": AddDiJetTrigger.vreq(self, branch=self.branch, _prefer_cli=["workflow"]),
            "offline": AddDiJetOffline.vreq(self, branch=self.branch, _prefer_cli=["workflow"])
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
                    "offline.DoubleIsoTau{0}er2p1Jet{1}dR0p5Jet{1}dR0p5".format(yy, zz)
                ).Define(
                    "DoubleIsoTau", "DoubleIsoTau{0}er2p1".format(xx)
                ).Define(
                    "pass",
                    "(DoubleIsoTau && offline.DoubleIsoTau{0}er2p1) "
                        "|| (DoubleIsoTau{1}er2p1Jet{2}dR0p5Jet{2}dR0p5 && dum)".format(xx, yy, zz)
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


class PlotDiJet2D(Plot2D):

    def __init__(self, *args, **kwargs):
        super(Plot2D, self).__init__(*args, **kwargs)
        self.xx_range = [int(xx) for xx in self.xx_range]
        self.labels = ["xx", "yy", "zz"]   
        self.ranges = [self.xx_range, PlotDiJetAcceptance.yy_range, PlotDiJetAcceptance.zz_range]

    def requires(self):
        reqs = {}
        for xx in range(*self.xx_range):
            reqs[xx] = PlotDiJetAcceptance.req(self, version="{}_{}".format(self.version, xx),
                xx_range=(xx, xx + 1))
        return reqs


class PlotDiJetRate(PlotAcceptance):

    tree_name = "l1UpgradeTree/L1UpgradeTree"
    xx_range = law.CSVParameter(default=("32", "33"))
    yy_range = (20, 33)
    zz_range = (20, 100)    


    def workflow_requires(self):
        return {
            "trigger": ComputeDiJetRate.vreq(self, _prefer_cli=["workflow"])
        }

    def requires(self):
        return {
            "trigger": ComputeDiJetRate.vreq(self, branch=self.branch, _prefer_cli=["workflow"])
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
        histos_ditau_dijet_or = {}
        histos_ditau_ditau_dijet_or = {}
        hmodel = ("", "", 1, 1, 2)

        # filling plots for DoubleTau trigger
        for xx in range(*self.xx_range):
            histos_ditau[xx] = df.Define(
                "pass", "DoubleIsoTau{0}er2p1".format(xx)
            ).Histo1D(hmodel, "pass")

        for yy, zz in itertools.product(
                range(*self.yy_range), range(*self.zz_range)):
            histos_ditau_dijet_or[(yy, zz)] = df.Define(
                    "pass",
                    "DoubleIsoTau{0}er2p1Jet{1}dR0p5Jet{1}dR0p5".format(yy, zz)
                ).Histo1D(hmodel, "pass")

        # filling plots for DoubleTau + jet w/o and w/ overlap removal
        for xx, yy, zz in itertools.product(
                range(*self.xx_range), range(*self.yy_range), range(*self.zz_range)):
            histos_ditau_ditau_dijet_or[(xx, yy, zz)] = df.Define(
                    "pass",
                    "DoubleIsoTau{0}er2p1 || DoubleIsoTau{1}er2p1Jet{2}dR0p5Jet{2}dR0p5".format(
                        xx, yy, zz)
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
        hmodel_ditau_dijet = ("histo_ditau_dijet_or", "; XX; YY; ZZ",
            nbinsY, self.yy_range[0], self.yy_range[1],
            nbinsZ, self.zz_range[0], self.zz_range[1],
        )
        hmodel_ditau_ditau_dijet = ("histo_ditau_ditau_dijet_or", "; XX; YY; ZZ",
            nbinsX, self.xx_range[0], self.xx_range[1],
            nbinsY, self.yy_range[0], self.yy_range[1],
            nbinsZ, self.zz_range[0], self.zz_range[1],
        )
        histo_ditau_dijet = ROOT.TH2F(*hmodel_ditau_dijet)
        for yy, zz in itertools.product(
                range(*self.yy_range), range(*self.zz_range)):
            histo_ditau_dijet.Fill(yy, zz, histos_ditau_dijet_or[(yy, zz)].Integral())
        
        histo_ditau_ditau_dijet = ROOT.TH3F(*hmodel_ditau_ditau_dijet)
        for xx, yy, zz in itertools.product(
                range(*self.xx_range), range(*self.yy_range), range(*self.zz_range)):
            histo_ditau_ditau_dijet.Fill(
                xx, yy, zz, histos_ditau_ditau_dijet_or[(xx, yy, zz)].Integral())

        # saving output plots in root files
        outp = self.output()["root"].path
        f = ROOT.TFile(create_file_dir(outp), "RECREATE")
        histo_ditau.Write()
        histo_ditau_dijet.Write()
        histo_ditau_ditau_dijet.Write()
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


class PlotDiJet2DRate(DatasetTaskWithCategory):
    
    xx_range = law.CSVParameter(default=("32", "40"))

    def __init__(self, *args, **kwargs):
        super(PlotDiJet2DRate, self).__init__(*args, **kwargs)
        self.xx_range = [int(xx) for xx in self.xx_range]
        self.labels = ["xx", "yy", "zz"]   
        self.ranges = [self.xx_range, PlotDiJetRate.yy_range, PlotDiJetRate.zz_range]

    def requires(self):
        reqs = {}
        for xx in range(*self.xx_range):
            reqs[xx] = PlotDiJetRate.req(self, version="{}_{}".format(self.version, xx),
                xx_range=(xx, xx + 1))
        return reqs

    def output(self):
        output = {
            "ditau_ditau_dijet_or": {}
        }

        output["ditau"] = self.local_target("rate_ditau.pdf")
        output["ditau_dijet_or"] = self.local_target("rate_ditau_dijet_or.pdf")
        for label, var_range in zip(self.labels, self.ranges):
            for val in range(*var_range):
                output["ditau_ditau_dijet_or"][(label, val)] = self.local_target(
                    "rate_ditau_ditau_dijet_or_{}{}.pdf".format(label, val))

        output["ditau_stats"] = self.local_target("rate_ditau.json")
        output["ditau_dijet_or_stats"] = self.local_target("rate_ditau_dijet_or.json")
        output["ditau_ditau_dijet_or_stats"] = self.local_target("rate_ditau_ditau_dijet_or.json")
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
        hmodel_ditau = ("histo_ditau", "; XX; Rate (kHz)",
            nbinsX, self.ranges[0][0], self.ranges[0][1])
        histo_ditau = ROOT.TH1F(*hmodel_ditau)

        # - summing the 3D plots for the DoubleTau + jet triggers
        hmodel_ditau_dijet_or = ("histo_ditau_dijet_or", "; YY; ZZ",
            nbinsY, self.ranges[1][0], self.ranges[1][1],
            nbinsZ, self.ranges[2][0], self.ranges[2][1],
        )
        hmodel_ditau_ditau_dijet_or = ("histo_ditau_ditau_dijet_or", "; XX; YY; ZZ",
            nbinsX, self.ranges[0][0], self.ranges[0][1],
            nbinsY, self.ranges[1][0], self.ranges[1][1],
            nbinsZ, self.ranges[2][0], self.ranges[2][1],
        )
        histo_ditau_dijet_or = ROOT.TH2F(*hmodel_ditau_dijet_or)
        histo_ditau_ditau_dijet_or = ROOT.TH3F(*hmodel_ditau_ditau_dijet_or)

        for i, inp in enumerate(inputs.values()):
            for elem in inp.collection.targets.values():
                rootfile = ROOT.TFile.Open(elem["root"].path)
                histo_ditau.Add(rootfile.Get("histo_ditau").Clone())
                histo_ditau_dijet_or.Add(rootfile.Get("histo_ditau_dijet_or").Clone())
                histo_ditau_ditau_dijet_or.Add(rootfile.Get("histo_ditau_ditau_dijet_or").Clone())
                rootfile.Clone()
                if i == 0:
                    jsonfile = elem["stats"].path
                    with open(jsonfile) as f:
                        d = json.load(f)
                    nevents += d["nevents"]

        # ditau
        histo_ditau.Scale((scaling * 2760. * 11246.) / (1000 * nevents))
        c = ROOT.TCanvas("", "", 800, 800)
        histo_ditau.Draw("")
        texts = get_labels(upper_right="           {} Simulation (13 TeV)".format(
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

        # ditau + dijet
        histo_ditau_dijet_or.Scale((scaling * 2760. * 11246.) / (1000 * nevents))
        c = ROOT.TCanvas("", "", 800, 800)
        histo_ditau_dijet_or.Draw("colz")
        texts = get_labels(upper_right="           {} Simulation (13 TeV)".format(
                self.config.year),
            inner_text=[self.dataset.process.label])
        for text in texts:
            text.Draw("same")
        #c.SetLogy()
        c.SaveAs(create_file_dir(output["ditau_dijet_or"].path))
        del c

        d = {}
        for yy in range(self.ranges[1][0], self.ranges[1][1]):
            for zz in range(self.ranges[2][0], self.ranges[2][1]):
                d["{}, {}".format(yy, zz)] = histo_ditau_dijet_or.GetBinContent(
                    yy - self.ranges[1][0] + 1, zz - self.ranges[2][0] + 1)
        with open(create_file_dir(output["ditau_dijet_or_stats"].path), "w") as f:
            json.dump(d, f, indent=4)

        for (histo, histoname) in [(histo_ditau_ditau_dijet_or, "ditau_ditau_dijet_or")]:
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
                    texts = get_labels(upper_right="           {} Simulation (13 TeV)".format(
                            self.config.year),
                        inner_text=[self.dataset.process.label, "{}={}".format(label, val)])
                    for text in texts:
                        text.Draw("same")
                    c.SaveAs(create_file_dir(output[histoname][(label, val)].path))
                    del histo2D, c


class PlotDiJet2DLimitRate(Plot2DLimitRate):

    rate_stats_name = "ditau_ditau_dijet_or_stats"

    def requires(self):
        reqs = {}
        reqs["acceptance"] = {}
        for xx in range(*self.xx_range):
            reqs["acceptance"][xx] = PlotDiJetAcceptance.req(self,
                version="{}_{}".format(self.version, xx), xx_range=(xx, xx + 1))
        reqs["rate"] = PlotDiJet2DRate.req(self, version=self.rate_version,
            dataset_name=self.rate_dataset_name, category_name=self.rate_category_name)
        return reqs


class MapDiJetAcceptance(MapAcceptance):
    xx_range = PlotDiJet2D.xx_range

    rate_title = "DoubleIsoTauXX OR DoubleIsoTauYYDoubleJetZZ Rate (kHz)"
    acceptance_title = "Acceptance gain"

    histo_name = "histo"
    rate_stats_name = "ditau_ditau_dijet_or_stats"

    def __init__(self, *args, **kwargs):
        super(MapDiJetAcceptance, self).__init__(*args, **kwargs)
        self.ranges = [self.xx_range, PlotDiJetAcceptance.yy_range, PlotDiJetAcceptance.zz_range]

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
                        if not PlotDiJetAcceptance.req(self,
                                version="{}_{}".format(self.acceptance_version, xx), dataset_name=dataset.name,
                                category_name=category.name, xx_range=(xx, xx + 1), branch=i).complete():
                            ok = False
                    if ok:
                        branches.append(i)
                for xx in range(*self.xx_range):
                    reqs["acceptance_%s" % postfix][xx] = PlotDiJetAcceptance.req(self,
                        version="{}_{}".format(self.acceptance_version, xx), dataset_name=dataset.name,
                        category_name=category.name, xx_range=(xx, xx + 1), branches=branches)
            else:
                for xx in range(*self.xx_range):
                    reqs["acceptance_%s" % postfix][xx] = PlotDiJetAcceptance.req(self,
                        version="{}_{}".format(self.acceptance_version, xx), dataset_name=dataset.name,
                        category_name=category.name, xx_range=(xx, xx + 1))
        reqs["rate"] = PlotDiJet2DRate.req(self, version=self.rate_version,
            dataset_name=self.rate_dataset_name, category_name=self.rate_category_name)
        return reqs
