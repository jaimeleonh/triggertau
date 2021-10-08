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
    AddTrigger, AddOffline, AddDiJetTrigger, AddDiJetOffline, ComputeRate, ComputeDiJetRate,
    Rate, AsymmRate, AsymmVBFRate, AsymmDiJetRate, AsymmVBFDiJetRate, Acceptance, AsymmAcceptance,
    AsymmVBFAcceptance, AsymmKetiAcceptance, AsymmDiJetAcceptance, AsymmVBFDiJetAcceptance,
    AsymmKetiDiJetAcceptance,
)
from cmt.tasks.plotting import PlotAcceptance, Plot2D, PlotRate, MapAcceptance  


class PlotTotalAcceptance(PlotAcceptance):

    yy_range = AddTrigger.yy_range
    zz_range = AddTrigger.zz_range
    yyp_range = AddDiJetTrigger.yy_range
    zzp_range = AddDiJetTrigger.zz_range

    def __init__(self, *args, **kwargs):
        super(PlotTotalAcceptance, self).__init__(*args, **kwargs)

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


class PlotTotal2DRate(Plot2D):
    min_rate = luigi.FloatParameter(default=0., description="min allowed rate "
        "default: 0")
    max_rate = luigi.FloatParameter(default=20., description="max allowed rate "
        "default: 20")
    xx_range = Rate.xx_range
    yy_range = Rate.yy_range
    zz_range = Rate.zz_range
    dzz_range = Rate.dzz_range

    def __init__(self, *args, **kwargs):
        super(PlotTotal2DRate, self).__init__(*args, **kwargs)

    def requires(self):
        return Rate.req(self)

    def output(self):
        output = self.local_target("rate.json")
        return output

    def run(self):
        from copy import deepcopy
        import json
        ROOT = import_root()
        
        inputs = self.input()
        output = self.output()
        nevents = 0
        
        # Create histos for ditau or ditau_jet or ditau_dijet
        nbinsZ = self.zz_range[1] - self.zz_range[0]
        nbinsdZ = self.dzz_range[1] - self.dzz_range[0]
        histos = {}
        for x, y, dy in itertools.product(
                range(*self.xx_range), range(*self.yy_range), range(*self.yy_range)):
            hmodel = ("histo_{}_{}_{}".format(x, y, dy), "; YYp; ZZp",
                nbinsZ, self.zz_range[0], self.zz_range[1],
                nbinsdZ, self.dzz_range[0], self.dzz_range[1],
            )
            histos["%s, %s, %s" % (x, y, dy)] = ROOT.TH2F(*hmodel)
        for elem in inputs.collection.targets.values():
            rootfile = ROOT.TFile.Open(elem["root"].path)
            for x, y, dy in itertools.product(
                    range(*self.xx_range), range(*self.yy_range), range(*self.yy_range)):
                histos["%s, %s, %s" % (x, y, dy)].Add(rootfile.Get(
                    "histo_ditau_{0}_{0}__ditau_{1}_{1}_jet__ditau_{2}_{2}_dijet".format(
                        x, y, dy)).Clone())
            rootfile.Close()
            jsonfile = elem["stats"].path
            with open(jsonfile) as f:
                d = json.load(f)
            nevents += d["nevents"]

        scaling = self.dataset.get_aux("rate_scaling")
        rate_dict = {} 
        for name, histo in histos.items():
            rate_dict[name] = {}
            histo.Scale((scaling * 2760. * 11246.) / (1000 * nevents))
            #print histo.Integral()
            for z, dz in itertools.product(range(*self.zz_range), range(*self.dzz_range)):
                rate = histo.GetBinContent(z - self.zz_range[0] + 1, dz - self.dzz_range[0] + 1)
                #print rate
                if rate >= self.min_rate and rate <= self.max_rate:
                    rate_dict[name]["%s, %s" % (z, dz)] = rate
        
        stats_path = self.output().path
        with open(create_file_dir(stats_path), "w") as json_f:
            json.dump(rate_dict, json_f, indent=0)


class MapTotalAcceptance(MapAcceptance):
    dyy_fixed = luigi.FloatParameter(default=-1., description="whether to show results "
        "only for 1 specific dyy value, default: -1")
    dzz_fixed = luigi.FloatParameter(default=-1., description="whether to show results "
        "only for 1 specific dzz value, default: -1")
    dzzp_fixed = luigi.FloatParameter(default=-1., description="whether to show results "
        "only for 1 specific dzzp value, default: -1")
        
    xx_range = Rate.xx_range
    yy_range = Rate.yy_range
    zz_range = Rate.zz_range
    dzz_range = Rate.dzz_range

    rate_title = "DoubleIsoTauXX OR DoubleIsoTauYYJetZZ OR DoubleIsoTauDYYJetDZZJetDZZ' Rate (kHz)"
    acceptance_title = "Acceptance gain"

    def requires(self):
        reqs = {}
        for dataset, category in zip(self.datasets, self.categories):
            postfix = "{}_{}".format(dataset.name, category.name)
            available_branches = len(dataset.get_files())
            if self.only_available_branches:
                branches = []
                for i in range(available_branches):
                    ok = True
                    if Acceptance.req(self, version=self.acceptance_version,
                            dataset_name=dataset.name, category_name=category.name,
                            branch=i).complete():
                        branches.append(i)
                reqs["acceptance_%s" % postfix] = Acceptance.req(self,
                    version=self.acceptance_version, dataset_name=dataset.name,
                    category_name=category.name, branches=branches)
            else:
                reqs["acceptance_%s" % postfix] = Acceptance.req(self,
                    version=self.acceptance_version, dataset_name=dataset.name,
                    category_name=category.name)
        # reqs["rate"] = PlotTotal2DRate.req(self, version=self.rate_version,
        reqs["rate"] = Rate.req(self, version=self.rate_version,
            dataset_name=self.rate_dataset_name, category_name=self.rate_category_name)
        return reqs

    def get_postfix(self, postfix):
        save_postfix = super(MapTotalAcceptance, self).get_postfix(postfix)
        if self.dyy_fixed != -1:
            save_postfix += "__dyy_" + str(self.dyy_fixed)
        if self.dzz_fixed != -1:
            save_postfix += "__dzz_" + str(self.dzz_fixed)
        if self.dzzp_fixed != -1:
            save_postfix += "__dzzp_" + str(self.dzzp_fixed)
        return save_postfix

    def is_fixed(self, xx, yy, dyy, zz = -1, dzz = -1, dzzp = -1):
        value = super(MapTotalAcceptance, self).is_fixed(xx, yy, zz)
        if self.dyy_fixed != -1 and dyy != self.dyy_fixed:
            return False
        if self.dzz_fixed != -1 and dzz != self.dzz_fixed and dzz != -1:
            return False
        if self.dzzp_fixed != -1 and dzzp != self.dzzp_fixed and dzzp != -1:
            return False
        return value

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

        # Create histos for ditau or ditau_jet or ditau_dijet
        nbinsZ = self.zz_range[1] - self.zz_range[0]
        nbinsdZ = self.dzz_range[1] - self.dzz_range[0]
        histos = {}
        # acceptance computation
        for dataset, category in zip(self.datasets, self.categories):
            postfix = "{}_{}".format(dataset.name, category.name)
            den[(dataset, category)] = 0
            histos[(dataset, category)] = {}
            for x, y, dy in itertools.product(
                    range(*self.xx_range), range(*self.yy_range), range(*self.yy_range)):
                if not self.is_fixed(x, y, dy):
                    continue
                hmodel = ("histo_{}_{}_{}_{}".format(postfix, x, y, dy), "; YYp; ZZp",
                    nbinsZ, self.zz_range[0], self.zz_range[1],
                    nbinsdZ, self.dzz_range[0], self.dzz_range[1],
                    nbinsdZ, self.dzz_range[0], self.dzz_range[1]
                )
                histos[(dataset, category)]["%s, %s, %s" % (x, y, dy)] = ROOT.TH3F(*hmodel)
            for elem in inputs["acceptance_%s" % postfix].collection.targets.values():
                rootfile = ROOT.TFile.Open(elem["root"].path)
                for x, y, dy in itertools.product(
                        range(*self.xx_range), range(*self.yy_range), range(*self.yy_range)):
                    if not self.is_fixed(x, y, dy):
                        continue
                    histos[(dataset, category)]["%s, %s, %s" % (x, y, dy)].Add(rootfile.Get(
                        "histo_ditau_{0}_{0}__ditau_{1}_{1}_jet__ditau_{2}_{2}_dijet".format(
                            x, y, dy)).Clone())
                rootfile.Close()
                jsonfile = elem["stats"].path
                with open(jsonfile) as f:
                    d = json.load(f)
                den[(dataset, category)] += d["den"]

        # rate computation
        histos["rate"] = {}
        scaling = self.config.datasets.get(self.rate_dataset_name).get_aux("rate_scaling")
        for x, y, dy in itertools.product(
                range(*self.xx_range), range(*self.yy_range), range(*self.yy_range)):
            if not self.is_fixed(x, y, dy):
                continue
            hmodel = ("histo_{}_{}_{}".format(x, y, dy), "; ZZ; dZZ; dZZp",
                nbinsZ, self.zz_range[0], self.zz_range[1],
                nbinsdZ, self.dzz_range[0], self.dzz_range[1],
                nbinsdZ, self.dzz_range[0], self.dzz_range[1]
            )
            histos["rate"]["%s, %s, %s" % (x, y, dy)] = ROOT.TH3F(*hmodel)

        nevents = 0
        for elem in inputs["rate"].collection.targets.values():
            rootfile = ROOT.TFile.Open(elem["root"].path)
            for x, y, dy in itertools.product(
                    range(*self.xx_range), range(*self.yy_range), range(*self.yy_range)):
                if not self.is_fixed(x, y, dy):
                    continue
                histos["rate"]["%s, %s, %s" % (x, y, dy)].Add(rootfile.Get(
                    "histo_ditau_{0}_{0}__ditau_{1}_{1}_jet__ditau_{2}_{2}_dijet".format(
                        x, y, dy)).Clone())
            rootfile.Close()
            jsonfile = elem["stats"].path
            with open(jsonfile) as f:
                d = json.load(f)
            nevents += d["nevents"]

        acceptances_to_plot = OrderedDict()
        for dataset, category in zip(self.datasets, self.categories):
            acceptances_to_plot[(dataset, category)] = []

        for x, y, dy in itertools.product(
                range(*self.xx_range), range(*self.yy_range), range(*self.yy_range)):
            if not self.is_fixed(x, y, dy):
                    continue
            histos["rate"]["%s, %s, %s" % (x, y, dy)].Scale((scaling * 2760. * 11246.) / (1000 * nevents))
            for z, dz in itertools.product(range(*self.zz_range), range(*self.dzz_range)):
                for dzp in range(self.dzz_range[0], dz + 1):
                    if not self.is_fixed(x, y, dy, z, dz, dzp):
                        continue
                    rate = round(histos["rate"]["%s, %s, %s" % (x, y, dy)].GetBinContent(
                        z - self.zz_range[0] + 1,
                        dz - self.dzz_range[0] + 1,
                        dzp - self.dzz_range[0] + 1), 3)
                    if (rate > self.max_rate or rate < self.min_rate):
                        continue
                    parameters = "{},{},{},{},{},{}".format(x, y, z, dy, dz, dzp)
                    for dataset, category in zip(self.datasets, self.categories):
                        acceptance = round(histos[(dataset, category)]["%s, %s, %s" % (x, y, dy)].GetBinContent(
                            z - self.zz_range[0] + 1,
                            dz - self.dzz_range[0] + 1,
                            dzp - self.dzz_range[0] + 1
                        ) / float(den[(dataset, category)]), 3)

                        # if acceptance < self.min_acceptance or acceptance > self.max_acceptance:
                        #     continue
                        acceptances_to_plot[(dataset, category)].append((parameters, rate, acceptance))

        self.plot_stuff(acceptances_to_plot)


class MapAsymmAcceptance(MapAcceptance):
    xx_symmetric = luigi.BoolParameter(default=False, description="whether to remove "
        "the asymmetry in the xx parameter, default: False")
    yy_symmetric = luigi.BoolParameter(default=False, description="whether to remove "
        "the asymmetry in the yy parameter, default: False")
    xxp_fixed = luigi.FloatParameter(default=-1., description="whether to show results "
        "only for 1 specific xxp value, default: -1")
    yyp_fixed = luigi.FloatParameter(default=-1., description="whether to show results "
        "only for 1 specific yyp value, default: -1")

    xx_range = AsymmRate.xx_range
    yy_range = AsymmRate.yy_range
    zz_range = AsymmRate.zz_range

    rate_title = "IsoTauXXIsoTauXX' OR IsoTauYYIsoTauYY'JetZZ Rate (kHz)"
    acceptance_title = "Acceptance gain"

    def requires(self):
        reqs = {}
        for dataset, category in zip(self.datasets, self.categories):
            postfix = "{}_{}".format(dataset.name, category.name)
            available_branches = len(dataset.get_files())
            if self.only_available_branches:
                branches = []
                for i in range(available_branches):
                    ok = True
                    if AsymmAcceptance.req(self, version=self.acceptance_version,
                            dataset_name=dataset.name, category_name=category.name,
                            branch=i).complete():
                        branches.append(i)
                reqs["acceptance_%s" % postfix] = AsymmAcceptance.req(self,
                    version=self.acceptance_version, dataset_name=dataset.name,
                    category_name=category.name, branches=branches)
            else:
                reqs["acceptance_%s" % postfix] = AsymmAcceptance.req(self,
                    version=self.acceptance_version, dataset_name=dataset.name,
                    category_name=category.name)
        reqs["rate"] = AsymmRate.req(self, version=self.rate_version,
            dataset_name=self.rate_dataset_name, category_name=self.rate_category_name)
        return reqs

    def get_postfix(self, postfix):
        save_postfix = super(MapAsymmAcceptance, self).get_postfix(postfix)
        if self.xx_symmetric:
            save_postfix += "__xx_sym"
        if self.yy_symmetric:
            save_postfix += "__yy_sym"
        return save_postfix

    def check_symmetry(self, x, xp, y, yp):
        if self.xx_symmetric and x != xp:
            return False
        if self.yy_symmetric and y != yp:
            return False
        return True

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

        # Create histos for ditau or ditau_jet or ditau_dijet
        nbinsZ = self.zz_range[1] - self.zz_range[0]
        histos = {}
        # acceptance computation
        for dataset, category in zip(self.datasets, self.categories):
            postfix = "{}_{}".format(dataset.name, category.name)
            den[(dataset, category)] = 0
            histos[(dataset, category)] = {}
            for x, y in itertools.product(
                    range(*self.xx_range), range(*self.yy_range)):
                for xp, yp in itertools.product(
                        range(self.xx_range[0], x + 1), range(self.yy_range[0], y + 1)):

                    if not self.check_symmetry(x, xp, y, yp):
                        continue

                    hmodel = ("histo_total_{}_{}_{}_{}_{}".format(postfix, x, xp, y, yp), "; ZZ; Acceptance",
                        nbinsZ, self.zz_range[0], self.zz_range[1],
                    )
                    histos[(dataset, category)]["%s, %s, %s, %s" % (x, xp, y, yp)] = ROOT.TH1F(*hmodel)
            for elem in inputs["acceptance_%s" % postfix].collection.targets.values():
                rootfile = ROOT.TFile.Open(elem["root"].path)
                for x, y in itertools.product(
                        range(*self.xx_range), range(*self.yy_range)):
                    for xp, yp in itertools.product(
                            range(self.xx_range[0], x + 1), range(self.yy_range[0], y + 1)):
            
                        if not self.check_symmetry(x, xp, y, yp):
                            continue

                        histos[(dataset, category)]["%s, %s, %s, %s" % (x, xp, y, yp)].Add(
                            rootfile.Get("histo_ditau_{0}_{1}__ditau_{2}_{3}_jet".format(
                                x, xp, y, yp)).Clone())
                rootfile.Close()
                jsonfile = elem["stats"].path
                with open(jsonfile) as f:
                    d = json.load(f)
                den[(dataset, category)] += d["den"]

        # rate computation
        histos["rate"] = {}
        scaling = self.config.datasets.get(self.rate_dataset_name).get_aux("rate_scaling")
        for x, y in itertools.product(
                range(*self.xx_range), range(*self.yy_range)):
            for xp, yp in itertools.product(
                    range(self.xx_range[0], x + 1), range(self.yy_range[0], y + 1)):

                if not self.check_symmetry(x, xp, y, yp):
                        continue

                hmodel = ("rate_histo_{}_{}_{}_{}_{}".format(postfix, x, xp, y, yp),
                    "; ZZ; Acceptance", nbinsZ, self.zz_range[0], self.zz_range[1]
                )
                histos["rate"]["%s, %s, %s, %s" % (x, xp, y, yp)] = ROOT.TH1F(*hmodel)

        nevents = 0
        for elem in inputs["rate"].collection.targets.values():
            rootfile = ROOT.TFile.Open(elem["root"].path)
            for x, y in itertools.product(
                    range(*self.xx_range), range(*self.yy_range)):
                for xp, yp in itertools.product(
                        range(self.xx_range[0], x + 1), range(self.yy_range[0], y + 1)):

                    if not self.check_symmetry(x, xp, y, yp):
                        continue

                    histos["rate"]["%s, %s, %s, %s" % (x, xp, y, yp)].Add(
                        rootfile.Get("histo_ditau_{0}_{1}__ditau_{2}_{3}_jet".format(
                            x, xp, y, yp)).Clone())
            rootfile.Close()
            jsonfile = elem["stats"].path
            with open(jsonfile) as f:
                d = json.load(f)
            nevents += d["nevents"]

        acceptances_to_plot = OrderedDict()
        for dataset, category in zip(self.datasets, self.categories):
            acceptances_to_plot[(dataset, category)] = []

        for x, y in itertools.product(
                range(*self.xx_range), range(*self.yy_range)):
            for xp, yp in itertools.product(
                    range(self.xx_range[0], x + 1), range(self.yy_range[0], y + 1)):

                if not self.check_symmetry(x, xp, y, yp):
                    continue

                if self.xx_fixed != -1 and self.xx_fixed != x:
                    continue
                if self.xxp_fixed != -1 and self.xxp_fixed != xp:
                    continue
                if self.yy_fixed != -1 and self.yy_fixed != y:
                    continue
                if self.yyp_fixed != -1 and self.yyp_fixed != yp:
                    continue

                # histos["rate"]["%s, %s, %s, %s" % (x, xp, y, yp)].Scale(
                    # (scaling * 2760. * 11246.) / (1000 * nevents))
                histos["rate"]["%s, %s, %s, %s" % (x, xp, y, yp)].Scale(
                    (60 * 2760. * 11246.) / (1000 * nevents))
                for z in range(*self.zz_range):
                    if self.zz_fixed != -1 and self.zz_fixed != z:
                        continue
                    rate = histos["rate"]["%s, %s, %s, %s" % (x, xp, y, yp)].GetBinContent(
                        z - self.zz_range[0] + 1)
                    if (rate > self.max_rate or rate < self.min_rate):
                        continue
                    parameters = "%s,%s,%s,%s,%s" % (x, xp, y, yp, z)
                    for dataset, category in zip(self.datasets, self.categories):
                        acceptance = histos[(dataset, category)][
                            "%s, %s, %s, %s" % (x, xp, y, yp)].GetBinContent(
                                z - self.zz_range[0] + 1
                        ) / float(den[(dataset, category)])

                        # if acceptance < self.min_acceptance or acceptance > self.max_acceptance:
                        #     continue
                        acceptances_to_plot[(dataset, category)].append((parameters, rate, acceptance))

        self.plot_stuff(acceptances_to_plot)


class MapAsymmVBFAcceptance(MapAsymmAcceptance):
    use_vbf_rate = luigi.BoolParameter(default=False, description="whether to use "
        "the the vbf trigger for the rate computation, default: False")
    # rate_title = "IsoTauXXIsoTauXX' OR IsoTauYYIsoTauYY'JetZZ OR L1 VBF Rate (kHz)"
    
    def requires(self):
        reqs = {}
        for dataset, category in zip(self.datasets, self.categories):
            postfix = "{}_{}".format(dataset.name, category.name)
            available_branches = len(dataset.get_files())
            if self.only_available_branches:
                branches = []
                for i in range(available_branches):
                    ok = True
                    if AsymmVBFAcceptance.req(self, version=self.acceptance_version,
                            dataset_name=dataset.name, category_name=category.name,
                            branch=i).complete():
                        branches.append(i)
                reqs["acceptance_%s" % postfix] = AsymmVBFAcceptance.req(self,
                    version=self.acceptance_version, dataset_name=dataset.name,
                    category_name=category.name, branches=branches)
            else:
                reqs["acceptance_%s" % postfix] = AsymmVBFAcceptance.req(self,
                    version=self.acceptance_version, dataset_name=dataset.name,
                    category_name=category.name)
        if self.use_vbf_rate:
            reqs["rate"] = AsymmVBFRate.req(self, version=self.rate_version,
                dataset_name=self.rate_dataset_name, category_name=self.rate_category_name)
        else:
            reqs["rate"] = AsymmRate.req(self, version=self.rate_version,
                dataset_name=self.rate_dataset_name, category_name=self.rate_category_name)
        return reqs

    def store_parts(self):
        parts = super(MapAsymmVBFAcceptance, self).store_parts()
        if self.use_vbf_rate:
            parts["rate"] = "{}__{}__withVBF".format(self.rate_dataset_name, self.rate_version)
        return parts


class MapAsymmKetiAcceptance(MapAsymmVBFAcceptance):
    #rate_title = "IsoTauXXIsoTauXX' OR IsoTauYYIsoTauYY'JetZZ OR L1 VBF Rate (kHz)"
    
    def requires(self):
        reqs = {}
        for dataset, category in zip(self.datasets, self.categories):
            postfix = "{}_{}".format(dataset.name, category.name)
            available_branches = len(dataset.get_files())
            if self.only_available_branches:
                branches = []
                for i in range(available_branches):
                    ok = True
                    if AsymmKetiAcceptance.req(self, version=self.acceptance_version,
                            dataset_name=dataset.name, category_name=category.name,
                            branch=i).complete():
                        branches.append(i)
                reqs["acceptance_%s" % postfix] = AsymmKetiAcceptance.req(self,
                    version=self.acceptance_version, dataset_name=dataset.name,
                    category_name=category.name, branches=branches)
            else:
                reqs["acceptance_%s" % postfix] = AsymmKetiAcceptance.req(self,
                    version=self.acceptance_version, dataset_name=dataset.name,
                    category_name=category.name)
        if self.use_vbf_rate:
            reqs["rate"] = AsymmKetiRate.req(self, version=self.rate_version,
                dataset_name=self.rate_dataset_name, category_name=self.rate_category_name)
        else:
            reqs["rate"] = AsymmRate.req(self, version=self.rate_version,
                dataset_name=self.rate_dataset_name, category_name=self.rate_category_name)
        return reqs


class MapVBFGainAcceptance(MapAsymmKetiAcceptance):
    #rate_title = "IsoTauXXIsoTauXX' OR IsoTauYYIsoTauYY'JetZZ OR L1 VBF Rate (kHz)"
    
    def requires(self):
        reqs = {}
        for dataset, category in zip(self.datasets, self.categories):
            postfix = "{}_{}".format(dataset.name, category.name)
            # available_branches = len(dataset.get_files())
            # if self.only_available_branches:
                # branches = []
                # for i in range(available_branches):
                    # ok = True
                    # if AsymmKetiAcceptance.req(self, version=self.acceptance_version,
                            # dataset_name=dataset.name, category_name=category.name,
                            # branch=i).complete():
                        # branches.append(i)
                # reqs["acceptance_%s" % postfix] = AsymmKetiAcceptance.req(self,
                    # version=self.acceptance_version, dataset_name=dataset.name,
                    # category_name=category.name, branches=branches)
            # else:
            reqs["vbf_acceptance_%s" % postfix] = AsymmKetiAcceptance.vreq(self,
                # version=self.acceptance_version,
                dataset_name=dataset.name,
                category_name=category.name)
            reqs["taujet_acceptance_%s" % postfix] = AsymmAcceptance.vreq(self,
                # version=self.acceptance_version,
                dataset_name=dataset.name,
                category_name=category.name)
        if self.use_vbf_rate:
            reqs["rate"] = AsymmKetiRate.req(self, version=self.rate_version,
                dataset_name=self.rate_dataset_name, category_name=self.rate_category_name)
        else:
            reqs["rate"] = AsymmRate.req(self, version=self.rate_version,
                dataset_name=self.rate_dataset_name, category_name=self.rate_category_name)
        return reqs

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

        # Create histos for ditau or ditau_jet or ditau_dijet
        nbinsZ = self.zz_range[1] - self.zz_range[0]

        # acceptance computation
        for dataset, category in zip(self.datasets, self.categories):
            postfix = "{}_{}".format(dataset.name, category.name)
            den[(dataset, category)] = {}
            histos[(dataset, category)] = {}
            for x, y in itertools.product(
                    range(*self.xx_range), range(*self.yy_range)):
                for xp, yp in itertools.product(
                        range(self.xx_range[0], x + 1), range(self.yy_range[0], y + 1)):

                    if not self.check_symmetry(x, xp, y, yp):
                        continue

                    hmodel = ("vbf_histo_total_{}_{}_{}_{}_{}".format(postfix, x, xp, y, yp), "; ZZ; Acceptance",
                        nbinsZ, self.zz_range[0], self.zz_range[1],
                    )
                    histos[(dataset, category)]["%s, %s, %s, %s" % (x, xp, y, yp)] = ROOT.TH1F(*hmodel)
                    hmodel = ("den_histo_total_{}_{}_{}_{}_{}".format(postfix, x, xp, y, yp), "; ZZ; Acceptance",
                        nbinsZ, self.zz_range[0], self.zz_range[1],
                    )
                    den[(dataset, category)]["%s, %s, %s, %s" % (x, xp, y, yp)] = ROOT.TH1F(*hmodel)
            for elem in inputs["vbf_acceptance_%s" % postfix].collection.targets.values():
                rootfile = ROOT.TFile.Open(elem["root"].path)
                for x, y in itertools.product(
                        range(*self.xx_range), range(*self.yy_range)):
                    for xp, yp in itertools.product(
                            range(self.xx_range[0], x + 1), range(self.yy_range[0], y + 1)):

                        if not self.check_symmetry(x, xp, y, yp):
                            continue

                        histos[(dataset, category)]["%s, %s, %s, %s" % (x, xp, y, yp)].Add(
                            rootfile.Get("histo_ditau_{0}_{1}__ditau_{2}_{3}_jet".format(
                                x, xp, y, yp)).Clone())
                rootfile.Close()
            for elem in inputs["taujet_acceptance_%s" % postfix].collection.targets.values():
                rootfile = ROOT.TFile.Open(elem["root"].path)
                for x, y in itertools.product(
                        range(*self.xx_range), range(*self.yy_range)):
                    for xp, yp in itertools.product(
                            range(self.xx_range[0], x + 1), range(self.yy_range[0], y + 1)):

                        if not self.check_symmetry(x, xp, y, yp):
                            continue

                        den[(dataset, category)]["%s, %s, %s, %s" % (x, xp, y, yp)].Add(
                            rootfile.Get("histo_ditau_{0}_{1}__ditau_{2}_{3}_jet".format(
                                x, xp, y, yp)).Clone())
                rootfile.Close()

        # rate computation
        histos["rate"] = {}
        scaling = self.config.datasets.get(self.rate_dataset_name).get_aux("rate_scaling")
        for x, y in itertools.product(
                range(*self.xx_range), range(*self.yy_range)):
            for xp, yp in itertools.product(
                    range(self.xx_range[0], x + 1), range(self.yy_range[0], y + 1)):

                if not self.check_symmetry(x, xp, y, yp):
                        continue

                hmodel = ("rate_histo_{}_{}_{}_{}_{}".format(postfix, x, xp, y, yp),
                    "; ZZ; Acceptance", nbinsZ, self.zz_range[0], self.zz_range[1]
                )
                histos["rate"]["%s, %s, %s, %s" % (x, xp, y, yp)] = ROOT.TH1F(*hmodel)

        nevents = 0
        for elem in inputs["rate"].collection.targets.values():
            rootfile = ROOT.TFile.Open(elem["root"].path)
            for x, y in itertools.product(
                    range(*self.xx_range), range(*self.yy_range)):
                for xp, yp in itertools.product(
                        range(self.xx_range[0], x + 1), range(self.yy_range[0], y + 1)):

                    if not self.check_symmetry(x, xp, y, yp):
                        continue

                    histos["rate"]["%s, %s, %s, %s" % (x, xp, y, yp)].Add(
                        rootfile.Get("histo_ditau_{0}_{1}__ditau_{2}_{3}_jet".format(
                            x, xp, y, yp)).Clone())
            rootfile.Close()
            jsonfile = elem["stats"].path
            with open(jsonfile) as f:
                d = json.load(f)
            nevents += d["nevents"]
            
        # get points if needed
        points_to_use = self.get_points()

        acceptances_to_plot = OrderedDict()
        for dataset, category in zip(self.datasets, self.categories):
            acceptances_to_plot[(dataset, category)] = []

        for x, y in itertools.product(
                range(*self.xx_range), range(*self.yy_range)):
            for xp, yp in itertools.product(
                    range(self.xx_range[0], x + 1), range(self.yy_range[0], y + 1)):

                if not self.check_symmetry(x, xp, y, yp):
                    continue

                if self.xx_fixed != -1 and self.xx_fixed != x:
                    continue
                if self.xxp_fixed != -1 and self.xxp_fixed != xp:
                    continue
                if self.yy_fixed != -1 and self.yy_fixed != y:
                    continue
                if self.yyp_fixed != -1 and self.yyp_fixed != yp:
                    continue

                # histos["rate"]["%s, %s, %s, %s" % (x, xp, y, yp)].Scale(
                    # (scaling * 2760. * 11246.) / (1000 * nevents))
                histos["rate"]["%s, %s, %s, %s" % (x, xp, y, yp)].Scale(
                    (60 * 2760. * 11246.) / (1000 * nevents))
                for z in range(*self.zz_range):
                    if points_to_use:
                        if (x, xp, y, yp, z) not in points_to_use:
                            continue

                    if self.zz_fixed != -1 and self.zz_fixed != z:
                        continue
                    rate = histos["rate"]["%s, %s, %s, %s" % (x, xp, y, yp)].GetBinContent(
                        z - self.zz_range[0] + 1)
                    if (rate > self.max_rate or rate < self.min_rate):
                        continue
                    parameters = "%s,%s,%s,%s,%s" % (x, xp, y, yp, z)
                    for dataset, category in zip(self.datasets, self.categories):
                        acceptance = (
                            float(
                                histos[(dataset, category)]["%s, %s, %s, %s" % (x, xp, y, yp)
                                    ].GetBinContent(z - self.zz_range[0] + 1)
                            ) / 
                            float(
                                den[(dataset, category)]["%s, %s, %s, %s" % (x, xp, y, yp)
                                    ].GetBinContent(z - self.zz_range[0] + 1)
                            )
                        )

                        # if acceptance < self.min_acceptance or acceptance > self.max_acceptance:
                        #     continue
                        acceptances_to_plot[(dataset, category)].append((parameters, rate, acceptance))

        self.plot_stuff(acceptances_to_plot)


class MapAsymmDiJetAcceptance(MapAsymmAcceptance):
    zz_symmetric = luigi.BoolParameter(default=False, description="whether to remove "
        "the asymmetry in the zz parameter, default: False")
    zzp_fixed = luigi.FloatParameter(default=-1., description="whether to show results "
        "only for 1 specific zzp value, default: -1")
        
    xx_range = AsymmDiJetRate.xx_range
    yy_range = AsymmDiJetRate.yy_range
    zz_range = AsymmDiJetRate.dzz_range

    rate_title = "IsoTauXXIsoTauXX' OR IsoTauYYIsoTauYY'JetZZJetZZ' Rate (kHz)"
    acceptance_title = "Acceptance gain"

    def requires(self):
        reqs = {}
        for dataset, category in zip(self.datasets, self.categories):
            postfix = "{}_{}".format(dataset.name, category.name)
            available_branches = len(dataset.get_files())
            # if self.only_available_branches:
                # branches = []
                # for i in range(available_branches):
                    # ok = True
                    # if AsymmAcceptance.req(self, version=self.acceptance_version,
                            # dataset_name=dataset.name, category_name=category.name,
                            # branch=i).complete():
                        # branches.append(i)
                # reqs["acceptance_%s" % postfix] = AsymmDiJetAcceptance.req(self,
                    # version=self.acceptance_version, dataset_name=dataset.name,
                    # category_name=category.name, branches=branches)
            # else:
            if not self.use_vbf_trigger:
                reqs["acceptance_%s" % postfix] = AsymmDiJetAcceptance.req(self,
                    version=self.acceptance_version, dataset_name=dataset.name,
                    category_name=category.name)
            else:
                reqs["acceptance_%s" % postfix] = AsymmVBFDiJetAcceptance.req(self,
                    version=self.acceptance_version, dataset_name=dataset.name,
                    category_name=category.name)
        reqs["rate"] = AsymmDiJetRate.req(self, version=self.rate_version,
            dataset_name=self.rate_dataset_name, category_name=self.rate_category_name)
        return reqs

    def get_postfix(self, postfix):
        save_postfix = super(MapAsymmDiJetAcceptance, self).get_postfix(postfix)
        if self.zz_symmetric:
            save_postfix += "__zz_sym"
        return save_postfix

    def check_symmetry(self, x, xp, y, yp, z=-1, zp=-1):
        if self.xx_symmetric and x != xp:
            return False
        if self.yy_symmetric and y != yp:
            return False
        if self.zz_symmetric and z != zp:
            return False
        return True

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

        # Create histos for ditau or ditau_jet or ditau_dijet
        nbinsZ = self.zz_range[1] - self.zz_range[0]
        histos = {}
        # acceptance computation
        for dataset, category in zip(self.datasets, self.categories):
            postfix = "{}_{}".format(dataset.name, category.name)
            den[(dataset, category)] = 0
            histos[(dataset, category)] = {}
            for x, y in itertools.product(
                    range(*self.xx_range), range(*self.yy_range)):
                for xp, yp in itertools.product(
                        range(self.xx_range[0], x + 1), range(self.yy_range[0], y + 1)):

                    if not self.check_symmetry(x, xp, y, yp):
                        continue

                    hmodel = ("histo_total_{}_{}_{}_{}_{}".format(postfix, x, xp, y, yp),
                        "; ZZ; ZZp; Acceptance",
                        nbinsZ, self.zz_range[0], self.zz_range[1],
                        nbinsZ, self.zz_range[0], self.zz_range[1],
                    )
                    histos[(dataset, category)]["%s, %s, %s, %s" % (x, xp, y, yp)] = ROOT.TH2F(
                        *hmodel)
            for elem in inputs["acceptance_%s" % postfix].collection.targets.values():
                rootfile = ROOT.TFile.Open(elem["root"].path)
                for x, y in itertools.product(
                        range(*self.xx_range), range(*self.yy_range)):
                    for xp, yp in itertools.product(
                            range(self.xx_range[0], x + 1), range(self.yy_range[0], y + 1)):

                        if not self.check_symmetry(x, xp, y, yp):
                            continue

                        histos[(dataset, category)]["%s, %s, %s, %s" % (x, xp, y, yp)].Add(
                            rootfile.Get("histo_ditau_{0}_{1}__ditau_{2}_{3}_dijet".format(
                                x, xp, y, yp)).Clone())
                rootfile.Close()
                jsonfile = elem["stats"].path
                with open(jsonfile) as f:
                    d = json.load(f)
                den[(dataset, category)] += d["den"]

        # rate computation
        histos["rate"] = {}
        scaling = self.config.datasets.get(self.rate_dataset_name).get_aux("rate_scaling")
        for x, y in itertools.product(
                range(*self.xx_range), range(*self.yy_range)):
            for xp, yp in itertools.product(
                    range(self.xx_range[0], x + 1), range(self.yy_range[0], y + 1)):

                if not self.check_symmetry(x, xp, y, yp):
                        continue

                hmodel = ("rate_histo_{}_{}_{}_{}_{}".format(postfix, x, xp, y, yp),
                    "; ZZ; ZZp, Rate", 
                    nbinsZ, self.zz_range[0], self.zz_range[1],
                    nbinsZ, self.zz_range[0], self.zz_range[1]
                )
                histos["rate"]["%s, %s, %s, %s" % (x, xp, y, yp)] = ROOT.TH2F(*hmodel)

        nevents = 0
        for elem in inputs["rate"].collection.targets.values():
            rootfile = ROOT.TFile.Open(elem["root"].path)
            for x, y in itertools.product(
                    range(*self.xx_range), range(*self.yy_range)):
                for xp, yp in itertools.product(
                        range(self.xx_range[0], x + 1), range(self.yy_range[0], y + 1)):

                    if not self.check_symmetry(x, xp, y, yp):
                        continue

                    histos["rate"]["%s, %s, %s, %s" % (x, xp, y, yp)].Add(
                        rootfile.Get("histo_ditau_{0}_{1}__ditau_{2}_{3}_dijet".format(
                            x, xp, y, yp)).Clone())
            rootfile.Close()
            jsonfile = elem["stats"].path
            with open(jsonfile) as f:
                d = json.load(f)
            nevents += d["nevents"]

        acceptances_to_plot = OrderedDict()
        for dataset, category in zip(self.datasets, self.categories):
            acceptances_to_plot[(dataset, category)] = []

        for x, y in itertools.product(
                range(*self.xx_range), range(*self.yy_range)):
            for xp, yp in itertools.product(
                    range(self.xx_range[0], x + 1), range(self.yy_range[0], y + 1)):

                if not self.check_symmetry(x, xp, y, yp):
                    continue

                if self.xx_fixed != -1 and self.xx_fixed != x:
                    continue
                if self.xxp_fixed != -1 and self.xxp_fixed != xp:
                    continue
                if self.yy_fixed != -1 and self.yy_fixed != y:
                    continue
                if self.yyp_fixed != -1 and self.yyp_fixed != yp:
                    continue

                histos["rate"]["%s, %s, %s, %s" % (x, xp, y, yp)].Scale(
                    (scaling * 2760. * 11246.) / (1000 * nevents))
                for z, zp in itertools.product(range(*self.zz_range), range(*self.zz_range)):
                    if self.zz_fixed != -1 and self.zz_fixed != z:
                        continue
                    if self.zzp_fixed != -1 and self.zzp_fixed != zp:
                        continue
                    rate = histos["rate"]["%s, %s, %s, %s" % (x, xp, y, yp)].GetBinContent(
                        z - self.zz_range[0] + 1, zp - self.zz_range[0] + 1)
                    if (rate > self.max_rate or rate < self.min_rate):
                        continue
                    parameters = "%s,%s,%s,%s,%s,%s" % (x, xp, y, yp, z, zp)
                    for dataset, category in zip(self.datasets, self.categories):
                        acceptance = histos[(dataset, category)][
                            "%s, %s, %s, %s" % (x, xp, y, yp)].GetBinContent(
                                z - self.zz_range[0] + 1, zp - self.zz_range[0] + 1
                        ) / float(den[(dataset, category)])

                        # if acceptance < self.min_acceptance or acceptance > self.max_acceptance:
                        #     continue
                        acceptances_to_plot[(dataset, category)].append((parameters, rate, acceptance))

        self.plot_stuff(acceptances_to_plot)


class MapAsymmVBFDiJetAcceptance(MapAsymmDiJetAcceptance):
    use_vbf_rate = luigi.BoolParameter(default=False, description="whether to use "
        "the the vbf trigger for the rate computation, default: False")
    #rate_title = "IsoTauXXIsoTauXX' OR IsoTauYYIsoTauYY'JetZZJetZZ' OR L1 VBF Rate (kHz)"

    def requires(self):
        reqs = {}
        for dataset, category in zip(self.datasets, self.categories):
            postfix = "{}_{}".format(dataset.name, category.name)
            available_branches = len(dataset.get_files())
            if self.only_available_branches:
                branches = []
                for i in range(available_branches):
                    ok = True
                    if AsymmVBFDiJetAcceptance.req(self, version=self.acceptance_version,
                            dataset_name=dataset.name, category_name=category.name,
                            branch=i).complete():
                        branches.append(i)
                reqs["acceptance_%s" % postfix] = AsymmVBFDiJetAcceptance.req(self,
                    version=self.acceptance_version, dataset_name=dataset.name,
                    category_name=category.name, branches=branches)
            else:
                reqs["acceptance_%s" % postfix] = AsymmVBFDiJetAcceptance.req(self,
                    version=self.acceptance_version, dataset_name=dataset.name,
                    category_name=category.name)
        if self.use_vbf_rate:
            reqs["rate"] = AsymmVBFDiJetRate.req(self, version=self.rate_version,
                dataset_name=self.rate_dataset_name, category_name=self.rate_category_name)
        else:
            reqs["rate"] = AsymmDiJetRate.req(self, version=self.rate_version,
                dataset_name=self.rate_dataset_name, category_name=self.rate_category_name)
        return reqs

    def store_parts(self):
        parts = super(MapAsymmVBFDiJetAcceptance, self).store_parts()
        if self.use_vbf_rate:
            parts["rate"] = "{}__{}__withVBF".format(self.rate_dataset_name, self.rate_version)
        return parts


class MapAsymmKetiDiJetAcceptance(MapAsymmVBFDiJetAcceptance):
    #rate_title = "IsoTauXXIsoTauXX' OR IsoTauYYIsoTauYY'JetZZJetZZ' OR new L1 VBF Rate (kHz)"

    def requires(self):
        reqs = {}
        for dataset, category in zip(self.datasets, self.categories):
            postfix = "{}_{}".format(dataset.name, category.name)
            available_branches = len(dataset.get_files())
            if self.only_available_branches:
                branches = []
                for i in range(available_branches):
                    ok = True
                    if AsymmKetiDiJetAcceptance.req(self, version=self.acceptance_version,
                            dataset_name=dataset.name, category_name=category.name,
                            branch=i).complete():
                        branches.append(i)
                reqs["acceptance_%s" % postfix] = AsymmKetiDiJetAcceptance.req(self,
                    version=self.acceptance_version, dataset_name=dataset.name,
                    category_name=category.name, branches=branches)
            else:
                reqs["acceptance_%s" % postfix] = AsymmKetiDiJetAcceptance.req(self,
                    version=self.acceptance_version, dataset_name=dataset.name,
                    category_name=category.name)
        if self.use_vbf_rate:
            reqs["rate"] = AsymmKetiDiJetRate.req(self, version=self.rate_version,
                dataset_name=self.rate_dataset_name, category_name=self.rate_category_name)
        else:
            reqs["rate"] = AsymmDiJetRate.req(self, version=self.rate_version,
                dataset_name=self.rate_dataset_name, category_name=self.rate_category_name)
        return reqs