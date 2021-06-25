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
    AddTrigger, AddOffline, AddDiJetTrigger, AddDiJetOffline, ComputeRate, ComputeDiJetRate, Rate, Acceptance
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
        
    xx_range = Rate.xx_range
    yy_range = Rate.yy_range
    zz_range = Rate.zz_range
    dzz_range = Rate.dzz_range

    rate_title = "DoubleIsoTauXX OR DoubleIsoTauYYJetZZ OR DoubleIsoTauDYYDoubleJetDZZ Rate (kHz)"
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
            save_postfix += "_dyy_" + str(self.dyy_fixed)
        if self.dzz_fixed != -1:
            save_postfix += "_dzz_" + str(self.dzz_fixed)
        return save_postfix

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
                hmodel = ("histo_{}_{}_{}_{}".format(postfix, x, y, dy), "; YYp; ZZp",
                    nbinsZ, self.zz_range[0], self.zz_range[1],
                    nbinsdZ, self.dzz_range[0], self.dzz_range[1],
                )
                histos[(dataset, category)]["%s, %s, %s" % (x, y, dy)] = ROOT.TH2F(*hmodel)
            for elem in inputs["acceptance_%s" % postfix].collection.targets.values():
                rootfile = ROOT.TFile.Open(elem["root"].path)
                for x, y, dy in itertools.product(
                        range(*self.xx_range), range(*self.yy_range), range(*self.yy_range)):
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
            hmodel = ("histo_{}_{}_{}".format(x, y, dy), "; YYp; ZZp",
                nbinsZ, self.zz_range[0], self.zz_range[1],
                nbinsdZ, self.dzz_range[0], self.dzz_range[1],
            )
            histos["rate"]["%s, %s, %s" % (x, y, dy)] = ROOT.TH2F(*hmodel)

        nevents = 0
        for elem in inputs["rate"].collection.targets.values():
            rootfile = ROOT.TFile.Open(elem["root"].path)
            for x, y, dy in itertools.product(
                    range(*self.xx_range), range(*self.yy_range), range(*self.yy_range)):
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
            if self.xx_fixed != -1 and self.xx_fixed != x:
                continue
            if self.yy_fixed != -1 and self.yy_fixed != y:
                continue
            if self.dyy_fixed != -1 and self.dyy_fixed != dy:
                continue
            histos["rate"]["%s, %s, %s" % (x, y, dy)].Scale((scaling * 2760. * 11246.) / (1000 * nevents))
            for z, dz in itertools.product(range(*self.zz_range), range(*self.dzz_range)):
                if self.zz_fixed != -1 and self.zz_fixed != z:
                    continue
                if self.dzz_fixed != -1 and self.dzz_fixed != dz:
                    continue
                rate = histos["rate"]["%s, %s, %s" % (x, y, dy)].GetBinContent(
                    z - self.zz_range[0] + 1, dz - self.dzz_range[0] + 1)
                if (rate > self.max_rate or rate < self.min_rate):
                    continue
                parameters = "{},{},{},{},{}".format(x, y, z, dy, dz)
                for dataset, category in zip(self.datasets, self.categories):
                    acceptance = histos[(dataset, category)]["%s, %s, %s" % (x, y, dy)].GetBinContent(
                        z - self.zz_range[0] + 1,
                        dz - self.dzz_range[0] + 1
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

            # postfix = "{}_{}".format(dataset.name, category.name)
            # with open(create_file_dir(output["json_rate_%s" % postfix].path), "w") as f:
                # json.dump(dict(zip(parameters, zip(rates, acceptances))), f, indent=4)
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
