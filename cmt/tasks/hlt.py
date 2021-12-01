# coding: utf-8

import law
import luigi
import os
from copy import deepcopy as copy

from analysis_tools.utils import (
    import_root, create_file_dir, join_root_selection
)

from cmt.base_tasks.base import ( 
    Task, DatasetTaskWithCategory, DatasetWrapperTask, HTCondorWorkflow, InputData,
    ConfigTaskWithCategory
)

from cmt.base_tasks.preprocessing import DatasetCategoryWrapperTask

from cmt.tasks.trigger import L1Skim


class HltTask():
    triggers = law.CSVParameter(
        # default=("DoubleTauJet30", "DoubleTau32", "DoubleTau35", "DoubleTauORDoubleTauJet"),
        default=("DoubleTauJet30", "DoubleTau32", "DoubleTau35", "DoubleTauORDoubleTauJet",
            "L1DoubleTauJet", "L1DoubleTau32", "L1DoubleTau35", "L1DoubleTauORDoubleTauJet"),
        choices=("DoubleTauJet30", "DoubleTau32", "DoubleTau35", "DoubleTauORDoubleTauJet",
            "L1DoubleTauJet", "L1DoubleTau32", "L1DoubleTau35", "L1DoubleTauORDoubleTauJet"),
        description="triggers to compute, default: DoubleTauJet30")
    trigger_ratios = law.CSVParameter(
        default=("DoubleTauORDoubleTauJet/DoubleTau32", "L1DoubleTauORDoubleTauJet/L1DoubleTau32",),
        description="trigger ratios to compute, default: DoubleTauORDoubleTauJet/DoubleTau32")

    trigger_requirements = {
        "L1DoubleTauJet": "passhltL1DoubleTauJet == 1",
        "L1DoubleTau32": "passhltL1DoubleTau32 == 1",
        "L1DoubleTau35": "passhltL1DoubleTau35 == 1",
        "L1DoubleTauORDoubleTauJet": "passhltL1DoubleTau35 == 1 || passhltL1DoubleTauJet == 1",
        "DoubleTauJet30": "passDoubleTauJetHLT == 1",
        "DoubleTau32": "passDoubleTau32HLT == 1",
        "DoubleTau35": "passDoubleTau35HLT == 1",
        "DoubleTauORDoubleTauJet": "passDoubleTau35HLT == 1 || passDoubleTauJetHLT == 1",
    }

    # some definitions needed
    @classmethod
    def add_to_root(self, root):
        root.gInterpreter.Declare("""
            using Vfloat = const ROOT::RVec<float>&;      
            ROOT::RVec<float> lead_sublead(Vfloat vec, int max){
                ROOT::RVec<float> leading_vec;
                for (size_t i = 0; i < max; i++) {
                    leading_vec.push_back(-1.);
                }
                for (size_t i = 0; i < vec.size(); i++) {
                    leading_vec[i] = vec[i];
                    if (i >= max - 1) break;
                }
                return leading_vec;
            }
            Double_t Phi_mpi_pi(Double_t x) {
                while (x >= 3.14159) x -= (2 * 3.14159);
                while (x < -3.14159) x += (2 * 3.14159);
                return x;
            }
            #include "TMath.h"
            using Vfloat = const ROOT::RVec<float>&;      
            ROOT::RVec<bool> maskDeltaR(Vfloat eta1, Vfloat phi1, Vfloat eta2, Vfloat phi2, float th_dr) {
                ROOT::RVec<bool> mask;
                for (size_t i = 0; i < eta1.size(); i++){
                    bool bigger_deltar = true;
                    for (size_t j = 0; j < eta2.size(); j++){
                        Double_t deta = eta1[i] - eta2[j];
                        Double_t dphi = Phi_mpi_pi(phi1[i] - phi2[j]);
                        Double_t dr = TMath::Sqrt(deta * deta + dphi * dphi);
                        if (dr < th_dr) bigger_deltar = false;
                    }
                    mask.push_back(bigger_deltar);
                }
                return mask;
            }
            // using Vint = const ROOT::RVec<int>&;   
            // int* get_int_array(Vint vec) {
            //    int output[256];
            //    for (size_t i = 0; i <= vec.size(); i++) {
            //        output[i] = vec.at(i);
            //    }
            //    return output;                
            // }
            using Vfloat = const ROOT::RVec<float>&;
            Float_t* get_float_array(Vfloat vec) {
                static Float_t output[256];
                for (size_t i = 0; i <= vec.size(); i++) {
                    output[i] = vec.at(i);
                }
                return output;  
            }
        """)
        return root
    
    def __init__(self, *args, **kwargs):
        self.trigger_reqs = [trigger_requirements[trigger]
            for trigger in self.triggers if trigger in trigger_definitions]


class PreEfficiency(DatasetTaskWithCategory, law.LocalWorkflow, HTCondorWorkflow, HltTask):
    tree_name = "demo/hltdev"

    default_store = "$CMT_STORE_EOS_CATEGORIZATION"
    default_wlcg_fs = "wlcg_fs_categorization"

    def __init__(self, *args, **kwargs):
        super(PreEfficiency, self).__init__(*args, **kwargs)

    def create_branch_map(self):
        return len(self.dataset.get_files())

    def workflow_requires(self):
        return {"data": InputData.vreq(self)}

    def requires(self):
        return {"data": InputData.vreq(self, file_index=self.branch)}

    def get_output_filename(self):
        p = self.requires()["data"].output().path
        d = self.dataset.folder + ("" if self.dataset.folder.endswith("/") else "/")
        return p[len(d):].replace("/", "_")

    def output(self):
        # return self.local_target("histo_%s.root" % self.branch)
        return self.local_target(self.get_output_filename())

    def define_taus(self, df):
        df = df.Define("lead_sublead_goodtau_pt",
                "lead_sublead(tauPt[abs(tauEta) <= 2.1"
                " && tauByVLooseDeepTau2017v2p1VSmu && tauByVVLooseDeepTau2017v2p1VSe && tauByMediumDeepTau2017v2p1VSjet"
                " && abs(taudz) < 0.2], 3)"
            ).Define("lead_sublead_goodtau_eta",
                "lead_sublead(tauEta[abs(tauEta) <= 2.1"
                " && tauByVLooseDeepTau2017v2p1VSmu && tauByVVLooseDeepTau2017v2p1VSe && tauByMediumDeepTau2017v2p1VSjet"
                " && abs(taudz) < 0.2], 3)"
            ).Define("lead_sublead_goodtau_phi",
                "lead_sublead(tauPhi[abs(tauEta) <= 2.1"
                " && tauByVLooseDeepTau2017v2p1VSmu && tauByVVLooseDeepTau2017v2p1VSe"" && tauByMediumDeepTau2017v2p1VSjet"
                " && abs(taudz) < 0.2], 3)"
            )
        return df

    def define_jets(self, df):
        df = df.Define("lead_sublead_goodjet_pt",
                "lead_sublead(jetPt["
                    "abs(jetEta) <= 4.7 && jetID >= 2 "
                    "&& ((jetPUFullID >= 4 && jetPt <= 50) || (jetPt > 50))"
                    "&& maskDeltaR("
                        "jetEta, "
                        "jetPhi, "
                        "lead_sublead_goodtau_eta, "
                        "lead_sublead_goodtau_phi, "
                        "0.5)"
                    "], 3)"
            ).Define("lead_sublead_goodjet_eta",
                "lead_sublead(jetEta["
                    "abs(jetEta) <= 4.7 && jetID >= 2 "
                    "&& ((jetPUFullID >= 4 && jetPt <= 50) || (jetPt > 50))"
                    "&& maskDeltaR("
                        "jetEta, "
                        "jetPhi, "
                        "lead_sublead_goodtau_eta, "
                        "lead_sublead_goodtau_phi, "
                        "0.5)"
                    "], 3)"
            ).Define("lead_sublead_goodjet_phi",
                "lead_sublead(jetPhi["
                    "abs(jetEta) <= 4.7 && jetID >= 2 "
                    "&& ((jetPUFullID >= 4 && jetPt <= 50) || (jetPt > 50))"
                    "&& maskDeltaR("
                        "jetEta, "
                        "jetPhi, "
                        "lead_sublead_goodtau_eta, "
                        "lead_sublead_goodtau_phi, "
                        "0.5)"
                    "], 3)"
            )
        return df

    def filter_jets(self, df, category=None):
        if not category:
            category = self.category
        return df.Filter(
        # ask for a minimum number of jets (depending on the category)
        # with pt > 20 (+ sth depending on the dataset)
            "jetPt["
                "jetPt >= (20 + {0}) && abs(jetEta) <= 4.7 && jetID >= 2 "
                "&& ((jetPUFullID >= 4 && jetPt <= 50) || (jetPt > 50))"
                "&& maskDeltaR("
                    "jetEta, "
                    "jetPhi, "
                    "lead_sublead_goodtau_eta, "
                    "lead_sublead_goodtau_phi, "
                    "0.5)"
                "].size() >= {1} "
            # ask for a maximum number of jets (depending on the category)
            # with pt > 20 (+ sth depending on the dataset)
            "&& jetPt["
                "jetPt >= (20 + {0}) && abs(jetEta) <= 4.7 && jetID >= 2 "
                "&& ((jetPUFullID >= 4 && jetPt <= 50) || (jetPt > 50))"
                "&& maskDeltaR("
                    "jetEta, "
                    "jetPhi, "
                    "lead_sublead_goodtau_eta, "
                    "lead_sublead_goodtau_phi, "
                    "0.5)"
                "].size() <= {2}".format(
                    category.get_aux("add_to_jet_pt"),
                    category.get_aux("nminjets"),
                    category.get_aux("nmaxjets")
                )
        )

    @law.decorator.notify
    @law.decorator.localize(input=False)
    def run(self):
        ROOT = import_root()
        ROOT = self.add_to_root(ROOT)
        # ROOT.ROOT.EnableImplicitMT()

        df = ROOT.RDataFrame(self.tree_name, self.input()["data"].path)
        category = self.category
        df = self.define_taus(df)
        df = self.define_jets(df)
        df = self.filter_jets(df)
        while category.get_aux("parent_category", None):
            category = self.config.categories.get(category.get_aux("parent_category", None))
            df = self.filter_jets(df, category=category)

        histos = {}
        # taus
        hmodel = ("tau_pt_total", "; subleading tau pt [GeV]; Events / 4 GeV", 20, 20, 100)
        histos["tau_pt_total"] = df.Filter("nTau >= 2").Define(
                "subleading_tau_pt", "lead_sublead_goodtau_pt[1]").Histo1D(hmodel, "subleading_tau_pt")
        hmodel = ("jet_pt_total", "; leading jet pt [GeV]; Events / 8 GeV", 20, 20, 200)
        histos["jet_pt_total"] = df.Filter("nTau >= 2").Define(
                "leading_jet_pt", "lead_sublead_goodjet_pt[0]").Histo1D(hmodel, "leading_jet_pt")
        hmodel = ("tau_pt_jet_pt_total", "; subleading tau pt [GeV]; leading jet pt [GeV]",
            20, 20, 100, 20, 20, 200)
        histos["tau_pt_jet_pt_total"] = df.Filter("nTau >= 2").Define(
            "leading_jet_pt", "lead_sublead_goodjet_pt[0]").Define(
            "subleading_tau_pt", "lead_sublead_goodtau_pt[1]").Histo2D(
            hmodel, "subleading_tau_pt", "leading_jet_pt")

        for trigger in self.triggers:
            trigger_req = self.trigger_requirements[trigger]
            hmodel = ("tau_pt_%s_pass" % trigger, "; subleading tau pt [GeV]; Events / 4 GeV",
                20, 20, 100)
            histos["tau_pt_%s_pass" % trigger] = df.Filter(trigger_req).Filter(
                "nTau >= 2").Filter("nJet >= 1").Define(
                "subleading_tau_pt", "lead_sublead_goodtau_pt[1]").Histo1D(
                hmodel, "subleading_tau_pt")
            hmodel = ("jet_pt_%s_pass" % trigger, "; leading jet pt [GeV]; Events / 4 GeV",
                20, 20, 200)
            histos["jet_pt_%s_pass" % trigger] = df.Filter(trigger_req).Filter(
                "nTau >= 2").Filter("nJet >= 1").Define(
                "leading_jet_pt", "lead_sublead_goodjet_pt[0]").Histo1D(
                hmodel, "leading_jet_pt")
            hmodel = ("tau_pt_jet_pt_%s_pass" % trigger,
                "; subleading tau pt [GeV]; leading jet pt [GeV]", 20, 20, 100, 20, 20, 200)
            histos["tau_pt_jet_pt_%s_pass" % trigger] = df.Filter(trigger_req).Filter(
                "nTau >= 2").Filter("nJet >= 1").Define(
                "leading_jet_pt", "lead_sublead_goodjet_pt[0]").Define(
                "subleading_tau_pt", "lead_sublead_goodtau_pt[1]").Histo2D(
                hmodel, "subleading_tau_pt", "leading_jet_pt")

        tf = ROOT.TFile.Open(create_file_dir(self.output().path), "RECREATE")
        tf.cd()
        for histo in histos.values():
            histo.Write()
        tf.Close()


class Efficiency(DatasetTaskWithCategory, HltTask):

    def requires(self):
        return PreEfficiency.vreq(self)

    def output(self):
        output = {}
        for trigger in self.triggers:
            output["tau_pt_%s" % trigger] = self.local_target("efficiency_tau_pt_%s.pdf" % trigger)
            output["jet_pt_%s" % trigger] = self.local_target("efficiency_jet_pt_%s.pdf" % trigger)
            output["tau_pt_jet_pt_%s" % trigger] = self.local_target(
                "efficiency_tau_pt_jet_pt_%s.pdf" % trigger)
        for trigger in self.trigger_ratios:
            output["tau_pt_%s" % trigger] = self.local_target("efficiency_tau_pt_%s.pdf" % trigger.replace("/", "_"))
            output["jet_pt_%s" % trigger] = self.local_target("efficiency_jet_pt_%s.pdf" % trigger.replace("/", "_"))
            output["tau_pt_jet_pt_%s" % trigger] = self.local_target(
                "efficiency_tau_pt_jet_pt_%s.pdf" % trigger.replace("/", "_"))
        return output

    def run(self):
        ROOT = import_root()
        histos = {}

        inp = self.input()
        output = self.output()

        names = ["tau_pt", "jet_pt"]
        names2D = ["tau_pt_jet_pt"]

        for elem in inp.collection.targets.values():
            rootfile = ROOT.TFile.Open(elem.path)
            for name in names + names2D:
                if "%s_total" % name not in histos:
                    histos["%s_total" % name] = copy(rootfile.Get(
                        "%s_total" % name).Clone())
                else:
                    histos["%s_total" % (name)].Add(
                        rootfile.Get("%s_total" % (name)))
            for trigger in self.triggers:
                for name in names + names2D:
                    if "%s_%s_pass" % (name, trigger) not in histos:
                        histos["%s_%s_pass" % (name, trigger)] = copy(rootfile.Get(
                            "%s_%s_pass" % (name, trigger)).Clone())
                    else:
                        histos["%s_%s_pass" % (name, trigger)].Add(
                            rootfile.Get("%s_%s_pass" % (name, trigger)))
        
        for trigger in self.triggers:
            for name in names + names2D:
                c = ROOT.TCanvas()
                if name in names:
                    histos["%s_%s_pass" % (name, trigger)].GetYaxis().SetTitle("Efficiency")
                    histos["%s_total" % name].GetYaxis().SetTitle("Efficiency")
                if name in names:
                    ratio = ROOT.TGraphAsymmErrors(histos["%s_%s_pass" % (name, trigger)],
                        histos["%s_total" % name])
                    ratio.GetXaxis().SetTitle(histos["%s_total" % name].GetXaxis().GetTitle())
                    ratio.GetYaxis().SetTitle(histos["%s_total" % name].GetYaxis().GetTitle())
                    ratio.SetMaximum(1)
                    ratio.Draw("AP")
                    ROOT.gPad.Update()                    
                else:
                    ratio = ROOT.TEfficiency(histos["%s_%s_pass" % (name, trigger)],
                        histos["%s_total" % name])
                    ratio.Draw("colz")
                c.SaveAs(create_file_dir(output["%s_%s" % (name, trigger)].path))
                del c

        for trigger_ratio in self.trigger_ratios:
            trigger1 = trigger_ratio.split("/")[0]
            trigger2 = trigger_ratio.split("/")[1]
            for name in names + names2D:
                c = ROOT.TCanvas()
                if name in names:
                    histos["%s_total" % name].GetYaxis().SetTitle("Efficiency gain")
                if name in names:
                    ratio = copy(histos["%s_total" % name].Clone())
                    for ibin in range(1, histos["%s_%s_pass" % (name, trigger1)].GetNbinsX() + 1):
                        content = ((float(histos["%s_%s_pass" % (name, trigger1)].GetBinContent(ibin))
                            / histos["%s_%s_pass" % (name, trigger2)].GetBinContent(ibin))
                            if histos["%s_%s_pass" % (name, trigger2)].GetBinContent(ibin) != 0 else 0)
                        ratio.SetBinContent(ibin, content)

                    # ratio.GetXaxis().SetTitle(histos["%s_total" % name].GetXaxis().GetTitle())
                    # ratio.GetYaxis().SetTitle(histos["%s_total" % name].GetYaxis().GetTitle())
                    # ratio.SetMaximum(1.5)
                    # ratio.SetMinimum(0.5)
                    ratio.Draw("E1")                
                else:
                    ratio = ROOT.TEfficiency(histos["%s_%s_pass" % (name, trigger1)],
                        histos["%s_%s_pass" % (name, trigger2)])
                    ratio.Draw("colz")
                c.SaveAs(create_file_dir(output["%s_%s" % (name, trigger_ratio)].path))
                del c

class EfficiencyWrapper(DatasetCategoryWrapperTask):

    def atomic_requires(self, dataset, category):
        return Efficiency.vreq(self, dataset_name=dataset.name, category_name=category.name)


class PreRate(PreEfficiency):
    tree_name = "demo/hltdev"
    def __init__(self, *args, **kwargs):
        super(PreRate, self).__init__(*args, **kwargs)
        if not hasattr(self, "pu_per_ls") and self.is_workflow():
            self.pu_per_ls = self.build_pu_per_ls()
        elif not hasattr(self, "pu_per_ls"):
            self.pu_per_ls = self.get_pu_per_ls
        if not hasattr(self, "dict_to_cpp") and self.is_workflow():
            self.dict_to_cpp = self.build_dict_to_cpp()
        elif not hasattr(self, "dict_to_cpp"):
            self.dict_to_cpp = self.get_dict_to_cpp

    def output(self):
        # return self.local_target("rates_%s.json" % self.branch)
        return self.local_target(self.get_output_filename().replace(".root", ".json"))
    
    def build_pu_per_ls(self, filename="$CMT_BASE/cmt/tasks/run_lumi.csv"):
        import csv
        d = {}
        keys = ["fill", "run", "ls", "pu"]
        with open(os.path.expandvars(filename)) as f:
            lines = csv.reader(f)
            for line in lines:
                line = dict(zip(keys, line))
                try:
                    run = int(line["run"])
                    if run not in d:
                        d[int(line["run"])] = []
                    d[int(line["run"])].append((int(line["ls"]), float(line["pu"])))
                except ValueError:
                    continue
        return d
    
    @law.workflow_property
    def get_pu_per_ls(self):
        return self.pu_per_ls

    def build_dict_to_cpp(self):
        d = self.pu_per_ls
        dict_to_cpp = "{"
        for irun, (run, values) in enumerate(d.items()):
            dict_to_cpp += "{%s, {" % run
            for i, value in enumerate(values):
                # if i == 0:
                    # dict_to_cpp += "{%s, %s} " % (value[0], value[1])
                    # break
                dict_to_cpp += "{%s, %s}%s " % (value[0], value[1], (", " if i < len(values) - 1 else ""))
            dict_to_cpp += "}}%s" % (", " if irun < len(d.keys()) - 1 else "")
        dict_to_cpp += "}"
        return dict_to_cpp

    @law.workflow_property
    def get_dict_to_cpp(self):
        return self.dict_to_cpp

    def add_to_root(self, root):
        # root = HltTask.add_to_root(root)
        root.gInterpreter.Declare("""
            std::map <int, std::map<int, float>> pu_lumi = %s;
        """ % self.dict_to_cpp)

        root.gInterpreter.Declare("""
            float compute_weight(int run, int ls) {
                if (pu_lumi[run][ls] == 0) return 0; 
                else return 1./pu_lumi[run][ls];
            }

            float compute_mc_weight(int pu) {
                if (pu == 0) return 0; 
                else return 1./pu;
            }
        """)
        return root

    def run(self):
        import json
        ROOT = import_root()
        ROOT = self.add_to_root(ROOT)

        df = ROOT.RDataFrame(self.tree_name, self.input()["data"].path)
        df = df.Define("weight", "compute_weight(runNumber, lumiBlock)")

        tf = ROOT.TFile.Open(self.input()["data"].path)
        tree = tf.Get(self.tree_name)

        values = {}
        for trigger in self.triggers:
            trigger_req = self.trigger_requirements[trigger]
            hmodel = ("pass_%s" % trigger, "; ; ", 1, 1, 2)
            values[trigger] = df.Define("pass", trigger_req).Histo1D(hmodel, "pass", "weight")

        results = {"total_entries": tree.GetEntries()}
        for trigger in self.triggers:
            results[trigger] = values[trigger].Integral()

        with open(create_file_dir(self.output().path), "w+") as f:
            json.dump(results, f, indent=4)


class HLTRate(Efficiency):

    def requires(self):
        return PreRate.vreq(self)
    
    def output(self):
        return {
            "txt": self.local_target("rates.txt"),
            "tex": self.local_target("rates.tex"),
        }

    def run(self):
        import json
        import tabulate

        values = {trigger: 0 for trigger in self.triggers}
        total_entries = 0
        inp = self.input()
        for elem in inp.collection.targets.values():
            with open(elem.path) as f:
                d = json.load(f)
            for trigger in self.triggers:
                values[trigger] += d[trigger]
            total_entries += d["total_entries"]
        for trigger in self.triggers:
            values[trigger] *= (60 * 2760. * 11246.) / total_entries

        headers = values.keys()
        table = [values.values()]

        print(50*"*")
        print(tabulate.tabulate(table, headers=headers))
        print(50*"*")

        with open(create_file_dir(self.output()["txt"].path), "w+") as f:
            f.write(tabulate.tabulate(table, headers=headers))
        with open(create_file_dir(self.output()["tex"].path), "w+") as f:
            f.write(tabulate.tabulate(table, headers=headers, tablefmt="latex_raw"))

