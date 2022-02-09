# coding: utf-8

import law
import luigi
import os
import json
import tabulate
from copy import deepcopy as copy
from collections import OrderedDict

from analysis_tools.utils import (
    import_root, create_file_dir, join_root_selection
)

from cmt.base_tasks.base import ( 
    Task, DatasetTaskWithCategory, DatasetWrapperTask, HTCondorWorkflow, InputData,
    ConfigTaskWithCategory
)

from cmt.base_tasks.preprocessing import DatasetCategoryWrapperTask

from cmt.tasks.trigger import L1Skim

from cmt.tasks.curves import TurnOnCurve

class HltNtupleTask():
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
            ROOT::RVec<float> remove_dup(Vfloat vec) {
                ROOT::RVec<float> newvec;
                for (auto & elem: vec) {
                    bool found = false;
                    for (auto & elem2: newvec) {
                        if (elem == elem2) {
                            found = true; 
                            break;
                        }
                    }
                    if (!found) newvec.push_back(elem);
                }
                return newvec;
            }
            ROOT::RVec<float> sort_vec(Vfloat vec) {
                ROOT::RVec<float> newvec;
                for (size_t i = 0; i < vec.size(); i++) {
                    newvec.push_back(vec[i]);
                }
                std::sort(newvec.begin(), newvec.end(), std::greater<float>());
                return newvec;
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

class HltTask():
    triggers = law.CSVParameter(
        default=("DoubleHPS30DeepTauJet", "DoubleTau32", "DoubleTauORDoubleTauJet", "L1DoubleTau32"),
        # default=("DoubleHPS35", "DoubleHPS35DeepTau", "DoubleHPS35DeepTauL135",
            # "DoubleHPS38DeepTauL135", "DoubleHPS30DeepTauJet",
            # "DoubleHPS35DeepTauL135ORDoubleHPS30DeepTauJet", 
            # "DoubleHPS38DeepTauL135ORDoubleHPS30DeepTauJet",
            # "DoubleHPS30DeepTau"),
        # default=("DoubleTauJet30", "DoubleTau32", "DoubleTau35", "DoubleTauORDoubleTauJet",
            # "L1DoubleTauJet", "L1DoubleTau32", "L1DoubleTau35", "L1DoubleTauORDoubleTauJet"),
        choices=("DoubleTauJet30", "DoubleTau32", "DoubleTau35", "DoubleTauORDoubleTauJet",
            "L1DoubleTauJet", "L1DoubleTau32", "L1DoubleTau35", "L1DoubleTauORDoubleTauJet",
            "DoubleHPS35", "DoubleHPS35DeepTau", "DoubleHPS30DeepTau", "DoubleHPS35DeepTauL135",
            "DoubleHPS38DeepTauL135", "DoubleHPS30DeepTauJet",
            "DoubleHPS35DeepTauL135ORDoubleHPS30DeepTauJet", 
            "DoubleHPS38DeepTauL135ORDoubleHPS30DeepTauJet", "DoubleHPS35DeepTauManual"),
        description="triggers to compute, default: DoubleTauJet30")
    trigger_ratios = law.CSVParameter(
        # default=(
            # "DoubleHPS35DeepTauL135ORDoubleHPS30DeepTauJet/DoubleHPS35",
            # "DoubleHPS38DeepTauL135ORDoubleHPS30DeepTauJet/DoubleHPS35",
            # "DoubleHPS35DeepTauL135ORDoubleHPS30DeepTauJet/DoubleHPS35DeepTau",
            # "DoubleHPS38DeepTauL135ORDoubleHPS30DeepTauJet/DoubleHPS35DeepTau",
            # # "DoubleHPS35DeepTauManual/DoubleHPS35DeepTau",
            # "DoubleHPS30DeepTau/DoubleHPS35DeepTau",
        # ),
        default=("DoubleTauORDoubleTauJet/DoubleTau32"),
        #default=(),
        description="trigger ratios to compute, default: deeptau paths")

    trigger_requirements = {
        "L1DoubleTauJet": "passhltL1DoubleTauJet == 1",
        "L1DoubleTau32": "passhltL1DoubleTau32 == 1",
        "L1DoubleTau35": "passhltL1DoubleTau35 == 1",
        "L1DoubleTauORDoubleTauJet": "passhltL1DoubleTau35 == 1 || passhltL1DoubleTauJet == 1",
        "DoubleTauJet30": "passDoubleTauJetHLT == 1",
        "DoubleTau32": "passDoubleTau32HLT == 1",
        "DoubleTau35": "passDoubleTau35HLT == 1",
        "DoubleTauORDoubleTauJet": "passDoubleTau32HLT == 1 || passDoubleTauJetHLT == 1",
        "DoubleHPS35": "HLT_DoubleMediumChargedIsoPFTauHPS35_Trk1_eta2p1_Reg_v4 == 1",
        "DoubleHPS35DeepTau": "HLT_DoubleMediumDeepTauIsoPFTauHPS35_L2NN_eta2p1_v1 == 1",
        "DoubleHPS30DeepTau": "HLT_DoubleMediumDeepTauIsoPFTauHPS30_L2NN_eta2p1_v1 == 1",
        "DoubleHPS35DeepTauManual":
            "passhltL1DoubleTau32 == 1 && remove_dup(hltHpsDoublePFTau20MediumDitauWPDeepTauL1HLTMatchedL120_pt"
                "[hltHpsDoublePFTau20MediumDitauWPDeepTauL1HLTMatchedL120_pt >= 35]).size() >= 2",
        "DoubleHPS35DeepTauL135": "HLT_DoubleMediumDeepTauIsoPFTauHPS35_L2NN_eta2p1_L135_v1 == 1",
        "DoubleHPS38DeepTauL135": "HLT_DoubleMediumDeepTauIsoPFTauHPS38_L2NN_eta2p1_L135_v1 == 1",
        # "DoubleHPS30DeepTauJet": "HLT_DoubleMediumDeepTauIsoPFTauHPS30_L2NN_eta2p1_PFJet60_v1 == 1",
        "DoubleHPS30DeepTauJet": "passDoubleTauJetHLT == 1",
        "DoubleHPS35DeepTauL135ORDoubleHPS30DeepTauJet":
            ("HLT_DoubleMediumDeepTauIsoPFTauHPS35_L2NN_eta2p1_L135_v1 == 1"
                "|| HLT_DoubleMediumDeepTauIsoPFTauHPS30_L2NN_eta2p1_PFJet60_v1 == 1"),
        "DoubleHPS38DeepTauL135ORDoubleHPS30DeepTauJet": 
            ("HLT_DoubleMediumDeepTauIsoPFTauHPS38_L2NN_eta2p1_L135_v1 == 1"
                "|| HLT_DoubleMediumDeepTauIsoPFTauHPS30_L2NN_eta2p1_PFJet60_v1 == 1"),
    }
    
    acceptance_requirements = {
        "L1DoubleTauJet": [(26, 55)],
        "L1DoubleTau32": [(32, -999)],
        "L1DoubleTau35": [(35, -999)],
        "L1DoubleTauORDoubleTauJet": [(35, -999), (26, 55)],
        "DoubleTauJet30": [(26, 55)],
        "DoubleTau32": [(32, -999)],
        "DoubleTau35": [(35, -999)],
        "DoubleTauORDoubleTauJet": [(35, -999), (26, 55)],
        "DoubleHPS35": [(32, -999)],
        "DoubleHPS35DeepTau": [(32, -999)],
        "DoubleHPS30DeepTau": [(27, -999)],
        "DoubleHPS35DeepTauManual": [(32, -999)],
        "DoubleHPS35DeepTauL135": [(35, -999)],
        "DoubleHPS38DeepTauL135": [(35, -999)],
        "DoubleHPS30DeepTauJet": [(26, 55)],
        "DoubleHPS35DeepTauL135ORDoubleHPS30DeepTauJet": [(35, -999), (26, 55)],
        "DoubleHPS38DeepTauL135ORDoubleHPS30DeepTauJet": [(35, -999), (26, 55)],
    }
    
    def __init__(self, *args, **kwargs):
        self.trigger_reqs = [trigger_requirements[trigger]
            for trigger in self.triggers if trigger in trigger_definitions]


class PreEfficiency(DatasetTaskWithCategory, law.LocalWorkflow, HTCondorWorkflow, HltTask, HltNtupleTask):
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
        return {
            "histo": self.local_target(self.get_output_filename()),
            "json": self.local_target(self.get_output_filename().replace(".root", ".json")),
        }

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
        counts = {}
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

            acceptance_req = self.acceptance_requirements[trigger]
            acc_string = trigger_req.split("||")
            acc_string = join_root_selection(
                [join_root_selection([
                    elem1,
                    "leading_tau_pt >= %s + %s" % (elem2[0], self.category.get_aux("add_to_leading_pt")),
                    "subleading_tau_pt >= %s + %s" % (elem2[0], self.category.get_aux("add_to_subleading_pt")),
                    "leading_jet_pt >= %s + %s" % (elem2[1], self.category.get_aux("add_to_trigger_jet_pt", 10))
                ], op="and") for (elem1, elem2) in zip(acc_string, acceptance_req)], op="or")
            total_string = join_root_selection(
                [join_root_selection([
                    "leading_tau_pt >= %s + %s" % (elem[0], self.category.get_aux("add_to_leading_pt")),
                    "subleading_tau_pt >= %s + %s" % (elem[0], self.category.get_aux("add_to_subleading_pt")),
                    "leading_jet_pt >= %s + %s" % (elem[1], self.category.get_aux("add_to_trigger_jet_pt", 10))
                ], op="and") for elem in acceptance_req], op="or")

            # print acc_string
            # print total_string

            counts[trigger] = {}
            counts[trigger]["total"] = df.Define("leading_jet_pt", "lead_sublead_goodjet_pt[0]"
                ).Define("leading_tau_pt", "lead_sublead_goodtau_pt[0]"
                ).Define("subleading_tau_pt", "lead_sublead_goodtau_pt[1]"
                ).Filter(total_string).Count()
            counts[trigger]["pass"] = df.Define("leading_jet_pt", "lead_sublead_goodjet_pt[0]"
                ).Define("leading_tau_pt", "lead_sublead_goodtau_pt[0]"
                ).Define("subleading_tau_pt", "lead_sublead_goodtau_pt[1]"
                ).Filter(acc_string).Count()

        tf = ROOT.TFile.Open(create_file_dir(self.output()["histo"].path), "RECREATE")
        tf.cd()
        for histo in histos.values():
            histo.Write()
        tf.Close()

        for trigger in self.triggers:
            counts[trigger]["pass"] = counts[trigger]["pass"].GetValue()
            counts[trigger]["total"] = counts[trigger]["total"].GetValue()
        with open(create_file_dir(self.output()["json"].path), "w+") as f:
            json.dump(counts, f, indent=4)


class Efficiency(DatasetTaskWithCategory, TurnOnCurve, HltTask):
    

    def requires(self):
        return PreEfficiency.vreq(self)

    def output(self):
        output = {}
        output["plots"] = {}
        output["stats"] = self.local_target("stats.json")
        for name in ["tau_pt", "jet_pt", "tau_pt_jet_pt"]:
            output["plots"]["%s" % name] = self.local_target("distributions/%s.pdf" % name)
            for trigger in self.triggers:
                output["plots"]["%s_%s" % (name, trigger)] = self.local_target("turnons/efficiency_%s_%s.pdf" % (name, trigger))
            for trigger in self.trigger_ratios:
                output["plots"]["%s_%s" % (name, trigger)] = self.local_target("ratios/efficiency_%s_%s.pdf" % (name, trigger.replace("/", "_")))

        return output

    def run(self):
        self.upper_left_text = "Work in progress"
        self.upper_right_text = "Run 3 Simulation"
        self.inner_text = [self.dataset.process.label, self.category.label]
        super(Efficiency, self).plot_curves()


class EfficiencyWrapper(DatasetCategoryWrapperTask):

    def atomic_requires(self, dataset, category):
        return Efficiency.vreq(self, dataset_name=dataset.name, category_name=category.name)


class HLTAcceptanceSummary(DatasetWrapperTask, HltTask):
    category_names = law.CSVParameter(default=(), description="names or name "
        "patterns of categories to use, uses all datasets when empty, default: ()")

    def __init__(self, *args, **kwargs):
        super(HLTAcceptanceSummary, self).__init__(*args, **kwargs) 
        self.categories = [self.config.categories.get(cat_name) for cat_name in self.category_names]
        assert len(self.categories) == len(self.datasets) or len(self.categories) == 1
        if len(self.categories) == 1:
            self.categories = [self.categories[0] for i in range(len(self.datasets))]

    def requires(self):
        reqs = {}
        for dataset, category in zip(self.datasets, self.categories):
            reqs["%s_%s" % (dataset.name, category.name)] = Efficiency.vreq(
                self, dataset_name=dataset.name, category_name=category.name)
        return reqs

    def output(self):
        return {
            "txt": self.local_target("stats.txt"),
            "tex": self.local_target("stats.tex")
        }

    def run(self):
        headers = [""] + ["%s, %s" % (dataset.name, category.name)
            for (dataset, category) in zip(self.datasets, self.categories)]
        table = []
        for trigger in self.trigger_ratios:
            line = [trigger]
            for dataset, category in zip(self.datasets, self.categories):
                with open(self.input()["%s_%s" % (dataset.name, category.name)]["stats"].path) as f:
                    d = json.load(f)
                    line.append(d["acc_gain"][trigger])
            table.append(line)
        table_txt = tabulate.tabulate(table, headers=headers)
        table_tex = tabulate.tabulate(table, headers=headers, tablefmt="latex_raw")
        table_tex = table_tex.replace("_", "\_")

        print "\n" + "*" * 50
        print table_tex
        print "*" * 50 + "\n"


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
        return {
            "json": self.local_target(self.get_output_filename().replace(".root", ".json")),
            "root": self.local_target(self.get_output_filename())
        }
    
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
            float get_pu(int run, int ls) {
                return pu_lumi[run][ls];
            }
        
            float compute_weight(int run, int ls) {
                if (get_pu(run, ls) == 0) return 0; 
                else return 1./get_pu(run, ls);
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
        df = df.Define("pu", "get_pu(runNumber, lumiBlock)").Filter("pu >= 30.").Define(
            "weight", "compute_weight(runNumber, lumiBlock)")

        hist_base = ROOT.TH2D("pu_vs_lumi", "; lumi; pu", 1000, 0, 1000, 60, 0, 60)
        hmodel = ROOT.RDF.TH2DModel(hist_base)
        histo = df.Histo2D(hmodel, "lumiBlock", "pu")

        values = {}
        values_no_weight = {}
        total_entries = df.Count()

        for trigger in self.triggers:
            trigger_req = self.trigger_requirements[trigger]
            hmodel = ("pass_%s" % trigger, "; ; ", 1, 1, 2)
            values[trigger] = df.Define("pass", trigger_req).Histo1D(hmodel, "pass", "weight")
            values_no_weight[trigger] = df.Define("pass", trigger_req).Histo1D(hmodel, "pass")

        results = {
            "total_entries": total_entries.GetValue(),
            "weighted": {},
            "non_weighted": {}
        }

        for trigger in self.triggers:
            results["weighted"][trigger] = values[trigger].Integral()
            results["non_weighted"][trigger] = values_no_weight[trigger].Integral()

        with open(create_file_dir(self.output()["json"].path), "w+") as f:
            json.dump(results, f, indent=4)

        tf = ROOT.TFile.Open(create_file_dir(self.output()["root"].path), "RECREATE")
        histo.Write()
        tf.Close()


class HLTRate(Efficiency):

    def requires(self):
        return PreRate.vreq(self)

    def output(self):
        return {
            "txt": self.local_target("rates.txt"),
            "tex": self.local_target("rates.tex"),
            "pdf": self.local_target("pu_vs_lumi.pdf"),
        }

    def run(self):
        ROOT = import_root()
        ROOT.gStyle.SetOptStat(0)
        events = {trigger: 0 for trigger in self.triggers}
        values = {trigger: 0 for trigger in self.triggers}
        values_noweight = {trigger: 0 for trigger in self.triggers}
        total_entries = 0
        inp = self.input()
        histo = ""
        for elem in inp.collection.targets.values():
            with open(elem["json"].path) as f:
                d = json.load(f)
            for trigger in self.triggers:
                values[trigger] += d["weighted"][trigger]
                # if d["weighted"][trigger] != 0:
                    # print elem.path
                values_noweight[trigger] += d["non_weighted"][trigger]
                events[trigger] += d["non_weighted"][trigger]
            total_entries += d["total_entries"]
            tf = ROOT.TFile.Open(elem["root"].path)
            if histo == "":
                histo = copy(tf.Get("pu_vs_lumi").Clone())
            else:
                histo.Add(tf.Get("pu_vs_lumi").Clone())
            
        for trigger in self.triggers:
            values[trigger] *= (53 * 2544. * 11246.) / total_entries
            values_noweight[trigger] *= (2544. * 11246.) / total_entries
            # values[trigger] *= (53 * 76883.45) / total_entries
            #values_noweight[trigger] *= (76883.45) / total_entries

        headers = [""] + values.keys()
        table = [["Rate (sc. PU 53)"] + values.values()]
        table +=[["Rate (No sc.)"] + values_noweight.values()]
        table +=[["Events"] + events.values()]

        print(50*"*")
        print "Total events:", total_entries
        print(tabulate.tabulate(table, headers=headers))
        print(50*"*")

        with open(create_file_dir(self.output()["txt"].path), "w+") as f:
            f.write(tabulate.tabulate(table, headers=headers))
        with open(create_file_dir(self.output()["tex"].path), "w+") as f:
            f.write(tabulate.tabulate(table, headers=headers, tablefmt="latex_raw"))

        nbinsx = histo.GetNbinsX()
        nbinsy = histo.GetNbinsY()
        last_bin = nbinsx
        for ibin in range(nbinsy, 0, -1):
            if histo.Integral(ibin, ibin, 1, nbinsy) != 0:
                last_bin = ibin
                break
        if last_bin < 500:
            histo.GetXaxis().SetRangeUser(0, 500)

        c = ROOT.TCanvas()
        histo.SetTitle("; LumiBlock; PU")
        histo.Draw()
        c.SaveAs(create_file_dir(self.output()["pdf"].path))


class L1HLTTurnOnProducer(DatasetTaskWithCategory, law.LocalWorkflow, HTCondorWorkflow, HltNtupleTask):  
    tree_name = "demo/hltdev"

    default_store = "$CMT_STORE_EOS_CATEGORIZATION"
    default_wlcg_fs = "wlcg_fs_categorization"
    
    triggers = {
        "DoubleIsoTau32": "passhltL1DoubleTau32 == 1",
        "DoubleIsoTau35": "passhltL1DoubleTau35 == 1",
        "DoubleIsoTauJet": "passhltL1DoubleTauJet == 1",
    }
    
    trigger_ratios = []

    def __init__(self, *args, **kwargs):
        super(L1HLTTurnOnProducer, self).__init__(*args, **kwargs)

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
        return {
            "histo": self.local_target(self.get_output_filename()),
            # "json": self.local_target(self.get_output_filename().replace(".root", ".json")),
        }

    @law.decorator.notify
    @law.decorator.localize(input=False)
    def run(self):
        ROOT = import_root()
        ROOT = self.add_to_root(ROOT)
        # ROOT.ROOT.EnableImplicitMT()

        df = ROOT.RDataFrame(self.tree_name, self.input()["data"].path)
        if self.category.name != "base":
            df = self.define_taus(df)
            df = self.filter_jets(df)
            category = self.category
            while category.get_aux("parent_category", None):
                category = self.config.categories.get(category.get_aux("parent_category", None))
                df = self.filter_jets(df, category=category)

        histos = {}
        # taus
        hmodel = ("tau_pt_total", "; subleading HLT tau pt [GeV]; Events / 2 GeV", 40, 20, 100)
        histos["tau_pt_total"] = df.Define("subleading_tau_pt",
            "sort_vec(lead_sublead(remove_dup(hltHpsDoublePFTau20MediumDitauWPDeepTauL1HLTMatchedL120_pt), 3))[1]"
        ).Histo1D(hmodel, "subleading_tau_pt")
        hmodel = ("tau_pt_jet_pt_100_total", "; subleading HLT tau pt [GeV]; Events / 2 GeV", 40, 20, 100)
        histos["tau_pt_jet_pt_100_total"] = df.Filter("jetPt["
            "jetPt >= 100 && abs(jetEta) <= 4.7 && jetID >= 2 "
                "&& ((jetPUFullID >= 4 && jetPt <= 50) || (jetPt > 50))"
            "].size() >= 1").Define("subleading_tau_pt",
            "sort_vec(lead_sublead(remove_dup(hltHpsDoublePFTau20MediumDitauWPDeepTauL1HLTMatchedL120_pt), 3))[1]"
        ).Histo1D(hmodel, "subleading_tau_pt")

        for trigger, trigger_req in self.triggers.items():
            hmodel = ("tau_pt_%s_pass" % trigger, "; subleading tau pt [GeV]; Events / 2 GeV",
                40, 20, 100)
            histos["tau_pt_%s_pass" % trigger] = df.Filter(trigger_req).Define(
                "subleading_tau_pt",
                "sort_vec(lead_sublead(remove_dup(hltHpsDoublePFTau20MediumDitauWPDeepTauL1HLTMatchedL120_pt), 3))[1]"
                ).Histo1D(hmodel, "subleading_tau_pt")
            hmodel = ("tau_pt_jet_pt_100_%s_pass" % trigger, "; subleading tau pt [GeV]; Events / 2 GeV", 40, 20, 100)
            histos["tau_pt_jet_pt_100_%s_pass" % trigger] = df.Filter(trigger_req).Filter(
                "jetPt["
                    "jetPt >= 100 && abs(jetEta) <= 4.7 && jetID >= 2 "
                    "&& ((jetPUFullID >= 4 && jetPt <= 50) || (jetPt > 50))"
                "].size() >= 1").Define("subleading_tau_pt",
                "sort_vec(lead_sublead(remove_dup(hltHpsDoublePFTau20MediumDitauWPDeepTauL1HLTMatchedL120_pt), 3))[1]"
            ).Histo1D(hmodel, "subleading_tau_pt")

        tf = ROOT.TFile.Open(create_file_dir(self.output()["histo"].path), "RECREATE")
        tf.cd()
        for histo in histos.values():
            histo.Write()
        tf.Close()


class L1HLTTurnOnPlotter(DatasetTaskWithCategory, TurnOnCurve):

    triggers = L1HLTTurnOnProducer.triggers
    trigger_ratios = L1HLTTurnOnProducer.trigger_ratios
    names = ["tau_pt", "tau_pt_jet_pt_100"]
    names2D = []

    def requires(self):
        return L1HLTTurnOnProducer.vreq(self)

    def output(self):
        output = {}
        output["plots"] = {}
        for name in self.names + self.names2D:
            output["plots"]["%s" % name] = self.local_target("distributions/%s.pdf" % name)
            for trigger in self.triggers:
                output["plots"]["%s_%s" % (name, trigger)] = self.local_target("turnons/efficiency_%s_%s.pdf" % (name, trigger))
            for trigger in self.trigger_ratios:
                output["plots"]["%s_%s" % (name, trigger)] = self.local_target("ratios/efficiency_%s_%s.pdf" % (name, trigger.replace("/", "_")))

        return output

    def run(self):
        self.upper_left_text = "Work in progress"
        self.upper_right_text = "Run 3 Simulation"
        self.inner_text = [self.dataset.process.label, self.category.label]
        super(L1HLTTurnOnPlotter, self).plot_curves()



class HLTFilterProducer(DatasetTaskWithCategory, law.LocalWorkflow, HTCondorWorkflow, HltNtupleTask):  
    tree_name = "demo/hltdev"

    default_store = "$CMT_STORE_EOS_CATEGORIZATION"
    default_wlcg_fs = "wlcg_fs_categorization"
    
    filters = [
        "nEvents",
        #"passhltL1DoubleTauJet",
        "passhltL2DoubleTauTagNNFilterDoubleTauJet",
        "passhltHpsPFTauTrack",
        "passhltHpsDoublePFTau30MediumDitauWPDeepTauL1HLTMatchedDoubleTauJet",
        "passhltHpsDoublePFTau30MediumDitauWPDeepTauDz02DoubleTauJet",
        #"passhltPFJets60L1HLTMatched",
        "passhltHpsOverlapFilterDeepTauDoublePFTau30PFJet60",
        "passDoubleTauJetHLT",
    ]
    
    trigger_ratios = []

    def __init__(self, *args, **kwargs):
        super(HLTFilterProducer, self).__init__(*args, **kwargs)

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
        return {
            "json": self.local_target(self.get_output_filename().replace(".root", ".json")),
            "root": self.local_target(self.get_output_filename())
        }
            #"histo": self.local_target(self.get_output_filename()),
            

    @law.decorator.notify
    @law.decorator.localize(input=False)
    def run(self):
        ROOT = import_root()
        ROOT = self.add_to_root(ROOT)
        # ROOT.ROOT.EnableImplicitMT()

        df = ROOT.RDataFrame(self.tree_name, self.input()["data"].path)
        df = self.define_taus(df)
        df = self.define_jets(df)
        if self.category.name != "base":
            df = self.filter_jets(df)
            category = self.category
            while category.get_aux("parent_category", None):
                category = self.config.categories.get(category.get_aux("parent_category", None))
                df = self.filter_jets(df, category=category)

        df = df.Define("subleading_tau_pt", "lead_sublead_goodtau_pt[1]").Define(
            "leading_jet_pt", "lead_sublead_goodjet_pt[0]").Filter(
            "subleading_tau_pt > 35 && leading_jet_pt > 70")

        counts = []
        for f in self.filters:
            df = df.Filter("%s == 1" % f)
            counts.append(df.Count())

        counts_to_print = dict([(f, int(count.GetValue())) for f, count in zip(self.filters, counts)])

        # from pprint import pprint
        # pprint(counts_to_print)

        with open(create_file_dir(self.output()["json"].path), "w+") as f:
            json.dump(counts_to_print, f, indent=2)
        df.Snapshot(self.tree_name, self.output()["root"].path)


class HLTFilterDataProducer(HLTFilterProducer):
    @law.decorator.notify
    @law.decorator.localize(input=False)
    def run(self):
        ROOT = import_root()
        ROOT = self.add_to_root(ROOT)
        # ROOT.ROOT.EnableImplicitMT()

        run_numbers = [321755, 323725, 323755, 323790, 323841, 323940, 323976, 323978, 324021,
            324077, 324201, 324237, 324245, 324293, 324315, 324420, 324747, 324785, 324835, 324897,
            324970, 324980, 324997, 325022, 325057, 325097, 325098, 325099]
        
        df = ROOT.RDataFrame(self.tree_name, self.input()["data"].path)
        counts = []
        for f in self.filters:
            df = df.Filter("%s == 1" % f)
            counts.append(df.Count())

        run_results = {}
        for run_number in run_numbers:
            run_results[str(run_number)] = df.Filter("runNumber == %s" % run_number).Count()

        counts_to_print = dict([(f, int(count.GetValue())) for f, count in zip(self.filters, counts)])
        for run_number in run_numbers:
            counts_to_print[str(run_number)] = run_results[str(run_number)].GetValue()

        with open(create_file_dir(self.output()["json"].path), "w+") as f:
            json.dump(counts_to_print, f, indent=2)

        df.Snapshot(self.tree_name, self.output()["root"].path)


class HLTFilter(DatasetTaskWithCategory):
    
    filters = HLTFilterProducer.filters

    def requires(self):
        return HLTFilterProducer.vreq(self)
    
    def output(self):
        return {
            "txt": self.local_target("filters.txt"),
            "tex": self.local_target("filters.tex"),
        }

    def run(self):

        run_numbers = [321755, 323725, 323755, 323790, 323841, 323940, 323976, 323978, 324021,
            324077, 324201, 324237, 324245, 324293, 324315, 324420, 324747, 324785, 324835, 324897,
            324970, 324980, 324997, 325022, 325057, 325097, 325098, 325099]

        values = OrderedDict([(f, 0) for f in self.filters + [str(r) for r in run_numbers]])
        inp = self.input()
        for elem in inp.collection.targets.values():
            with open(elem["json"].path) as f:
                d = json.load(f)
            for f in self.filters + [str(r) for r in run_numbers]:
                values[f] += d[f]
        
        from pprint import pprint
        pprint(values)

        headers = ["Filter", "Number of events", "Accumulative", "Non accumulative"]
        table = []
        for indexf, f in enumerate(self.filters):
            if indexf == 0:
                table.append([f, values[f], 100, 100])
            else:
                nevents = float(table[0][1])
                nprevevents = float(table[-1][1])
                acc = (100 - (100 * (table[0][1] - values[f]) / nevents) if nevents != 0 else 0)
                non_acc = (100 - (100 * (nprevevents - values[f]) / nprevevents) if nprevevents != 0 else 0)
                table.append([f, values[f], "{:.2f}".format(acc), "{:.2f}".format(non_acc)])

        print(50*"*")
        print(tabulate.tabulate(table, headers=headers))
        print(50*"*")

        with open(create_file_dir(self.output()["txt"].path), "w+") as f:
            f.write(tabulate.tabulate(table, headers=headers))
        with open(create_file_dir(self.output()["tex"].path), "w+") as f:
            f.write(tabulate.tabulate(table, headers=headers, tablefmt="latex_raw"))


class HLTFilterData(HLTFilter):
    def requires(self):
        return HLTFilterDataProducer.vreq(self)
