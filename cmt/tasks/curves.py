import os
import law
import luigi
import json
import math
import itertools
from analysis_tools.utils import (
    import_root, create_file_dir, join_root_selection
)
from copy import deepcopy as copy

from plotting_tools.root.labels import get_labels
from plotting_tools.root.canvas import Canvas

from cmt.base_tasks.base import ( 
    Task, DatasetTaskWithCategory, DatasetWrapperTask, HTCondorWorkflow, InputData,
    ConfigTaskWithCategory
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
                        histos[name_tag] = copy(rootfile.Get(name_tag))
        
        for name in histo_names + histo_names_2D:
            c = ROOT.TCanvas()
            eff = ROOT.TEfficiency(histos["%s_passed" % name], histos["%s_total" % name])
            if name in histo_names:
                eff.Draw()
            else:
                eff.Draw("colz")
            c.SaveAs(create_file_dir(output[name].path))


class L1Task():
    triggers = law.CSVParameter(
        # default=("DoubleTauJet30", "DoubleTau32", "DoubleTau35", "DoubleTauORDoubleTauJet"),
        default=("L1DoubleIsoTau32", "L1DoubleIsoTau35", "L1DoubleIsoTauJet", "L1DoubleIsoTauORDoubleTauJet"),
        choices=("L1DoubleIsoTau32", "L1DoubleIsoTau35", "L1DoubleIsoTauJet", "L1DoubleIsoTauORDoubleTauJet"),
        description="triggers to compute, default: a lot")
    trigger_ratios = law.CSVParameter(
        default=("L1DoubleIsoTauORDoubleTauJet/L1DoubleIsoTau32",),
        description="trigger ratios to compute, default: L1DoubleIsoTauORDoubleTauJet/L1DoubleIsoTau32")

    trigger_requirements = {
        "L1DoubleIsoTau32": "passDoubleIsoTau32 == 1",
        "L1DoubleIsoTau35": "passDoubleIsoTau35 == 1",
        "L1DoubleIsoTauJet": "passDoubleIsoTauJet == 1",
        "L1DoubleIsoTauORDoubleTauJet": "passDoubleIsoTauORDoubleIsoTauJet == 1",
    }


class L1TurnOnCurveProducer(DatasetTaskWithCategory, law.LocalWorkflow, HTCondorWorkflow, L1Task):
    
    tree_name = "Events"

    default_store = "$CMT_STORE_EOS_CATEGORIZATION"
    default_wlcg_fs = "wlcg_fs_categorization"

    def __init__(self, *args, **kwargs):
        super(L1TurnOnCurveProducer, self).__init__(*args, **kwargs)

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
            using Vint = const ROOT::RVec<int>&;
            ROOT::RVec<int> get_triggers(int nL1Obj, Vfloat L1Obj_pt, Vfloat L1Obj_eta, Vfloat L1Obj_phi, Vint L1Obj_type, Vint L1Obj_iso) {
                ROOT::RVec<float> L1tau_pt, L1tau_eta, L1tau_phi;
                ROOT::RVec<float> L1jet_pt, L1jet_eta, L1jet_phi;
                ROOT::RVec<int> triggers = {0, 0, 0};
                for (size_t iL1Obj = 0; iL1Obj < nL1Obj; iL1Obj++) {
                    // L1 taus
                    if (L1Obj_pt[iL1Obj] >= 26 && L1Obj_iso[iL1Obj] == 1 && L1Obj_type[iL1Obj] == 1 && fabs(L1Obj_eta[iL1Obj]) <= 2.1) {
                        L1tau_pt.push_back(L1Obj_pt[iL1Obj]);
                        L1tau_eta.push_back(L1Obj_eta[iL1Obj]);
                        L1tau_phi.push_back(L1Obj_phi[iL1Obj]);
                    // L1 jets
                    } else if (L1Obj_pt[iL1Obj] >= 55 && L1Obj_type[iL1Obj] == 0) {
                        L1jet_pt.push_back(L1Obj_pt[iL1Obj]);
                        L1jet_eta.push_back(L1Obj_eta[iL1Obj]);
                        L1jet_phi.push_back(L1Obj_phi[iL1Obj]);
                    }
                }
                if (L1tau_pt.size() < 2)
                    return triggers;

                std::vector <std::vector<float>> l1_triplets;
                for (size_t iLeadL1tau = 0; iLeadL1tau < L1tau_pt.size() - 1; iLeadL1tau++) {
                    for (size_t iSubleadL1tau = iLeadL1tau + 1; iSubleadL1tau < L1tau_pt.size(); iSubleadL1tau++) {
                        for (size_t iLeadL1jet = 0; iLeadL1jet < L1jet_pt.size(); iLeadL1jet++) {
                            
                            Double_t deta_lead = L1jet_eta[iLeadL1jet] - L1tau_eta[iLeadL1tau];
                            Double_t dphi_lead = Phi_mpi_pi(L1jet_phi[iLeadL1jet] - L1tau_phi[iLeadL1tau]);
                            Double_t dr_lead = TMath::Sqrt(deta_lead * deta_lead + dphi_lead * dphi_lead);
                            
                            Double_t deta_sublead = L1jet_eta[iLeadL1jet] - L1tau_eta[iSubleadL1tau];
                            Double_t dphi_sublead = Phi_mpi_pi(L1jet_phi[iLeadL1jet] - L1tau_phi[iSubleadL1tau]);
                            Double_t dr_sublead = TMath::Sqrt(deta_sublead * deta_sublead + dphi_sublead * dphi_sublead);
                            
                            if (dr_lead < 0.5 || dr_sublead < 0.5) continue;
                            l1_triplets.push_back(std::vector<float>({L1tau_pt[iLeadL1tau], L1tau_pt[iSubleadL1tau], L1jet_pt[iLeadL1jet]}));               
                            break;
                        }
                    }
                }
                Float_t leading_l1tau_pt_ = L1tau_pt[0];
                Float_t subleading_l1tau_pt_ = L1tau_pt[1];

                if (leading_l1tau_pt_ >= 32 && subleading_l1tau_pt_ >= 32)  triggers[0] = 1;
                if (leading_l1tau_pt_ >= 35 && subleading_l1tau_pt_ >= 35)  triggers[1] = 1;

                for (auto triplet: l1_triplets) {
                    if (triplet[0] >=  26
                            && triplet[1] >=  26
                            && triplet[2] >=  55) {
                        triggers[2] = 1;
                        break;
                    }
                }
                return triggers;
            }
            
        """)
        return root

    def define_taus(self, df):
        df = df.Define("lead_sublead_goodtau_pt",
                "lead_sublead(Tau_pt[abs(Tau_eta) <= 2.1"
                    " && Tau_idDeepTau2017v2p1VSmu >= 1 && Tau_idDeepTau2017v2p1VSe >= 3 && Tau_idDeepTau2017v2p1VSjet >= 31"
                    " && abs(Tau_dz) < 0.2], 3)"
            ).Define("lead_sublead_goodtau_eta",
                "lead_sublead(Tau_pt[abs(Tau_eta) <= 2.1"
                    " && Tau_idDeepTau2017v2p1VSmu >= 1 && Tau_idDeepTau2017v2p1VSe >= 3 && Tau_idDeepTau2017v2p1VSjet >= 31"
                    " && abs(Tau_dz) < 0.2], 3)"
            ).Define("lead_sublead_goodtau_phi",
                "lead_sublead(Tau_pt[abs(Tau_eta) <= 2.1"
                    " && Tau_idDeepTau2017v2p1VSmu >= 1 && Tau_idDeepTau2017v2p1VSe >= 3 && Tau_idDeepTau2017v2p1VSjet >= 31"
                    " && abs(Tau_dz) < 0.2], 3)"
            )
        return df

    def define_jets(self, df):
        df = df.Define("lead_sublead_goodjet_pt",
                "lead_sublead(Jet_pt["
                    "abs(Jet_eta) <= 4.7 && Jet_jetId >= 2 "
                    "&& ((Jet_puId >= 4 && Jet_pt <= 50) || (Jet_pt > 50))"
                    "&& maskDeltaR("
                        "Jet_eta, "
                        "Jet_phi, "
                        "lead_sublead_goodtau_eta, "
                        "lead_sublead_goodtau_phi, "
                        "0.5)"
                    "], 3)"
            ).Define("lead_sublead_goodjet_eta",
                "lead_sublead(Jet_eta["
                    "abs(Jet_eta) <= 4.7 && Jet_jetId >= 2 "
                    "&& ((Jet_puId >= 4 && Jet_pt <= 50) || (Jet_pt > 50))"
                    "&& maskDeltaR("
                        "Jet_eta, "
                        "Jet_phi, "
                        "lead_sublead_goodtau_eta, "
                        "lead_sublead_goodtau_phi, "
                        "0.5)"
                    "], 3)"
            ).Define("lead_sublead_goodjet_phi",
                "lead_sublead(Jet_phi["
                    "abs(Jet_eta) <= 4.7 && Jet_jetId >= 2 "
                    "&& ((Jet_puId >= 4 && Jet_pt <= 50) || (Jet_pt > 50))"
                    "&& maskDeltaR("
                        "Jet_eta, "
                        "Jet_phi, "
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
            "Jet_pt["
                "Jet_pt >= (20 + {0}) && abs(Jet_eta) <= 4.7 && Jet_jetId >= 2 "
                "&& ((Jet_puId >= 4 && Jet_pt <= 50) || (Jet_pt > 50))"
                "&& maskDeltaR("
                    "Jet_eta, "
                    "Jet_phi, "
                    "lead_sublead_goodtau_eta, "
                    "lead_sublead_goodtau_phi, "
                    "0.5)"
                "].size() >= {1} "
            # ask for a maximum number of jets (depending on the category)
            # with pt > 20 (+ sth depending on the dataset)
            "&& Jet_pt["
                "Jet_pt >= (20 + {0}) && abs(Jet_eta) <= 4.7 && Jet_jetId >= 2 "
                "&& ((Jet_puId >= 4 && Jet_pt <= 50) || (Jet_pt > 50))"
                "&& maskDeltaR("
                    "Jet_eta, "
                    "Jet_phi, "
                    "lead_sublead_goodtau_eta, "
                    "lead_sublead_goodtau_phi, "
                    "0.5)"
                "].size() <= {2}".format(
                    category.get_aux("add_to_jet_pt"),
                    category.get_aux("nminjets"),
                    category.get_aux("nmaxjets")
                )
        )

    def define_triggers(self, df):
        df = df.Define("triggers", "get_triggers(nL1Obj, L1Obj_pt, L1Obj_eta, L1Obj_phi, L1Obj_type, L1Obj_iso)")
        df = df.Define("passDoubleIsoTau32", "triggers[0]")
        df = df.Define("passDoubleIsoTau35", "triggers[1]")
        df = df.Define("passDoubleIsoTauJet", "triggers[2]")
        df = df.Define("passDoubleIsoTauORDoubleIsoTauJet", "passDoubleIsoTau35 == 1 || passDoubleIsoTauJet == 1")
        return df

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
        df = self.define_triggers(df)
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


class TurnOnCurve():
    names = ["tau_pt", "jet_pt"]
    names2D = ["tau_pt_jet_pt"]

    upper_left_text = "Work in progress"
    upper_right_text = "Run 3 Simulation"
    inner_text = []
    
    def plot_curves(self):
        ROOT = import_root()
        ROOT.gStyle.SetOptStat(0)
        histos = {}

        labels = get_labels(upper_left=self.upper_left_text,
            upper_right=self.upper_right_text, inner_text=self.inner_text)

        inp = self.input()
        output = self.output()

        counts = {}
        for elem in inp.collection.targets.values():
            rootfile = ROOT.TFile.Open(elem["histo"].path)
            for name in self.names + self.names2D:
                if "%s_total" % name not in histos:
                    histos["%s_total" % name] = copy(rootfile.Get(
                        "%s_total" % name).Clone())
                else:
                    histos["%s_total" % (name)].Add(
                        rootfile.Get("%s_total" % (name)))
            for trigger in self.triggers:
                for name in self.names + self.names2D:
                    if "%s_%s_pass" % (name, trigger) not in histos:
                        histos["%s_%s_pass" % (name, trigger)] = copy(rootfile.Get(
                            "%s_%s_pass" % (name, trigger)).Clone())
                    else:
                        histos["%s_%s_pass" % (name, trigger)].Add(
                            rootfile.Get("%s_%s_pass" % (name, trigger)))
            if "json" in elem:
                with open(elem["json"].path) as f:
                    d = json.load(f)
                    for trigger, values in d.items():
                        if trigger not in counts:
                            counts[trigger] = values
                        else:
                            counts[trigger]["pass"] += values["pass"]
                            counts[trigger]["total"] += values["total"]

        for name in self.names + self.names2D:
            c = Canvas()
            histos["%s_total" % name].Draw()
            for label in labels:
                label.Draw("same")
            c.SaveAs(create_file_dir(output["plots"]["%s" % name].path))
            del c
        
        d = {}
        d["efficiencies"] = {}
        for trigger in self.triggers:
            for name in self.names + self.names2D:
                c = Canvas()
                if name in self.names:
                    histos["%s_%s_pass" % (name, trigger)].GetYaxis().SetTitle("Efficiency")
                    histos["%s_total" % name].GetYaxis().SetTitle("Efficiency")
                if name in self.names:
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
                for label in labels:
                    label.Draw("same")
                c.SaveAs(create_file_dir(output["plots"]["%s_%s" % (name, trigger)].path))
                del c
            if "stats" in output:
                d["efficiencies"][trigger] = float(counts[trigger]["pass"]) / counts[trigger]["total"]

        d["acc_gain"] = {}
        for trigger_ratio in self.trigger_ratios:
            trigger1 = trigger_ratio.split("/")[0]
            trigger2 = trigger_ratio.split("/")[1]
            for name in self.names + self.names2D:
                c = Canvas()
                if name in self.names:
                    histos["%s_total" % name].GetYaxis().SetTitle("Efficiency gain")
                if name in self.names:
                    ratio = copy(histos["%s_total" % name].Clone())
                    for ibin in range(1, histos["%s_%s_pass" % (name, trigger1)].GetNbinsX() + 1):
                        content1 = float(histos["%s_%s_pass" % (name, trigger1)].GetBinContent(ibin))
                        content2 = float(histos["%s_%s_pass" % (name, trigger2)].GetBinContent(ibin))
                    
                        content = ((content1 / content2) if content2 != 0 else 0)
                        ratio.SetBinContent(ibin, content)
                        error = content * math.sqrt(abs(1 / content1 if content1 != 0 else 0) + abs(1 / content2 if content2 != 0 else 0))
                        ratio.SetBinError(ibin, error)

                    # ratio.GetXaxis().SetTitle(histos["%s_total" % name].GetXaxis().GetTitle())
                    # ratio.GetYaxis().SetTitle(histos["%s_total" % name].GetYaxis().GetTitle())
                    # ratio.SetMaximum(1.5)
                    # ratio.SetMinimum(0.5)
                    ratio.Draw("E1")                
                else:
                    ratio = ROOT.TEfficiency(histos["%s_%s_pass" % (name, trigger1)],
                        histos["%s_%s_pass" % (name, trigger2)])
                    ratio.Draw("colz")
                for label in labels:
                    label.Draw("same")
                c.SaveAs(create_file_dir(output["plots"]["%s_%s" % (name, trigger_ratio)].path))
                del c
            if "stats" in output:
                d["acc_gain"][trigger_ratio] = float(counts[trigger1]["pass"]) / counts[trigger2]["pass"]
        if "stats" in output:
            with open(create_file_dir(output["stats"].path), "w+") as f:
                json.dump(d, f, indent=4)


class L1TurnOnCurvePlotter(DatasetTaskWithCategory, TurnOnCurve, L1Task):
    def requires(self):
        return L1TurnOnCurveProducer.vreq(self)

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
        self.upper_left_text = "Work in progress"
        self.upper_right_text = "Run 3 Simulation"
        self.inner_text = [self.dataset.process.label, self.category.label]
        super(L1TurnOnCurvePlotter, self).plot_curves()
    
