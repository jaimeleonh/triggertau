# coding: utf-8

import law
import luigi
import json
import os

from analysis_tools.utils import (
    import_root, create_file_dir, join_root_selection
)
import itertools

from cmt.base_tasks.base import ( 
    DatasetTaskWithCategory, DatasetWrapperTask, HTCondorWorkflow, InputData,
    ConfigTaskWithCategory
)

from cmt.base_tasks.preprocessing import DatasetCategoryWrapperTask


class AddTrigger(DatasetTaskWithCategory, law.LocalWorkflow, HTCondorWorkflow):

    xx_range = (32, 40)
    yy_range = (20, 33)
    zz_range = (20, 160)

    additional_branches = [
        "nTau", "Tau_pt", "Tau_eta", "Tau_phi", "Tau_mass",
        "nJet", "Jet_pt", "Jet_eta", "Jet_phi", "Jet_mass", "Jet_jetId", "Jet_puId",
        #"nGenVisTau", "GenVisTau_pt", "GenVisTau_eta", "GenVisTau_phi", "GenVisTau_mass",
        #"nTrigTau",
        #"TrigTau_pt", "TrigTau_phi", "TrigTau_eta"
        "nL1Obj", "L1Obj_pt", "L1Obj_eta", "L1Obj_phi", "L1Obj_type", "L1Obj_iso",
    ]   

    # regions not supported
    region_name = None
    tree_name = "Events"

    default_store = "$CMT_STORE_EOS_CATEGORIZATION"
    default_wlcg_fs = "wlcg_fs_categorization"

    def __init__(self, *args, **kwargs):
        super(AddTrigger, self).__init__(*args, **kwargs)

    def create_branch_map(self):
        return len(self.dataset.get_files())

    def workflow_requires(self):
        return {"data": InputData.vreq(self)}

    def requires(self):
        return {"data": InputData.vreq(self, file_index=self.branch)}

    def output(self):
        return self.local_target("{}".format(self.input()["data"].path.split("/")[-1]))
        # return self.local_target("{}".format(self.input()["data"].split("/")[-1]))
    
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

    @classmethod
    def add_dataframe_definitions(self, df, category=None):
        # leading and subleading L1 Taus
        # df = df.Define("lead_sublead_goodl1tau_pt",
            # "lead_sublead("
                # "L1Obj_pt[L1Obj_type == 1 && L1Obj_iso == 1 && abs(L1Obj_eta) <= 2.1], "
                # "L1Obj_eta[L1Obj_type == 1 && L1Obj_iso == 1 && abs(L1Obj_eta) <= 2.1], "
                # "L1Obj_phi[L1Obj_type == 1 && L1Obj_iso == 1 && abs(L1Obj_eta) <= 2.1], "
                # "L1Obj_pt[L1Obj_type == 1 && L1Obj_iso == 1 && abs(L1Obj_eta) <= 2.1]"  # dum
            # ")[0]"
        # ).Define("lead_sublead_goodl1tau_eta", 
            # "lead_sublead("
                # "L1Obj_pt[L1Obj_type == 1 && L1Obj_iso == 1 && abs(L1Obj_eta) <= 2.1], "
                # "L1Obj_eta[L1Obj_type == 1 && L1Obj_iso == 1 && abs(L1Obj_eta) <= 2.1], "
                # "L1Obj_phi[L1Obj_type == 1 && L1Obj_iso == 1 && abs(L1Obj_eta) <= 2.1], "
                # "L1Obj_pt[L1Obj_type == 1 && L1Obj_iso == 1 && abs(L1Obj_eta) <= 2.1]"  # dum
            # ")[1]"
        # ).Define("lead_sublead_goodl1tau_phi", 
            # "lead_sublead("
                # "L1Obj_pt[L1Obj_type == 1 && L1Obj_iso == 1 && abs(L1Obj_eta) <= 2.1], "
                # "L1Obj_eta[L1Obj_type == 1 && L1Obj_iso == 1 && abs(L1Obj_eta) <= 2.1], "
                # "L1Obj_phi[L1Obj_type == 1 && L1Obj_iso == 1 && abs(L1Obj_eta) <= 2.1], "
                # "L1Obj_pt[L1Obj_type == 1 && L1Obj_iso == 1 && abs(L1Obj_eta) <= 2.1]"  # dum
            # ")[2]")
        
        df = df.Define(
            "lead_sublead_goodl1tau_pt",
                "lead_sublead(L1Obj_pt[L1Obj_type == 1 && L1Obj_iso == 1 && abs(L1Obj_eta) <= 2.1], 3)"
            ).Define("lead_sublead_goodl1tau_eta",
                "lead_sublead(L1Obj_eta[L1Obj_type == 1 && L1Obj_iso == 1 && abs(L1Obj_eta) <= 2.1], 3)"
            ).Define("lead_sublead_goodl1tau_phi",
                "lead_sublead(L1Obj_phi[L1Obj_type == 1 && L1Obj_iso == 1 && abs(L1Obj_eta) <= 2.1], 3)"
            )

        # leading and subleading Reco Taus
        # df = df.Define("lead_sublead_goodtau_pt",
            # "lead_sublead("
                # "Tau_pt[abs(Tau_eta) <= 2.1], "
                # "Tau_eta[abs(Tau_eta) <= 2.1], "
                # "Tau_phi[abs(Tau_eta) <= 2.1], "
                # "Tau_mass[abs(Tau_eta) <= 2.1]"
            # ")[0]").Define("lead_sublead_goodtau_eta", 
            # "lead_sublead("
                # "Tau_pt[abs(Tau_eta) <= 2.1], "
                # "Tau_eta[abs(Tau_eta) <= 2.1], "
                # "Tau_phi[abs(Tau_eta) <= 2.1], "
                # "Tau_mass[abs(Tau_eta) <= 2.1]"
            # ")[1]").Define("lead_sublead_goodtau_phi", 
            # "lead_sublead("
                # "Tau_pt[abs(Tau_eta) <= 2.1], "
                # "Tau_eta[abs(Tau_eta) <= 2.1], "
                # "Tau_phi[abs(Tau_eta) <= 2.1], "
                # "Tau_mass[abs(Tau_eta) <= 2.1]"
            # ")[2]")

        df = df.Define("lead_sublead_goodtau_pt",
                "lead_sublead(Tau_pt[abs(Tau_eta) <= 2.1], 3)"
            ).Define("lead_sublead_goodtau_eta",
                "lead_sublead(Tau_eta[abs(Tau_eta) <= 2.1], 3)"
            ).Define("lead_sublead_goodtau_phi",
                "lead_sublead(Tau_phi[abs(Tau_eta) <= 2.1], 3)"
            )
            
        
        # leading and subleading L1 jets
        # restriction = (
            # "L1Obj_type == 0 "
            # "&& maskDeltaR("
                # "L1Obj_eta, "
                # "L1Obj_phi, "
                # "lead_sublead_goodl1tau_eta, "
                # "lead_sublead_goodl1tau_phi, "
                # "0.5)"
        # )

        for tau_pt in range(*self.yy_range):
            restriction = (
                "L1Obj_type == 0 "
                "&& maskDeltaR("
                    "L1Obj_eta, "
                    "L1Obj_phi, "
                    "L1Obj_eta[L1Obj_pt >= {0} && L1Obj_type == 1 && L1Obj_iso == 1 && abs(L1Obj_eta) <= 2.1], "
                    "L1Obj_phi[L1Obj_pt >= {0} && L1Obj_type == 1 && L1Obj_iso == 1 && abs(L1Obj_eta) <= 2.1], "
                    "0.5)".format(tau_pt)
            )
            df = df.Define("lead_sublead_goodl1jet_pt_tau_pt_%s" % tau_pt,
                "lead_sublead(L1Obj_pt[{0}], 3)".format(restriction))
            df = df.Define("lead_sublead_goodl1jet_eta_tau_pt_%s" % tau_pt,
                "lead_sublead(L1Obj_eta[{0}], 3)".format(restriction))
            df = df.Define("lead_sublead_goodl1jet_phi_tau_pt_%s" % tau_pt,
                "lead_sublead(L1Obj_phi[{0}], 3)".format(restriction))

            # leading and subleading offline jets
            # restriction = (
                # "abs(Jet_eta) <= 4.7 && Jet_jetId >= 2 "
                # "&& ((Jet_puId >= 4 && Jet_pt <= 50) || (Jet_pt > 50))"
                # "&& maskDeltaR("
                    # "Jet_eta, "
                    # "Jet_phi, "
                    # "lead_sublead_goodtau_eta, "
                    # "lead_sublead_goodtau_phi, "
                    # "0.5)")
            restriction = (
                "abs(Jet_eta) <= 4.7 && Jet_jetId >= 2 "
                "&& ((Jet_puId >= 4 && Jet_pt <= 50) || (Jet_pt > 50))"
                "&& maskDeltaR("
                    "Jet_eta, "
                    "Jet_phi, "
                    "Tau_eta[Tau_pt >= {0} && abs(Tau_eta) <= 2.1], "
                    "Tau_eta[Tau_pt >= {0} && abs(Tau_phi) <= 2.1], "
                    "0.5)".format(tau_pt + category.get_aux("add_to_subleading_pt"))
            )
            df = df.Define("lead_sublead_goodjet_pt_tau_pt_%s" % tau_pt,
                "lead_sublead(Jet_pt[{0}], 3)".format(restriction))
            df = df.Define("lead_sublead_goodjet_eta_tau_pt_%s" % tau_pt,
                "lead_sublead(Jet_eta[{0}], 3)".format(restriction))
            df = df.Define("lead_sublead_goodjet_phi_tau_pt_%s" % tau_pt,
                "lead_sublead(Jet_phi[{0}], 3)".format(restriction))
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

    def get_new_dataframe(self, input_file, output_file):
        ROOT = import_root()
        ROOT = self.add_to_root(ROOT)
        # ROOT.ROOT.EnableImplicitMT()
        
        df = ROOT.RDataFrame(self.tree_name, input_file)

        # add the needed definitions
        df = self.add_dataframe_definitions(df)

        if self.category.selection:
            df = df.Filter(self.category.selection)

        # filter de number of offline jets in the event
        df = self.filter_jets(df)

        branch_names = [] 
        for xx in range(*self.xx_range):
            name = "DoubleIsoTau{}er2p1".format(xx)
            df = df.Define(name, 
                "L1Obj_pt["
                    "L1Obj_type == 1 "
                    "&& abs(L1Obj_eta) <= 2.1 "
                    "&& L1Obj_iso == 1 "
                    "&& L1Obj_pt >= {}"
                "].size() >= 2".format(xx)
            )
            branch_names.append(name)

        for yy, zz in itertools.product(range(*self.yy_range), range(*self.zz_range)):
            # Using L1 Taus and Jets
            name = "DoubleIsoTau{}er2p1Jet{}dR0p5".format(yy, zz)
            df = df.Define(name,
                "lead_sublead_goodl1tau_pt[0] >= {0} "
                "&& lead_sublead_goodl1tau_pt[1] >= {0}"
                "&& (L1Obj_pt["
                    "L1Obj_type == 0 "
                    "&& L1Obj_pt >= {1} "
                    "&& maskDeltaR("
                        "L1Obj_eta, "
                        "L1Obj_phi, "
                        "lead_sublead_goodl1tau_eta[lead_sublead_goodl1tau_pt >= {1}], "
                        "lead_sublead_goodl1tau_phi[lead_sublead_goodl1tau_pt >= {1}], "
                        "0.5)"
                    "].size() >= 1)".format(yy, zz))
            branch_names.append(name)

        branch_list = ROOT.vector('string')()
        for branch_name in self.additional_branches + branch_names:
            branch_list.push_back(branch_name)

        df.Snapshot(self.tree_name, create_file_dir(output_file), branch_list)

    @law.decorator.notify
    @law.decorator.localize(input=False)
    def run(self):
        inp = self.input()["data"].path
        outp = self.output().path
        self.get_new_dataframe(inp, outp)


class Skim(AddTrigger):
    def get_new_dataframe(self, input_file, output_file):
        ROOT = import_root()
        ROOT = self.add_to_root(ROOT)
        # ROOT.ROOT.EnableImplicitMT()
        print "RUNNING SKIMMING"
        df = ROOT.RDataFrame(self.tree_name, input_file)

        # add the needed definitions
        df = self.add_dataframe_definitions(df, self.category)
        print "DF definition"
        # if self.category.selection:
        #     df = df.Filter(self.category.selection)

        # filter the number of offline jets in the event
        category = self.category
        df = self.filter_jets(df)
        while category.get_aux("parent_category", None):
            category = self.config.categories.get(category.get_aux("parent_category", None))
            print category.name
            df = self.filter_jets(df, category=category)

        branch_names = [
            "leading_l1tau_pt",
            "leading_l1tau_eta",
            "leading_l1tau_phi",
            "subleading_l1tau_pt",
            "subleading_l1tau_eta",
            "subleading_l1tau_phi",
            "subsubleading_l1tau_pt",
            "subsubleading_l1tau_eta",
            "subsubleading_l1tau_phi",
            "leading_tau_pt",
            "leading_tau_eta",
            "leading_tau_phi",
            "subleading_tau_pt",
            "subleading_tau_eta",
            "subleading_tau_phi",
            "subsubleading_tau_pt",
            "subsubleading_tau_eta",
            "subsubleading_tau_phi",
        ]
        
        for tau_pt in range(*self.yy_range):
            branch_names += [
                "leading_l1jet_pt_tau_pt_%s" % tau_pt,
                "leading_l1jet_eta_tau_pt_%s" % tau_pt,
                "leading_l1jet_phi_tau_pt_%s" % tau_pt,
                "subleading_l1jet_pt_tau_pt_%s" % tau_pt,
                "subleading_l1jet_eta_tau_pt_%s" % tau_pt,
                "subleading_l1jet_phi_tau_pt_%s" % tau_pt,
                "leading_jet_pt_tau_pt_%s" % tau_pt,
                "leading_jet_eta_tau_pt_%s" % tau_pt,
                "leading_jet_phi_tau_pt_%s" % tau_pt,
                "subleading_jet_pt_tau_pt_%s" % tau_pt,
                "subleading_jet_eta_tau_pt_%s" % tau_pt,
                "subleading_jet_phi_tau_pt_%s" % tau_pt,
            ]

        df = df.Define("leading_l1tau_pt", "lead_sublead_goodl1tau_pt[0]")
        df = df.Define("leading_l1tau_eta", "lead_sublead_goodl1tau_eta[0]")
        df = df.Define("leading_l1tau_phi", "lead_sublead_goodl1tau_phi[0]")
        df = df.Define("subleading_l1tau_pt", "lead_sublead_goodl1tau_pt[1]")
        df = df.Define("subleading_l1tau_eta", "lead_sublead_goodl1tau_eta[1]")
        df = df.Define("subleading_l1tau_phi", "lead_sublead_goodl1tau_phi[1]")
        df = df.Define("subsubleading_l1tau_pt", "lead_sublead_goodl1tau_pt[2]")
        df = df.Define("subsubleading_l1tau_eta", "lead_sublead_goodl1tau_eta[2]")
        df = df.Define("subsubleading_l1tau_phi", "lead_sublead_goodl1tau_phi[2]")

        df = df.Define("leading_tau_pt", "lead_sublead_goodtau_pt[0]")
        df = df.Define("leading_tau_eta", "lead_sublead_goodtau_eta[0]")
        df = df.Define("leading_tau_phi", "lead_sublead_goodtau_phi[0]")
        df = df.Define("subleading_tau_pt", "lead_sublead_goodtau_pt[1]")
        df = df.Define("subleading_tau_eta", "lead_sublead_goodtau_eta[1]")
        df = df.Define("subleading_tau_phi", "lead_sublead_goodtau_phi[1]")
        df = df.Define("subsubleading_tau_pt", "lead_sublead_goodtau_pt[2]")
        df = df.Define("subsubleading_tau_eta", "lead_sublead_goodtau_eta[2]")
        df = df.Define("subsubleading_tau_phi", "lead_sublead_goodtau_phi[2]")

        for tau_pt in range(*self.yy_range):
            df = df.Define("leading_l1jet_pt_tau_pt_%s" % tau_pt,
                "lead_sublead_goodl1jet_pt_tau_pt_%s[0]" % tau_pt)
            df = df.Define("leading_l1jet_eta_tau_pt_%s" % tau_pt,
                "lead_sublead_goodl1jet_eta_tau_pt_%s[0]" % tau_pt)
            df = df.Define("leading_l1jet_phi_tau_pt_%s" % tau_pt,
                "lead_sublead_goodl1jet_phi_tau_pt_%s[0]" % tau_pt)
            df = df.Define("subleading_l1jet_pt_tau_pt_%s" % tau_pt,
                "lead_sublead_goodl1jet_pt_tau_pt_%s[1]" % tau_pt)
            df = df.Define("subleading_l1jet_eta_tau_pt_%s" % tau_pt,
                "lead_sublead_goodl1jet_eta_tau_pt_%s[1]" % tau_pt)
            df = df.Define("subleading_l1jet_phi_tau_pt_%s" % tau_pt,
                "lead_sublead_goodl1jet_phi_tau_pt_%s[1]" % tau_pt)

            df = df.Define("leading_jet_pt_tau_pt_%s" % tau_pt,
                "lead_sublead_goodjet_pt_tau_pt_%s[0]" % tau_pt)
            df = df.Define("leading_jet_eta_tau_pt_%s" % tau_pt,
                "lead_sublead_goodjet_eta_tau_pt_%s[0]" % tau_pt)
            df = df.Define("leading_jet_phi_tau_pt_%s" % tau_pt,
                "lead_sublead_goodjet_phi_tau_pt_%s[0]" % tau_pt)
            df = df.Define("subleading_jet_pt_tau_pt_%s" % tau_pt,
                "lead_sublead_goodjet_pt_tau_pt_%s[1]" % tau_pt)
            df = df.Define("subleading_jet_eta_tau_pt_%s" % tau_pt,
                "lead_sublead_goodjet_eta_tau_pt_%s[1]" % tau_pt)
            df = df.Define("subleading_jet_phi_tau_pt_%s" % tau_pt,
                "lead_sublead_goodjet_phi_tau_pt_%s[1]" % tau_pt)

        df = df.Filter("subleading_l1tau_pt >= 20 && subleading_tau_pt >= 20")
        print "Branches defined"
        branch_list = ROOT.vector('string')()
        for branch_name in self.additional_branches + branch_names:
            branch_list.push_back(branch_name)
        print "Saving..."
        df.Snapshot(self.tree_name, create_file_dir(output_file), branch_list)


class AddDiJetTrigger(AddTrigger):

    xx_range = (32, 40)
    yy_range = (20, 33)
    zz_range = (20, 100)

    def get_new_dataframe(self, input_file, output_file):
        ROOT = import_root()
        ROOT = self.add_to_root(ROOT)
        # ROOT.ROOT.EnableImplicitMT()
        
        df = ROOT.RDataFrame(self.tree_name, input_file)

        # add the needed definitions
        df = self.add_dataframe_definitions(df)
        
        if self.category.selection:
            df = df.Filter(self.category.selection)

        # filter de number of offline jets in the event
        df = self.filter_jets(df)

        branch_names = [] 
        for xx in range(*self.xx_range):
            name = "DoubleIsoTau{}er2p1".format(xx)
            df = df.Define(name, 
                "L1Obj_pt["
                    "L1Obj_type == 1 "
                    "&& abs(L1Obj_eta) <= 2.1 "
                    "&& L1Obj_iso == 1 "
                    "&& L1Obj_pt >= {}"
                "].size() >= 2".format(xx)
            )
            branch_names.append(name)

        #for yy, zz1 in itertools.product(range(*self.yy_range), range(*self.zz_range)):
        #    for zz2 in range(self.zz_range[0], zz1 + 1):
        for yy, zz in itertools.product(range(*self.yy_range), range(*self.zz_range)):
            # Using L1 Taus and Jets
            name = "DoubleIsoTau{0}er2p1Jet{1}dR0p5Jet{1}dR0p5".format(yy, zz)
            df = df.Define(name,
                "lead_sublead_goodl1tau_pt[0] >= {0} "
                "&& lead_sublead_goodl1tau_pt[1] >= {0}"
                "&& lead_sublead_goodl1jet_pt[0] >= {1}"
                "&& lead_sublead_goodl1jet_pt[1] >= {1}".format(yy, zz))
            branch_names.append(name)

        branch_list = ROOT.vector('string')()
        for branch_name in self.additional_branches + branch_names:
            branch_list.push_back(branch_name)

        df.Snapshot(self.tree_name, create_file_dir(output_file), branch_list)

    @law.decorator.notify
    @law.decorator.localize(input=False)
    def run(self):
        inp = self.input()["data"].path
        outp = self.output().path
        self.get_new_dataframe(inp, outp)


class AddOffline(AddTrigger):

    additional_branches = [
        "nTau","Tau_pt", "Tau_eta", "Tau_phi", "Tau_mass",
        "nJet","Jet_pt", "Jet_eta", "Jet_phi", "Jet_mass",
        "nGenVisTau", "GenVisTau_pt", "GenVisTau_eta", "GenVisTau_phi", "GenVisTau_mass",
    ]

    def get_new_dataframe(self, input_file, output_file):
        ROOT = import_root()
        ROOT = self.add_to_root(ROOT)

        df = ROOT.RDataFrame(self.tree_name, input_file)

        # add the needed definitions
        df = self.add_dataframe_definitions(df)

        if self.category.selection:
            df = df.Filter(self.category.selection)
        
        # filter de number of offline jets in the event
        df = self.filter_jets(df)

        branch_names = []
        for xx in range(*self.xx_range):
            name = "DoubleIsoTau{}er2p1".format(xx)
            df = df.Define(name, 
                "lead_sublead_goodtau_pt[0] >= ({0} + {1}) "
                "&& lead_sublead_goodtau_pt[1] >= ({0} + {2})".format(
                    xx, 
                    self.category.get_aux("add_to_leading_pt"),
                    self.category.get_aux("add_to_subleading_pt")
                )
            )
            branch_names.append(name)

        for yy, zz in itertools.product(range(*self.yy_range), range(*self.zz_range)):
            # Using Reco Taus and Jets
            name = "DoubleIsoTau{}er2p1Jet{}dR0p5".format(yy, zz)
            df = df.Define(name,
                # ask that the two taus have pt greater than yy (+ sth depending on the dataset)
                "lead_sublead_goodtau_pt[0] >= ({0} + {1}) "
                "&& lead_sublead_goodtau_pt[1] >= ({0} + {2})"
                # regarding the jets, we require the offline reqs from the analysis +
                # they are not matched to the leading and subleading taus we selected before
                # first, ask for at least 1 jet with pt > zz (+ sth depending on the dataset)
                "&& (Jet_pt["
                    "(Jet_pt >= ({3} + {4})) && abs(Jet_eta) <= 4.7 && Jet_jetId >= 2 "
                    "&& ((Jet_puId >= 4 && Jet_pt <= 50) || (Jet_pt > 50))"
                    "&& maskDeltaR("
                        "Jet_eta, "
                        "Jet_phi, "
                        "lead_sublead_goodtau_eta, "
                        "lead_sublead_goodtau_phi, "
                        "0.5)"
                    "].size() >= 1)".format(
                        yy, 
                        self.category.get_aux("add_to_leading_pt"),
                        self.category.get_aux("add_to_subleading_pt"),
                        zz,
                        self.category.get_aux("add_to_trigger_jets", 10),
                    )
            )
            branch_names.append(name)

        branch_list = ROOT.vector('string')()
        for branch_name in self.additional_branches + branch_names:
            branch_list.push_back(branch_name)

        df.Snapshot(self.tree_name, create_file_dir(output_file), branch_list)


class AddDiJetOffline(AddOffline):

    xx_range = (32, 40)
    yy_range = (20, 33)
    zz_range = (20, 100)

    def get_new_dataframe(self, input_file, output_file):
        ROOT = import_root()
        ROOT = self.add_to_root(ROOT)

        df = ROOT.RDataFrame(self.tree_name, input_file)

        # add the needed definitions
        df = self.add_dataframe_definitions(df)

        if self.category.selection:
            df = df.Filter(self.category.selection)
        
        # filter de number of offline jets in the event
        df = self.filter_jets(df)

        branch_names = []
        for xx in range(*self.xx_range):
            name = "DoubleIsoTau{}er2p1".format(xx)
            df = df.Define(name, 
                "lead_sublead_goodtau_pt[0] >= ({0} + {1}) "
                "&& lead_sublead_goodtau_pt[1] >= ({0} + {2})".format(
                    xx, 
                    self.category.get_aux("add_to_leading_pt"),
                    self.category.get_aux("add_to_subleading_pt")
                )
            )
            branch_names.append(name)

        for yy, zz in itertools.product(range(*self.yy_range), range(*self.zz_range)):
            # Using Reco Taus and Jets
            name = "DoubleIsoTau{0}er2p1Jet{1}dR0p5Jet{1}dR0p5".format(yy, zz)
            df = df.Define(name,
                # ask that the two taus have pt greater than yy (+ sth depending on the dataset)
                "lead_sublead_goodtau_pt[0] >= ({0} + {1}) "
                "&& lead_sublead_goodtau_pt[1] >= ({0} + {2})"
                # regarding the jets, we require the offline reqs from the analysis +
                # they are not matched to the leading and subleading taus we selected before
                # first, ask for at least 1 jet with pt > zz (+ sth depending on the dataset)
                "&& lead_sublead_goodjet_pt[0] >= ({3} + {4})"
                "&& lead_sublead_goodjet_pt[1] >= ({3} + {4})".format(
                    yy, 
                    self.category.get_aux("add_to_leading_pt"),
                    self.category.get_aux("add_to_subleading_pt"),
                    zz,
                    self.category.get_aux("add_to_trigger_jets", 10),
                )
            )
            branch_names.append(name)

        branch_list = ROOT.vector('string')()
        for branch_name in self.additional_branches + branch_names:
            branch_list.push_back(branch_name)

        df.Snapshot(self.tree_name, create_file_dir(output_file), branch_list)


class Acceptance(AddTrigger):
    xx_range = (32, 40)
    yy_range = (20, 33)
    zz_range = (20, 80)
    dzz_range = (20, 80)

    # xx_range = (32, 34)
    # yy_range = (20, 22)
    # zz_range = (20, 22)
    # dzz_range = (20, 22)

    def workflow_requires(self):
        return {"data": Skim.vreq(self, _prefer_cli=["workflow"])}

    def requires(self):
        return {"data": Skim.vreq(self, _prefer_cli=["workflow"], branch=self.branch)}

    def output(self):
        return {
            "root": self.local_target("{}".format(self.input()["data"].path.split("/")[-1])),
            "stats": self.local_target("{}".format(self.input()["data"].path.split("/")[-1]
                ).replace(".root", ".json")),
        }

    def get_new_dataframe(self, input_file, output_file):
        ROOT = import_root()
        #ROOT = self.add_to_root(ROOT)

        df = ROOT.RDataFrame(self.tree_name, input_file)

        # add the needed definitions
        #df = self.add_dataframe_definitions(df)

        ditau_acc = df.Filter(
            "leading_l1tau_pt >= {0} "
            "&& subleading_l1tau_pt >= {1}"
            "&& leading_tau_pt >= ({0} + {2}) "
            "&& subleading_tau_pt >= ({1} + {3})".format(
                32,
                32,
                self.category.get_aux("add_to_leading_pt"),
                self.category.get_aux("add_to_subleading_pt")
            )
        ).Count()

        #ROOT.gROOT.ProcessLine(
        #    ".L %s " % os.path.expandvars("$CMT_BASE/cmt/tasks/TotalTrigger.C++"))
        ROOT.gSystem.Load("%s" % os.path.expandvars("$CMT_BASE/cmt/tasks/./TotalTrigger_C.so"))

        run = ROOT.TotalTrigger(input_file, self.output()["root"].path, self.tree_name,
            self.xx_range[0], self.xx_range[1], self.yy_range[0], self.yy_range[1],
            self.zz_range[0], self.zz_range[1], self.dzz_range[0], self.dzz_range[1],
            self.category.get_aux("add_to_leading_pt"),
            self.category.get_aux("add_to_subleading_pt"),
            self.category.get_aux("add_to_trigger_jets", 10)
        )
        run.TotalLoop()

        f = ROOT.TFile.Open(input_file)
        tree = f.Get(self.tree_name)
        stats = {
            "nevents": tree.GetEntries(),
            "den": ditau_acc.GetValue()
        }
        f.Close()
        stats_path = self.output()["stats"].path
        with open(create_file_dir(stats_path), "w") as json_f:
            json.dump(stats, json_f, indent=4)

    @law.decorator.notify
    @law.decorator.localize(input=False)
    def run(self):
        inp = self.input()["data"].path
        self.get_new_dataframe(inp, None)

class AcceptanceWrapper(DatasetCategoryWrapperTask):

    def atomic_requires(self, dataset, category):
        return Acceptance.vreq(self, dataset_name=dataset.name, category_name=category.name)


class AsymmAcceptance(Acceptance):
    xx_range = (32, 40)
    yy_range = (20, 33)
    zz_range = (20, 160)

    def get_new_dataframe(self, input_file, output_file):
        ROOT = import_root()

        df = ROOT.RDataFrame(self.tree_name, input_file)

        ditau_acc = df.Filter(
            "leading_l1tau_pt >= {0} "
            "&& subleading_l1tau_pt >= {1}"
            "&& leading_tau_pt >= ({0} + {2}) "
            "&& subleading_tau_pt >= ({1} + {3})".format(
                32,
                32,
                self.category.get_aux("add_to_leading_pt"),
                self.category.get_aux("add_to_subleading_pt")
            )
        ).Count()

        ROOT.gSystem.Load("%s" % os.path.expandvars("$CMT_BASE/cmt/tasks/./TotalTrigger_C.so"))

        run = ROOT.TotalTrigger(input_file, self.output()["root"].path, self.tree_name,
            self.xx_range[0], self.xx_range[1], self.yy_range[0], self.yy_range[1],
            self.zz_range[0], self.zz_range[1], -1, -1,
            self.category.get_aux("add_to_leading_pt"),
            self.category.get_aux("add_to_subleading_pt"),
            self.category.get_aux("add_to_trigger_jets", 10)
        )
        run.AsymmLoop()

        f = ROOT.TFile.Open(input_file)
        tree = f.Get(self.tree_name)
        stats = {
            "nevents": tree.GetEntries(),
            "den": ditau_acc.GetValue()
        }
        f.Close()
        stats_path = self.output()["stats"].path
        with open(create_file_dir(stats_path), "w") as json_f:
            json.dump(stats, json_f, indent=4)


class AsymmAcceptanceWrapper(DatasetCategoryWrapperTask):

    def atomic_requires(self, dataset, category):
        return AsymmAcceptance.vreq(self, dataset_name=dataset.name, category_name=category.name)


class AsymmManfredAcceptance(Acceptance):
    xx_range = (32, 40)
    yy_range = (20, 33)
    zz_range = (20, 160)

    def get_new_dataframe(self, input_file, output_file):
        ROOT = import_root()

        df = ROOT.RDataFrame(self.tree_name, input_file)

        ditau_acc = df.Filter(
            "leading_l1tau_pt >= {0} "
            "&& subleading_l1tau_pt >= {1}"
            "&& leading_tau_pt >= ({0} + {2}) "
            "&& subleading_tau_pt >= ({1} + {3})"
            "&& leading_tau_pt >= {4}"
            "&& subleading_tau_pt >= {5}".format(
                32,
                32,
                self.category.get_aux("add_to_leading_pt"),
                self.category.get_aux("add_to_subleading_pt"),
                self.category.get_aux("min_leading_tau_pt"),
                self.category.get_aux("min_subleading_tau_pt")
            )
        ).Count()

        ROOT.gSystem.Load("%s" % os.path.expandvars("$CMT_BASE/cmt/tasks/./TotalTrigger_C.so"))

        run = ROOT.TotalTrigger(input_file, self.output()["root"].path, self.tree_name,
            self.xx_range[0], self.xx_range[1], self.yy_range[0], self.yy_range[1],
            self.zz_range[0], self.zz_range[1], -1, -1,
            self.category.get_aux("add_to_leading_pt"),
            self.category.get_aux("add_to_subleading_pt"),
            self.category.get_aux("add_to_trigger_jets", 10),
            self.category.get_aux("min_leading_tau_pt"),
            self.category.get_aux("min_subleading_tau_pt")
        )
        run.AsymmManfredLoop()

        f = ROOT.TFile.Open(input_file)
        tree = f.Get(self.tree_name)
        stats = {
            "nevents": tree.GetEntries(),
            "den": ditau_acc.GetValue()
        }
        f.Close()
        stats_path = self.output()["stats"].path
        with open(create_file_dir(stats_path), "w") as json_f:
            json.dump(stats, json_f, indent=4)


class AsymmManfredAcceptanceWrapper(DatasetCategoryWrapperTask):

    def atomic_requires(self, dataset, category):
        return AsymmManfredAcceptance.vreq(self, dataset_name=dataset.name, category_name=category.name)


class AsymmTriTauAcceptance(Acceptance):
    xx_range = (32, 40)
    yy_range = (20, 33)
    zz_range = (20, 160)

    def get_new_dataframe(self, input_file, output_file):
        ROOT = import_root()

        df = ROOT.RDataFrame(self.tree_name, input_file)

        ditau_acc = df.Filter(
            "leading_l1tau_pt >= {0} "
            "&& subleading_l1tau_pt >= {1}"
            "&& leading_tau_pt >= ({0} + {2}) "
            "&& subleading_tau_pt >= ({1} + {3})".format(
                32,
                32,
                self.category.get_aux("add_to_leading_pt"),
                self.category.get_aux("add_to_subleading_pt")
            )
        ).Count()

        ROOT.gSystem.Load("%s" % os.path.expandvars("$CMT_BASE/cmt/tasks/./TotalTrigger_C.so"))

        run = ROOT.TotalTrigger(input_file, self.output()["root"].path, self.tree_name,
            self.xx_range[0], self.xx_range[1], self.yy_range[0], self.yy_range[1],
            self.zz_range[0], self.zz_range[1], -1, -1,
            self.category.get_aux("add_to_leading_pt"),
            self.category.get_aux("add_to_subleading_pt"),
            self.category.get_aux("add_to_trigger_jets", 10)
        )
        run.AsymmTriTauLoop()

        f = ROOT.TFile.Open(input_file)
        tree = f.Get(self.tree_name)
        stats = {
            "nevents": tree.GetEntries(),
            "den": ditau_acc.GetValue()
        }
        f.Close()
        stats_path = self.output()["stats"].path
        with open(create_file_dir(stats_path), "w") as json_f:
            json.dump(stats, json_f, indent=4)


class AsymmTriTauAcceptanceWrapper(DatasetCategoryWrapperTask):

    def atomic_requires(self, dataset, category):
        return AsymmTriTauAcceptance.vreq(self,
            dataset_name=dataset.name, category_name=category.name)


class AsymmVBFAcceptance(Acceptance):
    xx_range = (32, 40)
    yy_range = (20, 33)
    zz_range = (20, 160)

    def get_new_dataframe(self, input_file, output_file):
        ROOT = import_root()

        df = ROOT.RDataFrame(self.tree_name, input_file)
        
        ROOT.gInterpreter.Declare("""
            #include <math.h>
            using Vfloat = const ROOT::RVec<float>&;
            float inv_mass(float pt1, float eta1, float phi1, float pt2, float eta2, float phi2){
                return sqrt(2 * pt1 * pt2 * (cosh(eta1 - eta2) - cos(phi1 - phi2)));
            }
            """
        )

        ditau_acc = df.Filter(
            "(leading_l1tau_pt >= {0} "
                "&& subleading_l1tau_pt >= {1}"
                "&& leading_tau_pt >= ({0} + {2}) "
                "&& subleading_tau_pt >= ({1} + {3})) || "
            "(leading_l1jet_pt >= 90"
                "&& subleading_l1jet_pt >= 30"
                "&& leading_jet_pt >= 90 + {4}"
                "&& subleading_jet_pt >= 30 + {4}"
                "&& inv_mass(leading_l1jet_pt, leading_l1jet_eta, leading_l1jet_phi,"
                    "subleading_l1jet_pt, subleading_l1jet_eta, subleading_l1jet_phi) >= 620"
                "&& inv_mass(leading_jet_pt, leading_jet_eta, leading_jet_phi,"
                    "subleading_jet_pt, subleading_jet_eta, subleading_jet_phi) >= 700)"
                "".format(
                    32,
                    32,
                    self.category.get_aux("add_to_leading_pt"),
                    self.category.get_aux("add_to_subleading_pt"),
                    self.category.get_aux("add_to_trigger_jets", 10)
            )
        ).Count()

        ROOT.gSystem.Load("%s" % os.path.expandvars("$CMT_BASE/cmt/tasks/./TotalTrigger_C.so"))

        run = ROOT.TotalTrigger(input_file, self.output()["root"].path, self.tree_name,
            self.xx_range[0], self.xx_range[1], self.yy_range[0], self.yy_range[1],
            self.zz_range[0], self.zz_range[1], -1, -1,
            self.category.get_aux("add_to_leading_pt"),
            self.category.get_aux("add_to_subleading_pt"),
            self.category.get_aux("add_to_trigger_jets", 10)
        )
        run.AsymmVBFLoop()

        f = ROOT.TFile.Open(input_file)
        tree = f.Get(self.tree_name)
        stats = {
            "nevents": tree.GetEntries(),
            "den": ditau_acc.GetValue()
        }
        f.Close()
        stats_path = self.output()["stats"].path
        with open(create_file_dir(stats_path), "w") as json_f:
            json.dump(stats, json_f, indent=4)


class AsymmKetiAcceptance(Acceptance):
    xx_range = (32, 40)
    yy_range = (20, 33)
    zz_range = (20, 160)

    def get_new_dataframe(self, input_file, output_file):
        ROOT = import_root()

        df = ROOT.RDataFrame(self.tree_name, input_file)
        
        ROOT.gInterpreter.Declare("""
            #include <math.h>
            using Vfloat = const ROOT::RVec<float>&;
            float inv_mass(float pt1, float eta1, float phi1, float pt2, float eta2, float phi2){
                return sqrt(2 * pt1 * pt2 * (cosh(eta1 - eta2) - cos(phi1 - phi2)));
            }
            """
        )

        ditau_acc = df.Filter(
            "(leading_l1tau_pt >= {0} "
                "&& subleading_l1tau_pt >= {1}"
                "&& leading_tau_pt >= ({0} + {2}) "
                "&& subleading_tau_pt >= ({1} + {3})) || "
            "(leading_l1jet_pt >= 35"
                "&& subleading_l1jet_pt >= 35"
                "&& leading_jet_pt >= 35 + {4}"
                "&& subleading_jet_pt >= 35 + {4}"
                "&& inv_mass(leading_l1jet_pt, leading_l1jet_eta, leading_l1jet_phi,"
                    "subleading_l1jet_pt, subleading_l1jet_eta, subleading_l1jet_phi) >= 450"
                "&& inv_mass(leading_jet_pt, leading_jet_eta, leading_jet_phi,"
                    "subleading_jet_pt, subleading_jet_eta, subleading_jet_phi) >= 550"
                "&& leading_l1tau_pt >= 45"
                "&& leading_tau_pt >= 45 + {2})".format(
                    32,
                    32,
                    self.category.get_aux("add_to_leading_pt"),
                    self.category.get_aux("add_to_subleading_pt"),
                    self.category.get_aux("add_to_trigger_jets", 10)
            )
        ).Count()

        ROOT.gSystem.Load("%s" % os.path.expandvars("$CMT_BASE/cmt/tasks/./TotalTrigger_C.so"))

        run = ROOT.TotalTrigger(input_file, self.output()["root"].path, self.tree_name,
            self.xx_range[0], self.xx_range[1], self.yy_range[0], self.yy_range[1],
            self.zz_range[0], self.zz_range[1], -1, -1,
            self.category.get_aux("add_to_leading_pt"),
            self.category.get_aux("add_to_subleading_pt"),
            self.category.get_aux("add_to_trigger_jets", 10)
        )
        run.AsymmKetiLoop()

        f = ROOT.TFile.Open(input_file)
        tree = f.Get(self.tree_name)
        stats = {
            "nevents": tree.GetEntries(),
            "den": ditau_acc.GetValue()
        }
        f.Close()
        stats_path = self.output()["stats"].path
        with open(create_file_dir(stats_path), "w") as json_f:
            json.dump(stats, json_f, indent=4)


class AsymmVBFAcceptanceWrapper(DatasetCategoryWrapperTask):

    def atomic_requires(self, dataset, category):
        return AsymmVBFAcceptance.vreq(self, dataset_name=dataset.name, category_name=category.name)


class AsymmKetiAcceptanceWrapper(DatasetCategoryWrapperTask):

    def atomic_requires(self, dataset, category):
        return AsymmKetiAcceptance.vreq(self, dataset_name=dataset.name, category_name=category.name)


class AsymmDiJetAcceptance(Acceptance):
    xx_range = (32, 40)
    yy_range = (20, 33)
    dzz_range = (20, 100)
    def get_new_dataframe(self, input_file, output_file):
        ROOT = import_root()

        df = ROOT.RDataFrame(self.tree_name, input_file)

        ditau_acc = df.Filter(
            "leading_l1tau_pt >= {0} "
            "&& subleading_l1tau_pt >= {1}"
            "&& leading_tau_pt >= ({0} + {2}) "
            "&& subleading_tau_pt >= ({1} + {3})".format(
                32,
                32,
                self.category.get_aux("add_to_leading_pt"),
                self.category.get_aux("add_to_subleading_pt")
            )
        ).Count()

        ROOT.gSystem.Load("%s" % os.path.expandvars("$CMT_BASE/cmt/tasks/./TotalTrigger_C.so"))

        run = ROOT.TotalTrigger(input_file, self.output()["root"].path, self.tree_name,
            self.xx_range[0], self.xx_range[1], self.yy_range[0], self.yy_range[1],
            -1, -1, self.dzz_range[0], self.dzz_range[1],
            self.category.get_aux("add_to_leading_pt"),
            self.category.get_aux("add_to_subleading_pt"),
            self.category.get_aux("add_to_trigger_jets", 10)
        )
        run.AsymmDiJetLoop()

        f = ROOT.TFile.Open(input_file)
        tree = f.Get(self.tree_name)
        stats = {
            "nevents": tree.GetEntries(),
            "den": ditau_acc.GetValue()
        }
        f.Close()
        stats_path = self.output()["stats"].path
        with open(create_file_dir(stats_path), "w") as json_f:
            json.dump(stats, json_f, indent=4)


class AsymmDiJetAcceptanceWrapper(DatasetCategoryWrapperTask):

    def atomic_requires(self, dataset, category):
        return AsymmDiJetAcceptance.vreq(self, dataset_name=dataset.name, category_name=category.name)


class AsymmVBFDiJetAcceptance(Acceptance):
    xx_range = (32, 40)
    yy_range = (20, 33)
    dzz_range = (20, 100)
    def get_new_dataframe(self, input_file, output_file):
        ROOT = import_root()

        df = ROOT.RDataFrame(self.tree_name, input_file)

        ROOT.gInterpreter.Declare("""
            #include <math.h>
            using Vfloat = const ROOT::RVec<float>&;
            float inv_mass(float pt1, float eta1, float phi1, float pt2, float eta2, float phi2){
                return sqrt(2 * pt1 * pt2 * (cosh(eta1 - eta2) - cos(phi1 - phi2)));
            }
            """
        )

        ditau_acc = df.Filter(
            "(leading_l1tau_pt >= {0} "
                "&& subleading_l1tau_pt >= {1}"
                "&& leading_tau_pt >= ({0} + {2}) "
                "&& subleading_tau_pt >= ({1} + {3})) || "
            "(leading_l1jet_pt >= 90"
                "&& subleading_l1jet_pt >= 30"
                "&& leading_jet_pt >= 90 + {4}"
                "&& subleading_jet_pt >= 30 + {4}"
                "&& inv_mass(leading_l1jet_pt, leading_l1jet_eta, leading_l1jet_phi,"
                    "subleading_l1jet_pt, subleading_l1jet_eta, subleading_l1jet_phi) >= 620"
                "&& inv_mass(leading_jet_pt, leading_jet_eta, leading_jet_phi,"
                    "subleading_jet_pt, subleading_jet_eta, subleading_jet_phi) >= 750)"
                "".format(
                    32,
                    32,
                    self.category.get_aux("add_to_leading_pt"),
                    self.category.get_aux("add_to_subleading_pt"),
                    self.category.get_aux("add_to_trigger_jets", 10)
            )
        ).Count()

        ROOT.gSystem.Load("%s" % os.path.expandvars("$CMT_BASE/cmt/tasks/./TotalTrigger_C.so"))

        run = ROOT.TotalTrigger(input_file, self.output()["root"].path, self.tree_name,
            self.xx_range[0], self.xx_range[1], self.yy_range[0], self.yy_range[1],
            -1, -1, self.dzz_range[0], self.dzz_range[1],
            self.category.get_aux("add_to_leading_pt"),
            self.category.get_aux("add_to_subleading_pt"),
            self.category.get_aux("add_to_trigger_jets", 10)
        )
        run.AsymmVBFDiJetLoop()

        f = ROOT.TFile.Open(input_file)
        tree = f.Get(self.tree_name)
        stats = {
            "nevents": tree.GetEntries(),
            "den": ditau_acc.GetValue()
        }
        f.Close()
        stats_path = self.output()["stats"].path
        with open(create_file_dir(stats_path), "w") as json_f:
            json.dump(stats, json_f, indent=4)


class AsymmKetiDiJetAcceptance(Acceptance):
    xx_range = (32, 40)
    yy_range = (20, 33)
    dzz_range = (20, 100)
    def get_new_dataframe(self, input_file, output_file):
        ROOT = import_root()

        df = ROOT.RDataFrame(self.tree_name, input_file)

        ROOT.gInterpreter.Declare("""
            #include <math.h>
            using Vfloat = const ROOT::RVec<float>&;
            float inv_mass(float pt1, float eta1, float phi1, float pt2, float eta2, float phi2){
                return sqrt(2 * pt1 * pt2 * (cosh(eta1 - eta2) - cos(phi1 - phi2)));
            }
            """
        )

        ditau_acc = df.Filter(
            "(leading_l1tau_pt >= {0} "
                "&& subleading_l1tau_pt >= {1}"
                "&& leading_tau_pt >= ({0} + {2}) "
                "&& subleading_tau_pt >= ({1} + {3})) || "
            "(leading_l1jet_pt >= 35"
                "&& subleading_l1jet_pt >= 35"
                "&& leading_jet_pt >= 35 + {4}"
                "&& subleading_jet_pt >= 35 + {4}"
                "&& inv_mass(leading_l1jet_pt, leading_l1jet_eta, leading_l1jet_phi,"
                    "subleading_l1jet_pt, subleading_l1jet_eta, subleading_l1jet_phi) >= 450"
                "&& inv_mass(leading_jet_pt, leading_jet_eta, leading_jet_phi,"
                    "subleading_jet_pt, subleading_jet_eta, subleading_jet_phi) >= 550"
                "&& leading_l1tau_pt >= 45"
                "&& leading_tau_pt >= 45 + {2})".format(
                    32,
                    32,
                    self.category.get_aux("add_to_leading_pt"),
                    self.category.get_aux("add_to_subleading_pt"),
                    self.category.get_aux("add_to_trigger_jets", 10)
            )
        ).Count()

        ROOT.gSystem.Load("%s" % os.path.expandvars("$CMT_BASE/cmt/tasks/./TotalTrigger_C.so"))

        run = ROOT.TotalTrigger(input_file, self.output()["root"].path, self.tree_name,
            self.xx_range[0], self.xx_range[1], self.yy_range[0], self.yy_range[1],
            -1, -1, self.dzz_range[0], self.dzz_range[1],
            self.category.get_aux("add_to_leading_pt"),
            self.category.get_aux("add_to_subleading_pt"),
            self.category.get_aux("add_to_trigger_jets", 10)
        )
        run.AsymmKetiDiJetLoop()

        f = ROOT.TFile.Open(input_file)
        tree = f.Get(self.tree_name)
        stats = {
            "nevents": tree.GetEntries(),
            "den": ditau_acc.GetValue()
        }
        f.Close()
        stats_path = self.output()["stats"].path
        with open(create_file_dir(stats_path), "w") as json_f:
            json.dump(stats, json_f, indent=4)


class AsymmVBFDiJetAcceptanceWrapper(DatasetCategoryWrapperTask):

    def atomic_requires(self, dataset, category):
        return AsymmVBFDiJetAcceptance.vreq(self, dataset_name=dataset.name, category_name=category.name)


class AsymmKetiDiJetAcceptanceWrapper(DatasetCategoryWrapperTask):

    def atomic_requires(self, dataset, category):
        return AsymmKetiDiJetAcceptance.vreq(self, dataset_name=dataset.name, category_name=category.name)


class ComputeRate(AddTrigger):

    additional_branches = [
        'nTaus', 'tauEt', 'tauEta', 'tauPhi', 'tauIso', 'tauBx',
        'nJets', 'jetEt', 'jetEta', 'jetPhi', 'jetBx'
    ]
    
    def __init__(self, *args, **kwargs):
        super(ComputeRate, self).__init__(*args, **kwargs)
        self.tree_name = self.dataset.get_aux("treename", "l1UpgradeTree/L1UpgradeTree")
        #tree_name = "l1UpgradeEmuTree/L1UpgradeTree"

    def output(self):
        return self.local_target("data_{}.root".format(self.branch))

    @classmethod
    def add_dataframe_definitions(self, df):
        # leading and subleading L1 Taus
        # df = df.Define("lead_sublead_goodl1tau_pt",
            # "lead_sublead("
                # "tauEt[tauIso == 1 && tauBx == 0 && abs(tauEta) <= 2.1], "
                # "tauEta[tauIso == 1 && tauBx == 0 && abs(tauEta) <= 2.1], "
                # "tauPhi[tauIso == 1 && tauBx == 0 && abs(tauEta) <= 2.1], "
                # "tauEt[tauIso == 1 && tauBx == 0 && abs(tauEta) <= 2.1]"  # dum
            # ")[0]"
        # )
        # df = df.Define("lead_sublead_goodl1tau_eta",
            # "lead_sublead("
                # "tauEt[tauIso == 1 && tauBx == 0 && abs(tauEta) <= 2.1], "
                # "tauEta[tauIso == 1 && tauBx == 0 && abs(tauEta) <= 2.1], "
                # "tauPhi[tauIso == 1 && tauBx == 0 && abs(tauEta) <= 2.1], "
                # "tauEt[tauIso == 1 && tauBx == 0 && abs(tauEta) <= 2.1]"  # dum
            # ")[1]"
        # ).Define("lead_sublead_goodl1tau_phi",
            # "lead_sublead("
                # "tauEt[tauIso == 1 && tauBx == 0 && abs(tauEta) <= 2.1], "
                # "tauEta[tauIso == 1 && tauBx == 0 && abs(tauEta) <= 2.1], "
                # "tauPhi[tauIso == 1 && tauBx == 0 && abs(tauEta) <= 2.1], "
                # "tauEt[tauIso == 1 && tauBx == 0 && abs(tauEta) <= 2.1]"  # dum
            # ")[2]"
        # )
        df = df.Define("lead_sublead_goodl1tau_pt",
            "lead_sublead(tauEt[tauIso == 1 && tauBx == 0 && abs(tauEta) <= 2.1], 3)"
        )
        df = df.Define("lead_sublead_goodl1tau_eta",
            "lead_sublead(tauEta[tauIso == 1 && tauBx == 0 && abs(tauEta) <= 2.1], 3)"
        )
        df = df.Define("lead_sublead_goodl1tau_phi",
            "lead_sublead(tauPhi[tauIso == 1 && tauBx == 0 && abs(tauEta) <= 2.1], 3)"
        )
        
    
        
        # restriction = (
            # "maskDeltaR("
                # "jetEta, "
                # "jetPhi, "
                # "lead_sublead_goodl1tau_eta, "
                # "lead_sublead_goodl1tau_phi, "
                # "0.5)"
            # "&& jetBx == 0"
        # )
        for tau_pt in range(*self.yy_range):
            restriction = (
                "maskDeltaR("
                    "jetEta, "
                    "jetPhi, "
                    "tauEta[tauEt >= {0} && tauIso == 1 && tauBx == 0 && abs(tauEta) <= 2.1], "
                    "tauPhi[tauEt >= {0} && tauIso == 1 && tauBx == 0 && abs(tauEta) <= 2.1], "
                    "0.5)"
                "&& jetBx == 0".format(tau_pt)
            )
            
            df = df.Define("lead_sublead_goodl1jet_pt_tau_pt_%s" % tau_pt,
                "lead_sublead(jetEt[{0}], 3)".format(restriction))

            df = df.Define("lead_sublead_goodl1jet_eta_tau_pt_%s" % tau_pt,
                "lead_sublead(jetEta[{0}], 3)".format(restriction))

            df = df.Define("lead_sublead_goodl1jet_phi_tau_pt_%s" % tau_pt,
                "lead_sublead(jetPhi[{0}], 3)".format(restriction))

        return df

    def get_new_dataframe(self, input_file, output_file):
        from analysis_tools.utils import (
            import_root, create_file_dir, join_root_selection
        )
        import itertools
        ROOT = import_root()
        ROOT = self.add_to_root(ROOT)

        df = ROOT.RDataFrame(self.tree_name, input_file)

        # add the needed definitions
        df = self.add_dataframe_definitions(df)

        if self.category.selection:
            df = df.Filter(self.category.selection)

        branch_names = []
        for xx in range(*self.xx_range):
            name = "DoubleIsoTau{}er2p1".format(xx)
            df = df.Define(name, 
                "(tauEt["
                    "abs(tauEta) <= 2.1 "
                    "&& tauIso == 1 "
                    "&& tauBx == 0 "
                    "&& tauEt >= {}"
                "].size() >= 2)".format(xx)
            )
            branch_names.append(name)

        for yy, zz in itertools.product(range(*self.yy_range), range(*self.zz_range)):
            # Using L1 Taus and Jets
            name = "DoubleIsoTau{}er2p1Jet{}".format(yy, zz)
            df = df.Define(name,
                "lead_sublead_goodl1tau_pt[0] >= {0} "
                "&& lead_sublead_goodl1tau_pt[1] >= {0}"
                "&& (jetEt["
                        "jetEt >= {1} "
                        "&& jetBx == 0"
                    "].size() >= 1)".format(yy, zz))
            branch_names.append(name)
            name = "DoubleIsoTau{}er2p1Jet{}dR0p5".format(yy, zz)
            df = df.Define(name,
                "lead_sublead_goodl1tau_pt[0] >= {0} "
                "&& lead_sublead_goodl1tau_pt[1] >= {0}"
                "&& lead_sublead_goodl1jet_pt[0] >= {1} ".format(yy, zz))
            branch_names.append(name)

        branch_list = ROOT.vector('string')()
        for branch_name in self.additional_branches + branch_names:
            branch_list.push_back(branch_name)

        df.Snapshot(self.tree_name, create_file_dir(output_file), branch_list)


class L1Skim(ComputeRate):
    
    def __init__(self, *args, **kwargs):
        super(L1Skim, self).__init__(*args, **kwargs)
        self.event_tree_name = self.dataset.get_aux("event_treename", "l1EventTree/L1EventTree")

    @classmethod
    def get_pu_per_ls(self, filename="$CMT_BASE/cmt/tasks/run_lumi.csv"):
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
        
    @classmethod
    def add_to_root(self, root):
        root = ComputeRate.add_to_root(root)
        d = self.get_pu_per_ls()
        
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
        root.gInterpreter.Declare("""
            std::map <int, std::map<int, float>> pu_lumi = %s;
        """ % dict_to_cpp)
            
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

    def get_new_dataframe(self, input_file, output_file):
        ROOT = import_root()
        ROOT = self.add_to_root(ROOT)
        # ROOT.ROOT.EnableImplicitMT()

        tchain = ROOT.TChain(self.tree_name)
        tchain.Add("{}/{}".format(input_file, self.tree_name))
        event_tchain = ROOT.TChain(self.event_tree_name)
        event_tchain.Add("{}/{}".format(input_file, self.event_tree_name))
        tchain.AddFriend(event_tchain, "event")
        df = ROOT.RDataFrame(tchain)

        #df = ROOT.RDataFrame(self.tree_name, input_file)

        # add the needed definitions
        df = self.add_dataframe_definitions(df)

        if self.category.selection:
            df = df.Filter(self.category.selection)

        # filter de number of offline jets in the event
        #df = self.filter_jets(df)

        branch_names = [
            "eventnumber",
            "leading_l1tau_pt",
            "leading_l1tau_eta",
            "leading_l1tau_phi",
            "subleading_l1tau_pt",
            "subleading_l1tau_eta",
            "subleading_l1tau_phi",
            "subsubleading_l1tau_pt",
            "subsubleading_l1tau_eta",
            "subsubleading_l1tau_phi",
            "weight"
        ]
        
        for tau_pt in range(*self.yy_range):
            branch_names += [
                "leading_l1jet_pt_tau_pt_%s" % tau_pt,
                "leading_l1jet_eta_tau_pt_%s" % tau_pt,
                "leading_l1jet_phi_tau_pt_%s" % tau_pt,
                "subleading_l1jet_pt_tau_pt_%s" % tau_pt,
                "subleading_l1jet_eta_tau_pt_%s" % tau_pt,
                "subleading_l1jet_phi_tau_pt_%s" % tau_pt,
            ]

        df = df.Define("leading_l1tau_pt", "lead_sublead_goodl1tau_pt[0]")
        df = df.Define("leading_l1tau_eta", "lead_sublead_goodl1tau_eta[0]")
        df = df.Define("leading_l1tau_phi", "lead_sublead_goodl1tau_phi[0]")
        df = df.Define("subleading_l1tau_pt", "lead_sublead_goodl1tau_pt[1]")
        df = df.Define("subleading_l1tau_eta", "lead_sublead_goodl1tau_eta[1]")
        df = df.Define("subleading_l1tau_phi", "lead_sublead_goodl1tau_phi[1]")
        df = df.Define("subsubleading_l1tau_pt", "lead_sublead_goodl1tau_pt[2]")
        df = df.Define("subsubleading_l1tau_eta", "lead_sublead_goodl1tau_eta[2]")
        df = df.Define("subsubleading_l1tau_phi", "lead_sublead_goodl1tau_phi[2]")
        for tau_pt in range(*self.yy_range):
            df = df.Define("leading_l1jet_pt_tau_pt_%s" % tau_pt,
                "lead_sublead_goodl1jet_pt_tau_pt_%s[0]" % tau_pt)
            df = df.Define("leading_l1jet_eta_tau_pt_%s" % tau_pt,
                "lead_sublead_goodl1jet_eta_tau_pt_%s[0]" % tau_pt)
            df = df.Define("leading_l1jet_phi_tau_pt_%s" % tau_pt,
                "lead_sublead_goodl1jet_phi_tau_pt_%s[0]" % tau_pt)
            df = df.Define("subleading_l1jet_pt_tau_pt_%s" % tau_pt,
                "lead_sublead_goodl1jet_pt_tau_pt_%s[1]" % tau_pt)
            df = df.Define("subleading_l1jet_eta_tau_pt_%s" % tau_pt,
                "lead_sublead_goodl1jet_eta_tau_pt_%s[1]" % tau_pt)
            df = df.Define("subleading_l1jet_phi_tau_pt_%s" % tau_pt,
                "lead_sublead_goodl1jet_phi_tau_pt_%s[1]" % tau_pt)
        # df = df.Define("run", "event.run")
        # df = df.Define("ls", "event.lumi")
        
        
        if self.dataset.isMC: 
            df = df.Define("weight", "compute_mc_weight(event.nPV_True)")
        else:
            df = df.Define("weight", "compute_weight(event.run, event.lumi)")
        df = df.Define("eventnumber", "event.event")
        
        
        # tau_restr = "tauEt >= 20 && abs(tauEta) <= 2.1 && tauIso == 1 && tauBx == 0"
        # df = df.Define("l1_tau_pt", "get_float_array(tauEt[%s])" % tau_restr)
        # df = df.Define("l1_tau_eta", "get_float_array(tauEta[%s])" % tau_restr)
        # df = df.Define("l1_tau_phi", "get_float_array(tauPhi[%s])" % tau_restr)
        # jet_restr = "jetEt >= 20 && jetBx == 0"
        # df = df.Define("l1_jet_pt", "get_float_array(jetEt[%s])" % jet_restr)
        # df = df.Define("l1_jet_eta", "get_float_array(jetEta[%s])" % jet_restr)
        # df = df.Define("l1_jet_phi", "get_float_array(jetPhi[%s])" % jet_restr)
        # branch_names += [
            # "l1_tau_pt", #"l1_tau_eta", "l1_tau_phi",
            # "l1_jet_pt", "l1_jet_eta", "l1_jet_phi", 
        # ]

        branch_list = ROOT.vector('string')()
        for branch_name in self.additional_branches + branch_names:
            branch_list.push_back(branch_name)

        df.Snapshot(self.tree_name, create_file_dir(output_file), branch_list)


class Rate(ComputeRate):
    xx_range = (32, 40)
    yy_range = (20, 33)
    zz_range = (20, 80)
    dzz_range = (20, 80)

    def workflow_requires(self):
        return {"data": L1Skim.vreq(self, _prefer_cli=["workflow"])}

    def requires(self):
        return {"data": L1Skim.vreq(self, _prefer_cli=["workflow"], branch=self.branch)}
    
    def output(self):
        return {
            "root": self.local_target("{}".format(self.input()["data"].path.split("/")[-1])),
            "stats": self.local_target("{}".format(self.input()["data"].path.split("/")[-1]
                ).replace(".root", ".json")),
        }

    def get_new_dataframe(self, input_file, output_file):
        ROOT = import_root()

        #ROOT.gROOT.ProcessLine(
        #    ".L %s " % os.path.expandvars("$CMT_BASE/cmt/tasks/TotalTrigger.C++"))
        ROOT.gSystem.Load("%s" % os.path.expandvars("$CMT_BASE/cmt/tasks/./TotalTrigger_C.so"))
        
        run = ROOT.TotalTrigger(input_file, self.output()["root"].path, self.tree_name,
            self.xx_range[0], self.xx_range[1], self.yy_range[0], self.yy_range[1],
            self.zz_range[0], self.zz_range[1], self.dzz_range[0], self.dzz_range[1],
            -1, -1, -1
        )
        run.RateTotalLoop()

        f = ROOT.TFile.Open(input_file)
        tree = f.Get(self.tree_name)
        stats = {
            "nevents": tree.GetEntries(),
        }
        f.Close()
        stats_path = self.output()["stats"].path
        with open(create_file_dir(stats_path), "w") as json_f:
            json.dump(stats, json_f, indent=4)

    @law.decorator.notify
    @law.decorator.localize(input=False)
    def run(self):
        inp = self.input()["data"].path
        self.get_new_dataframe(inp, None)


class DiTauRate(ComputeRate):
    xx_range = (32, 40)

    def workflow_requires(self):
        return {"data": L1Skim.vreq(self, _prefer_cli=["workflow"])}

    def requires(self):
        return {"data": L1Skim.vreq(self, _prefer_cli=["workflow"], branch=self.branch)}
    
    def output(self):
        return {
            "root": self.local_target("{}".format(self.input()["data"].path.split("/")[-1])),
            "stats": self.local_target("{}".format(self.input()["data"].path.split("/")[-1]
                ).replace(".root", ".json")),
        }

    def get_new_dataframe(self, input_file, output_file):
        ROOT = import_root()

        df = ROOT.RDataFrame(self.tree_name, input_file)
        counts = {}
        counts_noweight = {}
        hmodel = ("total", "", 1, 1, 2)
        
        ROOT.gInterpreter.Declare("""
            float get_weight(float weight) {
                if (weight == 0) return 0.;
                else return weight;                    
            }
        """)
        
        # counts["total"] = df.Define("myweight", "get_weight(weight)").Define(
        #   "pass", "leading_l1tau_pt > -999").Histo1D(hmodel, "pass", "myweight")
        for xx in range(*self.xx_range):
            for xxp in range(self.xx_range[0], xx + 1):
                hmodel = ("%s_%s" % (xx, xxp), "", 1, 1, 2)
                counts[(xx, xxp)] = df.Define("myweight", "get_weight(weight)").Define(
                    "pass", "leading_l1tau_pt >= {0} && subleading_l1tau_pt >= {1}".format(xx, xxp)
                ).Histo1D(hmodel, "pass", "myweight")
                counts_noweight[(xx, xxp)] = df.Define("pass", "leading_l1tau_pt >= {0} "
                    "&& subleading_l1tau_pt >= {1}".format(xx, xxp)).Histo1D(hmodel, "pass")
                # counts[(xx, xxp)] = df.Filter(
                    # "leading_l1tau_pt >= {0} "
                    # "&& subleading_l1tau_pt >= {1}".format(xx, xxp)
                # ).Count()
        
        histo2D = ROOT.TH2F("histo_ditau", "; xx; xxp; Events", 8, 32, 40, 8, 32, 40)
        histo2D_noweight = ROOT.TH2F("histo_ditau_noweight", "; xx; xxp; Events", 8, 32, 40, 8, 32, 40)
        for xx in range(*self.xx_range):
            for xxp in range(self.xx_range[0], xx + 1):
                histo2D.Fill(xx, xxp, counts[(xx, xxp)].Integral())
                histo2D_noweight.Fill(xx, xxp, counts_noweight[(xx, xxp)].Integral())

        f = ROOT.TFile.Open(self.output()["root"].path, "RECREATE")
        histo2D.Write()
        histo2D_noweight.Write()
        f.Close()

        f = ROOT.TFile.Open(input_file)
        tree = f.Get(self.tree_name)
        stats = {
            #"nevents": counts["total"].Integral(),
             "nevents": tree.GetEntries(),
        }
        f.Close()
        stats_path = self.output()["stats"].path
        with open(create_file_dir(stats_path), "w") as json_f:
            json.dump(stats, json_f, indent=4)

    @law.decorator.notify
    @law.decorator.localize(input=False)
    def run(self):
        inp = self.input()["data"].path
        self.get_new_dataframe(inp, None)


# class RateWrapper(DatasetCategoryWrapperTask):

    # def atomic_requires(self, dataset, category):
        # return Rate.vreq(self, dataset_name=dataset.name, category_name=category.name)


class AsymmRate(Rate):

    xx_range = (32, 40)
    yy_range = (20, 33)
    zz_range = (20, 160)

    def get_new_dataframe(self, input_file, output_file):
        ROOT = import_root()

        #ROOT.gROOT.ProcessLine(
        #    ".L %s " % os.path.expandvars("$CMT_BASE/cmt/tasks/TotalTrigger.C++"))
        ROOT.gSystem.Load("%s" % os.path.expandvars("$CMT_BASE/cmt/tasks/./TotalTrigger_C.so"))
        
        run = ROOT.TotalTrigger(input_file, self.output()["root"].path, self.tree_name,
            self.xx_range[0], self.xx_range[1], self.yy_range[0], self.yy_range[1],
            self.zz_range[0], self.zz_range[1], -1, -1,
            -1, -1, -1
        )
        run.RateAsymmLoop()

        f = ROOT.TFile.Open(input_file)
        tree = f.Get(self.tree_name)
        stats = {
            "nevents": tree.GetEntries(),
        }
        f.Close()
        stats_path = self.output()["stats"].path
        with open(create_file_dir(stats_path), "w") as json_f:
            json.dump(stats, json_f, indent=4)


class AsymmTriTauRate(Rate):

    xx_range = (32, 40)
    yy_range = (20, 33)
    zz_range = (20, 160)

    def get_new_dataframe(self, input_file, output_file):
        ROOT = import_root()

        #ROOT.gROOT.ProcessLine(
        #    ".L %s " % os.path.expandvars("$CMT_BASE/cmt/tasks/TotalTrigger.C++"))
        ROOT.gSystem.Load("%s" % os.path.expandvars("$CMT_BASE/cmt/tasks/./TotalTrigger_C.so"))
        
        run = ROOT.TotalTrigger(input_file, self.output()["root"].path, self.tree_name,
            self.xx_range[0], self.xx_range[1], self.yy_range[0], self.yy_range[1],
            self.zz_range[0], self.zz_range[1], -1, -1,
            -1, -1, -1
        )
        run.RateAsymmTriTauLoop()

        f = ROOT.TFile.Open(input_file)
        tree = f.Get(self.tree_name)
        stats = {
            "nevents": tree.GetEntries(),
        }
        f.Close()
        stats_path = self.output()["stats"].path
        with open(create_file_dir(stats_path), "w") as json_f:
            json.dump(stats, json_f, indent=4)


class AsymmVBFRate(Rate):

    xx_range = (32, 40)
    yy_range = (20, 33)
    zz_range = (20, 160)

    def get_new_dataframe(self, input_file, output_file):
        ROOT = import_root()

        #ROOT.gROOT.ProcessLine(
        #    ".L %s " % os.path.expandvars("$CMT_BASE/cmt/tasks/TotalTrigger.C++"))
        ROOT.gSystem.Load("%s" % os.path.expandvars("$CMT_BASE/cmt/tasks/./TotalTrigger_C.so"))
        
        run = ROOT.TotalTrigger(input_file, self.output()["root"].path, self.tree_name,
            self.xx_range[0], self.xx_range[1], self.yy_range[0], self.yy_range[1],
            self.zz_range[0], self.zz_range[1], -1, -1,
            -1, -1, -1
        )
        run.RateAsymmVBFLoop()

        f = ROOT.TFile.Open(input_file)
        tree = f.Get(self.tree_name)
        stats = {
            "nevents": tree.GetEntries(),
        }
        f.Close()
        stats_path = self.output()["stats"].path
        with open(create_file_dir(stats_path), "w") as json_f:
            json.dump(stats, json_f, indent=4)


# class AsymmRateWrapper(DatasetCategoryWrapperTask):

    # def atomic_requires(self, dataset, category):
        # return AsymmRate.vreq(self, dataset_name=dataset.name, category_name=category.name)


class AsymmDiJetRate(Rate):

    xx_range = (32, 40)
    yy_range = (20, 33)
    dzz_range = (20, 100)

    def get_new_dataframe(self, input_file, output_file):
        ROOT = import_root()

        #ROOT.gROOT.ProcessLine(
        #    ".L %s " % os.path.expandvars("$CMT_BASE/cmt/tasks/TotalTrigger.C++"))
        ROOT.gSystem.Load("%s" % os.path.expandvars("$CMT_BASE/cmt/tasks/./TotalTrigger_C.so"))
        
        run = ROOT.TotalTrigger(input_file, self.output()["root"].path, self.tree_name,
            self.xx_range[0], self.xx_range[1], self.yy_range[0], self.yy_range[1],
            -1, -1, self.dzz_range[0], self.dzz_range[1], 
            -1, -1, -1
        )
        run.RateAsymmDiJetLoop()

        f = ROOT.TFile.Open(input_file)
        tree = f.Get(self.tree_name)
        stats = {
            "nevents": tree.GetEntries(),
        }
        f.Close()
        stats_path = self.output()["stats"].path
        with open(create_file_dir(stats_path), "w") as json_f:
            json.dump(stats, json_f, indent=4)


# class AsymmDiJetRateWrapper(DatasetCategoryWrapperTask):

    # def atomic_requires(self, dataset, category):
        # return AsymmDiJetRate.vreq(self, dataset_name=dataset.name, category_name=category.name)


class AsymmVBFDiJetRate(Rate):

    xx_range = (32, 40)
    yy_range = (20, 33)
    dzz_range = (20, 100)

    def get_new_dataframe(self, input_file, output_file):
        ROOT = import_root()

        #ROOT.gROOT.ProcessLine(
        #    ".L %s " % os.path.expandvars("$CMT_BASE/cmt/tasks/TotalTrigger.C++"))
        ROOT.gSystem.Load("%s" % os.path.expandvars("$CMT_BASE/cmt/tasks/./TotalTrigger_C.so"))
        
        run = ROOT.TotalTrigger(input_file, self.output()["root"].path, self.tree_name,
            self.xx_range[0], self.xx_range[1], self.yy_range[0], self.yy_range[1],
            -1, -1, self.dzz_range[0], self.dzz_range[1], 
            -1, -1, -1
        )
        run.RateAsymmVBFDiJetLoop()

        f = ROOT.TFile.Open(input_file)
        tree = f.Get(self.tree_name)
        stats = {
            "nevents": tree.GetEntries(),
        }
        f.Close()
        stats_path = self.output()["stats"].path
        with open(create_file_dir(stats_path), "w") as json_f:
            json.dump(stats, json_f, indent=4)


class AsymmManfredRate(AsymmRate):

    xx_range = (32, 40)
    yy_range = (20, 33)
    zz_range = (20, 160)

    def get_new_dataframe(self, input_file, output_file):
        ROOT = import_root()

        #ROOT.gROOT.ProcessLine(
        #    ".L %s " % os.path.expandvars("$CMT_BASE/cmt/tasks/TotalTrigger.C++"))
        ROOT.gSystem.Load("%s" % os.path.expandvars("$CMT_BASE/cmt/tasks/./TotalTrigger_C.so"))
        
        run = ROOT.TotalTrigger(input_file, self.output()["root"].path, self.tree_name,
            self.xx_range[0], self.xx_range[1], self.yy_range[0], self.yy_range[1],
            self.zz_range[0], self.zz_range[1], -1, -1,
            -1, -1, -1
        )
        run.RateAsymmManfredLoop()

        f = ROOT.TFile.Open(input_file)
        tree = f.Get(self.tree_name)
        stats = {
            "nevents": tree.GetEntries(),
        }
        f.Close()
        stats_path = self.output()["stats"].path
        with open(create_file_dir(stats_path), "w") as json_f:
            json.dump(stats, json_f, indent=4)


class ComputeDiJetRate(ComputeRate):

    xx_range = (32, 40)
    yy_range = (20, 33)
    zz_range = (20, 100)

    def get_new_dataframe(self, input_file, output_file):
        from analysis_tools.utils import (
            import_root, create_file_dir, join_root_selection
        )
        import itertools
        ROOT = import_root()
        ROOT = self.add_to_root(ROOT)

        df = ROOT.RDataFrame(self.tree_name, input_file)

        # add the needed definitions
        df = self.add_dataframe_definitions(df)

        if self.category.selection:
            df = df.Filter(self.category.selection)

        branch_names = []
        for xx in range(*self.xx_range):
            name = "DoubleIsoTau{}er2p1".format(xx)
            df = df.Define(name, 
                "(tauEt["
                    "abs(tauEta) <= 2.1 "
                    "&& tauIso == 1 "
                    "&& tauBx == 0 "
                    "&& tauEt >= {} "
                "].size() >= 2)".format(xx)
            )
            branch_names.append(name)

        for yy, zz in itertools.product(range(*self.yy_range), range(*self.zz_range)):
            # Using L1 Taus and Jets
            name = "DoubleIsoTau{0}er2p1Jet{1}dR0p5Jet{1}dR0p5".format(yy, zz)
            df = df.Define(name,
                "lead_sublead_goodl1tau_pt[0] >= {0} "
                "&& lead_sublead_goodl1tau_pt[1] >= {0}"
                "&& lead_sublead_goodl1jet_pt[0] >= {1} "
                "&& lead_sublead_goodl1jet_pt[1] >= {1} ".format(yy, zz))
            branch_names.append(name)

        branch_list = ROOT.vector('string')()
        for branch_name in self.additional_branches + branch_names:
            branch_list.push_back(branch_name)

        df.Snapshot(self.tree_name, create_file_dir(output_file), branch_list)


class ComputeAsymRate(ComputeRate):

    xx_range = (32, 40)
    yy_range = (20, 33)
    zz_range = (20, 160)

    def get_new_dataframe(self, input_file, output_file):
        from analysis_tools.utils import (
            import_root, create_file_dir, join_root_selection
        )
        import itertools
        ROOT = import_root()
        ROOT = self.add_to_root(ROOT)

        df = ROOT.RDataFrame(self.tree_name, input_file)

        # add the needed definitions
        df = self.add_dataframe_definitions(df)

        if self.category.selection:
            df = df.Filter(self.category.selection)

        branch_names = []
        for xx in range(*self.xx_range):
            name = "DoubleIsoTau{}er2p1".format(xx)
            df = df.Define(name,
                "(tauEt["
                    "abs(tauEta) <= 2.1 "
                    "&& tauIso == 1 "
                    "&& tauBx == 0 "
                    "&& tauEt >= {} "
                "].size() >= 2)".format(xx)
            )
            branch_names.append(name)

        for yy, zz in itertools.product(range(*self.yy_range), range(*self.zz_range)):
            for yyp in range(self.yy_range[0], yy + 1):
                # Using L1 Taus and Jets
                name = "IsoTau{0}IsoTau{1}er2p1Jet{2}dR0p5".format(yy, yyp, zz)
                df = df.Define(name,
                    "lead_sublead_goodl1tau_pt[0] >= {0} "
                    "&& lead_sublead_goodl1tau_pt[1] >= {1}"
                    "&& lead_sublead_goodl1jet_pt[0] >= {2}".format(yy, yyp, zz))
                branch_names.append(name)

        branch_list = ROOT.vector('string')()
        for branch_name in self.additional_branches + branch_names:
            branch_list.push_back(branch_name)

        df.Snapshot(self.tree_name, create_file_dir(output_file), branch_list)
