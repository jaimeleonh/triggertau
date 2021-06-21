# coding: utf-8

import law
import luigi

from cmt.base_tasks.base import ( 
    DatasetTaskWithCategory, DatasetWrapperTask, HTCondorWorkflow, InputData, ConfigTaskWithCategory
)


class AddTrigger(DatasetTaskWithCategory, law.LocalWorkflow, HTCondorWorkflow):

    #xx_range = (20, 70)
    #yy_range = (20, 60)
    #zz_range = (20, 90)

    xx_range = (32, 40)
    yy_range = (20, 33)
    zz_range = (20, 160)

    additional_branches = [
        #"nTau","Tau_pt", "Tau_eta", "Tau_phi", "Tau_mass",
        #"nJet","Jet_pt", "Jet_eta", "Jet_phi", "Jet_mass",
        #"nGenVisTau", "GenVisTau_pt", "GenVisTau_eta", "GenVisTau_phi", "GenVisTau_mass",
        #"nTrigTau",
        #"TrigTau_pt", "TrigTau_phi", "TrigTau_eta"
        "nL1Obj", "L1Obj_pt", "L1Obj_eta", "L1Obj_phi", "L1Obj_type",
    ]   

    # regions not supported
    region_name = None
    tree_name = "Events"

    default_store = "$CMT_STORE_EOS_CATEGORIZATION"
    default_wlcg_fs = "wlcg_fs_categorization"

    def create_branch_map(self):
        return len(self.dataset.get_files())

    def workflow_requires(self):
        return {"data": InputData.req(self)}

    def requires(self):
        return {"data": InputData.req(self, file_index=self.branch)}

    def output(self):
        return self.local_target("{}".format(self.input()["data"].path.split("/")[-1]))
        # return self.local_target("{}".format(self.input()["data"].split("/")[-1]))
    
    # some definitions needed
    @classmethod
    def add_to_root(self, root):
        root.gInterpreter.Declare("""
            using Vfloat = const ROOT::RVec<float>&;      
            ROOT::RVec<ROOT::RVec<float>> lead_sublead(Vfloat pt, Vfloat eta, Vfloat phi, Vfloat mass){
                ROOT::RVec<float> leading_pts = {-1., -1.};
                ROOT::RVec<float> leading_etas = {-1., -1.};
                ROOT::RVec<float> leading_phis = {-1., -1.};
                ROOT::RVec<float> leading_mass = {-1., -1.};
                for (size_t i = 0; i < pt.size(); i++) {
                    if (pt[i] > leading_pts[0]){
                        leading_pts[1] = leading_pts[0];
                        leading_etas[1] = leading_etas[0];
                        leading_phis[1] = leading_phis[0];
                        leading_mass[1] = leading_mass[0];
        
                        leading_pts[0] = pt[i];
                        leading_etas[0] = eta[i];
                        leading_phis[0] = phi[i];
                        leading_mass[0] = mass[i];
                    } 
                    else if (pt[i] > leading_pts[1]){
                        leading_pts[1] = pt[i];
                        leading_etas[1] = eta[i];
                        leading_phis[1] = phi[i];
                        leading_mass[1] = mass[i];
                    }
                }
                return ROOT::RVec({leading_pts, leading_etas, leading_phis, leading_mass});
            }
        """)
        
        root.gInterpreter.Declare("""
            Double_t Phi_mpi_pi(Double_t x) {
                while (x >= 3.14159) x -= (2 * 3.14159);
                while (x < -3.14159) x += (2 * 3.14159);
                return x;
            }
        """)

        root.gInterpreter.Declare("""
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
        """)
        return root

    @classmethod
    def add_dataframe_definitions(self, df):
        # leading and subleading L1 Taus
        df = df.Define("lead_sublead_goodl1tau_pt",
            "lead_sublead("
                "L1Obj_pt[L1Obj_type == 1 && L1Obj_iso == 1 && abs(L1Obj_eta) <= 2.1], "
                "L1Obj_eta[L1Obj_type == 1 && L1Obj_iso == 1 && abs(L1Obj_eta) <= 2.1], "
                "L1Obj_phi[L1Obj_type == 1 && L1Obj_iso == 1 && abs(L1Obj_eta) <= 2.1], "
                "L1Obj_pt[L1Obj_type == 1 && L1Obj_iso == 1 && abs(L1Obj_eta) <= 2.1]"  # dum
            ")[0]"
        ).Define("lead_sublead_goodl1tau_eta", 
            "lead_sublead("
                "L1Obj_pt[L1Obj_type == 1 && L1Obj_iso == 1 && abs(L1Obj_eta) <= 2.1], "
                "L1Obj_eta[L1Obj_type == 1 && L1Obj_iso == 1 && abs(L1Obj_eta) <= 2.1], "
                "L1Obj_phi[L1Obj_type == 1 && L1Obj_iso == 1 && abs(L1Obj_eta) <= 2.1], "
                "L1Obj_pt[L1Obj_type == 1 && L1Obj_iso == 1 && abs(L1Obj_eta) <= 2.1]"  # dum
            ")[1]"
        ).Define("lead_sublead_goodl1tau_phi", 
            "lead_sublead("
                "L1Obj_pt[L1Obj_type == 1 && L1Obj_iso == 1 && abs(L1Obj_eta) <= 4.7], "
                "L1Obj_eta[L1Obj_type == 1 && L1Obj_iso == 1 && abs(L1Obj_eta) <= 4.7], "
                "L1Obj_phi[L1Obj_type == 1 && L1Obj_iso == 1 && abs(L1Obj_eta) <= 4.7], "
                "L1Obj_pt[L1Obj_type == 1 && L1Obj_iso == 1 && abs(L1Obj_eta) <= 4.7]"  # dum
            ")[2]")

        # leading and subleading Reco Taus
        df = df.Define("lead_sublead_goodtau_pt",
            "lead_sublead("
                "Tau_pt[abs(Tau_eta) <= 2.1], "
                "Tau_eta[abs(Tau_eta) <= 2.1], "
                "Tau_phi[abs(Tau_eta) <= 2.1], "
                "Tau_mass[abs(Tau_eta) <= 2.1]"
            ")[0]").Define("lead_sublead_goodtau_eta", 
            "lead_sublead("
                "Tau_pt[abs(Tau_eta) <= 2.1], "
                "Tau_eta[abs(Tau_eta) <= 2.1], "
                "Tau_phi[abs(Tau_eta) <= 2.1], "
                "Tau_mass[abs(Tau_eta) <= 2.1]"
            ")[1]").Define("lead_sublead_goodtau_phi", 
            "lead_sublead("
                "Tau_pt[abs(Tau_eta) <= 2.1], "
                "Tau_eta[abs(Tau_eta) <= 2.1], "
                "Tau_phi[abs(Tau_eta) <= 2.1], "
                "Tau_mass[abs(Tau_eta) <= 2.1]"
            ")[2]")
        
        # leading and subleading L1 jets
        restriction = (
            "L1Obj_type == 0 "
            "&& maskDeltaR("
                "L1Obj_eta, "
                "L1Obj_phi, "
                "lead_sublead_goodl1tau_eta, "
                "lead_sublead_goodl1tau_phi, "
                "0.5)"
        )
        df = df.Define("lead_sublead_goodl1jet_pt",
            "lead_sublead("
                "L1Obj_pt[{0}],"
                "L1Obj_eta[{0}],"
                "L1Obj_phi[{0}],"
                "L1Obj_pt[{0}]"
            ")[0]".format(restriction))

        # leading and subleading offline jets
        restriction = (
            "abs(Jet_eta) <= 4.7 && Jet_jetId >= 2 "
            "&& ((Jet_puId >= 4 && Jet_pt <= 50) || (Jet_pt > 50))"
            "&& maskDeltaR("
                "Jet_eta, "
                "Jet_phi, "
                "lead_sublead_goodtau_eta, "
                "lead_sublead_goodtau_phi, "
                "0.5)")
        df = df.Define("lead_sublead_goodjet_pt",
            "lead_sublead("
                "Jet_pt[{0}],"
                "Jet_eta[{0}],"
                "Jet_phi[{0}],"
                "Jet_mass[{0}]"
            ")[0]".format(restriction))
        
        return df
    
    def filter_jets(self, df):
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
                    self.category.get_aux("add_to_jet_pt"),
                    self.category.get_aux("nminjets"),
                    self.category.get_aux("nmaxjets")
                )
        )

    def get_new_dataframe(self, input_file, output_file):
        from analysis_tools.utils import (
            import_root, create_file_dir, join_root_selection
        )
        import itertools
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
                        "lead_sublead_goodl1tau_eta, "
                        "lead_sublead_goodl1tau_phi, "
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


class AddDiJetTrigger(AddTrigger):

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


class ComputeRate(AddTrigger):

    additional_branches = [
        'nTaus', 'tauEt', 'tauEta', 'tauPhi', 'tauIso',
        'nJets', 'jetEt', 'jetEta', 'jetPhi'
    ]
    
    tree_name = "l1UpgradeTree/L1UpgradeTree"
    #tree_name = "l1UpgradeEmuTree/L1UpgradeTree"

    def output(self):
        return self.local_target("data_{}.root".format(self.branch))

    @classmethod
    def add_dataframe_definitions(self, df):
        # leading and subleading L1 Taus
        df = df.Define("lead_sublead_goodl1tau_pt",
            "lead_sublead("
                "tauEt[tauIso == 1 && tauBx == 0 && abs(tauEta) <= 2.1], "
                "tauEta[tauIso == 1 && tauBx == 0 && abs(tauEta) <= 2.1], "
                "tauPhi[tauIso == 1 && tauBx == 0 && abs(tauEta) <= 2.1], "
                "tauEt[tauIso == 1 && tauBx == 0 && abs(tauEta) <= 2.1]"  # dum
            ")[0]"
        )
        df = df.Define("lead_sublead_goodl1tau_eta",
            "lead_sublead("
                "tauEt[tauIso == 1 && tauBx == 0 && abs(tauEta) <= 2.1], "
                "tauEta[tauIso == 1 && tauBx == 0 && abs(tauEta) <= 2.1], "
                "tauPhi[tauIso == 1 && tauBx == 0 && abs(tauEta) <= 2.1], "
                "tauEt[tauIso == 1 && tauBx == 0 && abs(tauEta) <= 2.1]"  # dum
            ")[1]"
        ).Define("lead_sublead_goodl1tau_phi",
            "lead_sublead("
                "tauEt[tauIso == 1 && tauBx == 0 && abs(tauEta) <= 2.1], "
                "tauEta[tauIso == 1 && tauBx == 0 && abs(tauEta) <= 2.1], "
                "tauPhi[tauIso == 1 && tauBx == 0 && abs(tauEta) <= 2.1], "
                "tauEt[tauIso == 1 && tauBx == 0 && abs(tauEta) <= 2.1]"  # dum
            ")[2]"
        )
        
        df = df.Define("lead_sublead_goodl1jet_pt",
            "lead_sublead("
                "jetEt["
                    "maskDeltaR("
                        "jetEta, "
                        "jetPhi, "
                        "lead_sublead_goodl1tau_eta, "
                        "lead_sublead_goodl1tau_phi, "
                        "0.5) "
                    "&& jetBx == 0"
                "],"
                "jetEta["
                    "maskDeltaR("
                        "jetEta, "
                        "jetPhi, "
                        "lead_sublead_goodl1tau_eta, "
                        "lead_sublead_goodl1tau_phi, "
                        "0.5) "
                    "&& jetBx == 0"
                "],"
                "jetPhi["
                    "maskDeltaR("
                        "jetEta, "
                        "jetPhi, "
                        "lead_sublead_goodl1tau_eta, "
                        "lead_sublead_goodl1tau_phi, "
                        "0.5) "
                    "&& jetBx == 0"
                "],"
                "jetEt["
                    "maskDeltaR("
                        "jetEta, "
                        "jetPhi, "
                        "lead_sublead_goodl1tau_eta, "
                        "lead_sublead_goodl1tau_phi, "
                        "0.5) "
                    "&& jetBx == 0"
                "]"
            ")[0]")
        
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
