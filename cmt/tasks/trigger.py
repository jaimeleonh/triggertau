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

    xx_range = (20, 40)
    yy_range = (20, 40)
    zz_range = (20, 150)

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
    
    def add_to_root(self, root):
        root.gInterpreter.Declare("""
            using Vfloat = const ROOT::RVec<float>&;      
            ROOT::RVec<float> lead_sublead_pt(Vfloat pt){
                ROOT::RVec<float> leading_pts;
                leading_pts.push_back(-1.);
                leading_pts.push_back(-1.);
                for (size_t i = 0; i < pt.size(); i++) {
                    if (pt[i] > leading_pts[0]) {
                        leading_pts[1] = leading_pts[0];
                        leading_pts[0] = pt[i];
                    } 
                    else if (pt[i] > leading_pts[1])
                        leading_pts[1] = pt[i];
                }
                return leading_pts;
            }
        """)
        return root

    def get_new_dataframe(self, input_file, output_file):
        from analysis_tools.utils import (
            import_root, create_file_dir, join_root_selection
        )
        import itertools
        ROOT = import_root()
        ROOT = self.add_to_root(ROOT)
        # ROOT.ROOT.EnableImplicitMT()
        
        df = ROOT.RDataFrame(self.tree_name, input_file)
        
        if self.category.selection:
            df = df.Filter(self.category.selection)

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

        ROOT.gInterpreter.Declare("""
            #include <TLorentzVector.h>
            using Vfloat = const ROOT::RVec<float>&;      
            ROOT::RVec<bool> maskDeltaR(Vfloat pt1, Vfloat eta1, Vfloat phi1, Vfloat mass1,
                    Vfloat pt2, Vfloat eta2, Vfloat phi2, Vfloat mass2, float th_dr) {
                ROOT::RVec<bool> mask;
                for (size_t i = 0; i < pt1.size(); i++){
                    TLorentzVector v1;
                    v1.SetPtEtaPhiM(pt1[i], eta1[i], phi1[i], mass1[i]);
                    bool bigger_deltar = true;
                    for (size_t j = 0; j < pt2.size(); j++){
                        TLorentzVector v2;
                        v2.SetPtEtaPhiM(pt2[j], eta2[j], phi2[j], mass2[j]);
                        float this_deltar = fabs(v1.DeltaR(v2));
                        if (abs(this_deltar) < th_dr) bigger_deltar = false;
                    }
                    mask.push_back(bigger_deltar);
                }
                return mask;
            }
        """)

        for yy, zz in itertools.product(range(*self.yy_range), range(*self.zz_range)):
            # Using L1 Taus and Jets
            # name = "DoubleIsoTau{}er2p1Jet{}".format(yy, zz)
            goodL1Tau = join_root_selection(["L1Obj_type == 1", "abs(L1Obj_eta) <= 2.1",
                "L1Obj_iso == 1", "L1Obj_pt >= {0}".format(yy)], op="and")
            # df = df.Define(name,
                # "(L1Obj_pt[{0}].size() >= 2) && "
                # "(L1Obj_pt[L1Obj_type == 0 "
                    # "&& L1Obj_pt >= {1}].size() >= 1)".format(goodL1Tau, zz))
            # branch_names.append(name)
            name = "DoubleIsoTau{}er2p1Jet{}dR0p5".format(yy, zz)
            df = df.Define(name,
                "(L1Obj_pt[{0}].size() >= 2) && "
                "(L1Obj_pt["
                    "L1Obj_type == 0 "
                    "&& L1Obj_pt >= {1} "
                    "&& maskDeltaR("
                        "L1Obj_pt, "
                        "L1Obj_eta, "
                        "L1Obj_phi, "
                        "L1Obj_pt, " # dum, not needed for deltaR, just for LV
                        "L1Obj_pt[{0}], "
                        "L1Obj_eta[{0}], "
                        "L1Obj_phi[{0}], "
                        "L1Obj_pt[{0}], " # dum
                        "0.5)"
                    "].size() >= 1)".format(goodL1Tau, zz))
            branch_names.append(name)

        # df = df.Define("nTrigTau", "TrigObj_pt[TrigObj_id == 15].size()")
        # df = df.Define("TrigTau_pt", "TrigObj_pt[TrigObj_id == 15]")
        # df = df.Define("TrigTau_eta", "TrigObj_eta[TrigObj_id == 15]")
        # df = df.Define("TrigTau_phi", "TrigObj_phi[TrigObj_id == 15]")

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
        #"nTrigTau",
        #"TrigTau_pt", "TrigTau_phi", "TrigTau_eta"
        #"nL1Obj", "L1Obj_pt", "L1Obj_eta", "L1Obj_phi", "L1Obj_type",
    ]

    def get_new_dataframe(self, input_file, output_file):
        from analysis_tools.utils import (
            import_root, create_file_dir, join_root_selection
        )
        import itertools
        ROOT = import_root()
        ROOT = self.add_to_root(ROOT)

        df = ROOT.RDataFrame(self.tree_name, input_file)

        if self.category.selection:
            df = df.Filter(self.category.selection)

        branch_names = []
        for xx in range(*self.xx_range):
            name = "DoubleIsoTau{}er2p1".format(xx)
            df = df.Define(name, 
                "lead_sublead_pt(Tau_pt[abs(Tau_eta) <= 2.1])[0] >= ({0} + {1}) "
                "&& lead_sublead_pt(Tau_pt[abs(Tau_eta) <= 2.1])[1] >= ({0} + {2})".format(
                    xx, 
                    self.dataset.get_aux("add_to_leading_pt"),
                    self.dataset.get_aux("add_to_subleading_pt")
                )
            )
            branch_names.append(name)

        ROOT.gInterpreter.Declare("""
            #include <TLorentzVector.h>
            using Vfloat = const ROOT::RVec<float>&;      
            ROOT::RVec<bool> maskDeltaR(Vfloat pt1, Vfloat eta1, Vfloat phi1, Vfloat mass1,
                    Vfloat pt2, Vfloat eta2, Vfloat phi2, Vfloat mass2, float th_dr) {
                ROOT::RVec<bool> mask;
                for (size_t i = 0; i < pt1.size(); i++){
                    TLorentzVector v1;
                    v1.SetPtEtaPhiM(pt1[i], eta1[i], phi1[i], mass1[i]);
                    bool bigger_deltar = true;
                    for (size_t j = 0; j < pt2.size(); j++){
                        TLorentzVector v2;
                        v2.SetPtEtaPhiM(pt2[j], eta2[j], phi2[j], mass2[j]);
                        float this_deltar = fabs(v1.DeltaR(v2));
                        if (abs(this_deltar) < th_dr) bigger_deltar = false;
                    }
                    mask.push_back(bigger_deltar);
                }
                return mask;
            }
        """)

        for yy, zz in itertools.product(range(*self.yy_range), range(*self.zz_range)):
            # Using Reco Taus and Jets
            name = "DoubleIsoTau{}er2p1Jet{}dR0p5".format(yy, zz)
            df = df.Define(name,
                "(lead_sublead_pt(Tau_pt[abs(Tau_eta) <= 2.1])[0] >= ({0} + {1}) "
                "&& lead_sublead_pt(Tau_pt[abs(Tau_eta) <= 2.1])[1] >= ({0} + {2})) && "
                "(Jet_pt["
                    "(Jet_pt >= ({3} + {4})) && abs(Jet_eta) <= 4.7 && Jet_jetId == 2 "
                    "&& ((Jet_puId >= 4 && Jet_pt <= 50) || (Jet_pt > 50))"
                    "&& maskDeltaR("
                        "Jet_pt, "
                        "Jet_eta, "
                        "Jet_phi, "
                        "Jet_pt, " # dum, not needed for deltaR, just for LV
                        "Tau_pt[abs(Tau_eta) <= 2.1 && Tau_pt >= ({0} + {2})], "
                        "Tau_eta[abs(Tau_eta) <= 2.1 && Tau_pt >= ({0} + {2})], "
                        "Tau_phi[abs(Tau_eta) <= 2.1 && Tau_pt >= ({0} + {2})], "
                        "Tau_pt[abs(Tau_eta) <= 2.1 && Tau_pt >= ({0} + {2})], " # dum
                        "0.5)"
                    "].size() >= 1)".format(
                        yy, 
                        self.dataset.get_aux("add_to_leading_pt"),
                        self.dataset.get_aux("add_to_subleading_pt"),
                        zz,
                        self.dataset.get_aux("add_to_jet", 10))
                )
            branch_names.append(name)

        # df = df.Define("nTrigTau", "TrigObj_pt[TrigObj_id == 15].size()")
        # df = df.Define("TrigTau_pt", "TrigObj_pt[TrigObj_id == 15]")
        # df = df.Define("TrigTau_eta", "TrigObj_eta[TrigObj_id == 15]")
        # df = df.Define("TrigTau_phi", "TrigObj_phi[TrigObj_id == 15]")

        branch_list = ROOT.vector('string')()
        for branch_name in self.additional_branches + branch_names:
            branch_list.push_back(branch_name)

        df.Snapshot(self.tree_name, create_file_dir(output_file), branch_list)


class ComputeRate(AddTrigger):

    xx_range = (20, 40)
    yy_range = (20, 40)
    zz_range = (20, 150)

    additional_branches = [
        'nTaus', 'tauEt', 'tauEta', 'tauPhi', 'tauIso',
        'nJets', 'jetEt', 'jetEta', 'jetPhi'
    ]
    
    tree_name = "l1UpgradeEmuTree/L1UpgradeTree"

    def get_new_dataframe(self, input_file, output_file):
        from analysis_tools.utils import (
            import_root, create_file_dir, join_root_selection
        )
        import itertools
        ROOT = import_root()
        ROOT = self.add_to_root(ROOT)

        df = ROOT.RDataFrame(self.tree_name, input_file)

        if self.category.selection:
            df = df.Filter(self.category.selection)

        branch_names = []
        for xx in range(*self.xx_range):
            name = "DoubleIsoTau{}er2p1".format(xx)
            df = df.Define(name, 
                "(tauEt["
                    "abs(tauEta) <= 2.1 "
                    "&& tauIso == 1 "
                    "&& tauEt >= {}"
                "].size() >= 2)".format(xx)
            )
            branch_names.append(name)

        ROOT.gInterpreter.Declare("""
            #include <TLorentzVector.h>
            using Vfloat = const ROOT::RVec<float>&;      
            ROOT::RVec<bool> maskDeltaR(Vfloat pt1, Vfloat eta1, Vfloat phi1, Vfloat mass1,
                    Vfloat pt2, Vfloat eta2, Vfloat phi2, Vfloat mass2, float th_dr) {
                ROOT::RVec<bool> mask;
                for (size_t i = 0; i < pt1.size(); i++){
                    TLorentzVector v1;
                    v1.SetPtEtaPhiM(pt1[i], eta1[i], phi1[i], mass1[i]);
                    bool bigger_deltar = true;
                    for (size_t j = 0; j < pt2.size(); j++){
                        TLorentzVector v2;
                        v2.SetPtEtaPhiM(pt2[j], eta2[j], phi2[j], mass2[j]);
                        float this_deltar = fabs(v1.DeltaR(v2));
                        if (abs(this_deltar) < th_dr) bigger_deltar = false;
                    }
                    mask.push_back(bigger_deltar);
                }
                return mask;
            }
        """)

        for yy, zz in itertools.product(range(*self.yy_range), range(*self.zz_range)):
            # Using L1 Taus and Jets
            goodTau = join_root_selection(["abs(tauEta) <= 2.1", "tauIso == 1", 
                "tauEt >= {0}".format(yy)], op="and")
            name = "DoubleIsoTau{}er2p1Jet{}".format(yy, zz)
            df = df.Define(name,
                "(tauEt[{0}].size() >= 2) && "
                "(jetEt["
                    "jetEt >= {1} "
                    "].size() >= 1)".format(goodTau, zz))
            branch_names.append(name)
            name = "DoubleIsoTau{}er2p1Jet{}dR0p5".format(yy, zz)
            df = df.Define(name,
                "(tauEt[{0}].size() >= 2) && "
                "(jetEt["
                    "jetEt >= {1} "
                    "&& maskDeltaR("
                        "jetEt, "
                        "jetEta, "
                        "jetPhi, "
                        "jetEt, " # dum, not needed for deltaR, just for LV
                        "tauEt[{0}], "
                        "tauEta[{0}], "
                        "tauPhi[{0}], "
                        "tauEt[{0}], " # dum
                        "0.5)"
                    "].size() >= 1)".format(goodTau, zz))
            branch_names.append(name)

        # df = df.Define("nTrigTau", "TrigObj_pt[TrigObj_id == 15].size()")
        # df = df.Define("TrigTau_pt", "TrigObj_pt[TrigObj_id == 15]")
        # df = df.Define("TrigTau_eta", "TrigObj_eta[TrigObj_id == 15]")
        # df = df.Define("TrigTau_phi", "TrigObj_phi[TrigObj_id == 15]")

        branch_list = ROOT.vector('string')()
        for branch_name in self.additional_branches + branch_names:
            branch_list.push_back(branch_name)

        df.Snapshot(self.tree_name, create_file_dir(output_file), branch_list)
