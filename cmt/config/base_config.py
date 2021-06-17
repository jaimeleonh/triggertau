from analysis_tools import ObjectCollection, Category, Process, Dataset
from plotting_tools import Label

class Config():
    
    def __init__(self, name, year, ecm, lumi, **kwargs):
        self.name=name
        self.year=year
        self.ecm=ecm
        self.lumi=lumi
        self.x = kwargs

        self.categories = self.add_categories()
        self.processes = self.add_processes()
        self.datasets = self.add_datasets()
        self.versions = self.add_versions()

    def add_categories(self):
        categories = [
            Category("base", Label(root="base category")),
            Category("bbtt", Label(root="HH#to bb#tau#tau"),
                selection="Jet_pt[Jet_pt >= 20 && abs(Jet_eta) <= 4.7 && Jet_jetId >= 2 && ((Jet_puId >= 4 && Jet_pt <= 50) || (Jet_pt > 50))].size() >= 2", 
                nminjets=2, nmaxjets=999, add_to_jet_pt=0, add_to_leading_pt=18, add_to_subleading_pt=8),
            # H->tautau selection, extracted from cms.cern.ch/iCMS/jsp/openfile.jsp?tp=draft&files=AN2019_109_v17.pdf, L719
            Category("htt_0jet", Label(root=" 0 jet cat."),
                selection="Jet_pt[Jet_pt >= 30 && abs(Jet_eta) <= 4.7 && Jet_jetId >= 2 && ((Jet_puId >= 4 && Jet_pt <= 50) || (Jet_pt > 50))].size() >= 0", 
                nminjets=0, nmaxjets=0, add_to_jet_pt=10, add_to_leading_pt=8, add_to_subleading_pt=8),
            Category("htt_1jet", Label(root="1 jet cat."),
                selection="Jet_pt[Jet_pt >= 30 && abs(Jet_eta) <= 4.7 && Jet_jetId >= 2 && ((Jet_puId >= 4 && Jet_pt <= 50) || (Jet_pt > 50))].size() >= 1", 
                nminjets=1, nmaxjets=1, add_to_jet_pt=10, add_to_leading_pt=8, add_to_subleading_pt=8),
            Category("htt_1jet_highPt", Label(root="1 jet, High pt cat.,"),
                selection="Jet_pt[Jet_pt >= 70 && abs(Jet_eta) <= 4.7 && Jet_jetId >= 2 && ((Jet_puId >= 4 && Jet_pt <= 50) || (Jet_pt > 50))].size() >= 1", 
                nminjets=1, nmaxjets=1, add_to_jet_pt=50, add_to_leading_pt=8, add_to_subleading_pt=8),
            Category("htt_2jet", Label(root="2 jet cat."),
                selection="Jet_pt[Jet_pt >= 30 && abs(Jet_eta) <= 4.7 && Jet_jetId >= 2 && ((Jet_puId >= 4 && Jet_pt <= 50) || (Jet_pt > 50))].size() >= 2", 
                nminjets=2, nmaxjets=999, add_to_jet_pt=0, add_to_leading_pt=8, add_to_subleading_pt=8)
        ]
        return ObjectCollection(categories)

    def add_processes(self):
        processes = [
            Process("ggf_lo", Label(root="HH #rightarrow bb#tau#tau, ggHH SM LO"), color=(0, 0, 0)),
            Process("ggf_sm", Label(root="HH #rightarrow bb#tau#tau, ggHH SM"), color=(0, 0, 0)),
            Process("vbf_sm", Label(root="HH #rightarrow bb#tau#tau, VBF SM"), color=(0, 0, 0)),
            Process("htautau_ggf", Label(root="H #rightarrow #tau#tau, ggH SM"), color=(0, 0, 0)),
            Process("htautau_vbf", Label(root="H #rightarrow #tau#tau, VBF SM"), color=(0, 0, 0)),
            Process("tt_fh", Label(root="t#bar{t}, FH"), color=(0, 0, 0)),
            Process("tt_dl", Label(root="t#bar{t}, DL"), color=(0, 0, 0)),
            Process("wjets", Label(root="W + Jets"), color=(0, 0, 0)),
            # rate computation
            Process("nu", Label(root="nu gun"), color=(255, 255, 255)),
            Process("zero_bias", Label(root="zero bias"), color=(255, 255, 255)),
            Process("run322599", Label(root="Run 322599"), color=(255, 255, 255)),
        ]
        return ObjectCollection(processes)

    def add_datasets(self):
        datasets = [
            # Dataset("ggf_sm",
                # "/store/mc/RunIIAutumn18NanoAODv7/GluGluToHHTo2B2Tau_node_cHHH1_TuneCP5_PSWeights_13TeV-powheg-pythia8"
                # "/NANOAODSIM/Nano02Apr2020_102X_upgrade2018_realistic_v21-v1/",
                # self.processes.get("ggf_sm"),
                # prefix="cms-xrd-global.cern.ch/",
                # locate="ingrid-se04.cism.ucl.ac.be:1094/"),
            Dataset("ggf_sm",
                "/eos/user/j/jleonhol/HH/ggf_2018_nanotest2/",
                self.processes.get("ggf_sm"),
               ),
            Dataset("vbf_sm",
                "/eos/user/j/jleonhol/HH/vbf_sm/",
                self.processes.get("vbf_sm")),
            Dataset("ggf_lo",
                "/eos/home-j/jleonhol/HH/ggf_lo/",
                self.processes.get("ggf_lo")),
            Dataset("htautau_ggf",
                "/eos/user/j/jleonhol/HH/htautau_ggf/",
                self.processes.get("htautau_ggf")),
            Dataset("htautau_vbf",
                "/eos/user/j/jleonhol/HH/htautau_vbf/",
                self.processes.get("htautau_vbf")),
            Dataset("tt_dl",
                "/eos/user/j/jleonhol/HH/TTTo2L2Nu_TuneCP5_13TeV-powheg-pythia8",
                self.processes.get("tt_dl")),
            Dataset("wjets",
                "/eos/user/j/jleonhol/HH/WJetsToLNu_TuneCP5_13TeV-madgraphMLM-pythia8",
                self.processes.get("wjets")),
            Dataset("tt_fh",
                "/eos/user/j/jleonhol/HH/TTToHadronic_TuneCP5_13TeV-powheg-pythia8",
                self.processes.get("tt_fh")),
            Dataset("nu",
                "/eos/cms/store/group/dpg_trigger/comm_trigger/L1Trigger/stempl/condor/menu_Nu_11_0_X_1614189426/",
                self.processes.get("nu"),
                skipFiles=[
                    "/eos/cms/store/group/dpg_trigger/comm_trigger/L1Trigger/stempl/condor/menu_Nu_11_0_X_1614189426//44.root"],
                rate_scaling=1.),
            Dataset("zero_bias",
                "/eos/cms/store/group/dpg_trigger/comm_trigger/L1Trigger/TEAshiftNtuples/ZeroBias2018D-week36-l1t-integration-v100p0-CMSSW-10_2_1/ZeroBias/",
                self.processes.get("zero_bias"),
                rate_scaling=2./1.587),
            Dataset("run322599",
                "/eos/home-j/jleonhol/HH/run322599/",
                self.processes.get("run322599"),
                rate_scaling=2./1.803),
        ]
        return ObjectCollection(datasets)
    
    def add_versions(self):
        versions = {}
        return versions


config = Config("base", year=2018, ecm=13, lumi=59741)
