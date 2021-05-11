from analysis_tools import ObjectCollection, Category, Process, Dataset

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
            Category("base", "base category"),
            Category("bbtt", "bbtautau",
                selection="Jet_pt[Jet_pt >= 20 && abs(Jet_eta) <= 4.7 && Jet_jetId >= 2 && ((Jet_puId >= 4 && Jet_pt <= 50) || (Jet_pt > 50))].size() >= 2", 
                nminjets=2, nmaxjets=999),
            # H->tautau selection, extracted from cms.cern.ch/iCMS/jsp/openfile.jsp?tp=draft&files=AN2019_109_v17.pdf, L719
            Category("htt_0jet", "htt_0jet",
                selection="Jet_pt[Jet_pt >= 30 && abs(Jet_eta) <= 4.7 && Jet_jetId >= 2 && ((Jet_puId >= 4 && Jet_pt <= 50) || (Jet_pt > 50))].size() >= 0", 
                nminjets=0, nmaxjets=0),
            Category("htt_1jet", "htt_1jet",
                selection="Jet_pt[Jet_pt >= 30 && abs(Jet_eta) <= 4.7 && Jet_jetId >= 2 && ((Jet_puId >= 4 && Jet_pt <= 50) || (Jet_pt > 50))].size() >= 1", 
                nminjets=1, nmaxjets=1),
            Category("htt_2jet", "htt_2jet",
                selection="Jet_pt[Jet_pt >= 30 && abs(Jet_eta) <= 4.7 && Jet_jetId >= 2 && ((Jet_puId >= 4 && Jet_pt <= 50) || (Jet_pt > 50))].size() >= 2", 
                nminjets=2, nmaxjets=999)
        ]
        return ObjectCollection(categories)

    def add_processes(self):
        processes = [
            Process("ggf_lo", "HH #rightarrow bb#tau#tau, ggHH SM LO", color=(0, 0, 0)),
            Process("ggf_sm", "HH #rightarrow bb#tau#tau, ggHH SM", color=(0, 0, 0)),
            Process("htautau_ggf", "H #rightarrow #tau#tau, ggH SM", color=(0, 0, 0)),
            Process("data_tau", "DATA_TAU", color=(255, 255, 255)),
            Process("nu", "nu gun", color=(255, 255, 255)),
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
                add_to_leading_pt=8,
                add_to_subleading_pt=8,
                add_to_jet_pt=0),
            Dataset("ggf_lo",
                "/eos/home-j/jleonhol/HH/ggf_lo/",
                self.processes.get("ggf_lo"),
                add_to_leading_pt=8,
                add_to_subleading_pt=8,
                add_to_jet_pt=0),
            Dataset("htautau_ggf",
                "/eos/user/j/jleonhol/HH/htautau_ggf/",
                self.processes.get("htautau_ggf"),
                add_to_leading_pt=18,
                add_to_subleading_pt=8,
                add_to_jet_pt=10),
            Dataset("data_dum",
                "/store/data/Run2018A/Tau/NANOAOD/02Apr2020-v1/",
                self.processes.get("data_tau"),
                isData=True,
                runPeriod="A",
                prefix="cms-xrd-global.cern.ch/",
                locate="grid-dcache.physik.rwth-aachen.de:1094/"),
            Dataset("nu",
                "/eos/cms/store/group/dpg_trigger/comm_trigger/L1Trigger/stempl/condor/menu_Nu_11_0_X_1614189426/",
                self.processes.get("nu"),
                skipFiles=[
                    "/eos/cms/store/group/dpg_trigger/comm_trigger/L1Trigger/stempl/condor/menu_Nu_11_0_X_1614189426//44.root"])
        ]
        return ObjectCollection(datasets)
    
    def add_versions(self):
        versions = {}
        return versions


config = Config("base", year=2018, ecm=13, lumi=59741)
