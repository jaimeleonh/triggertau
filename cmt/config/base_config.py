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
            Category("bbtt", Label(root="HH#rightarrow bb#tau#tau"),
                selection=("Jet_pt[Jet_pt >= 20 && abs(Jet_eta) <= 4.7 && Jet_jetId >= 2 && "
                    "((Jet_puId >= 4 && Jet_pt <= 50) || (Jet_pt > 50))].size() >= 2"), 
                nminjets=2, nmaxjets=999, add_to_jet_pt=0,
                add_to_leading_pt=8, add_to_subleading_pt=8),
            # H->tautau selection, extracted from cms.cern.ch/iCMS/jsp/openfile.jsp?tp=draft&files=AN2019_109_v17.pdf, L719
            Category("htt_0jet", Label(root=" 0 jet cat."),
                #selection="Jet_pt[Jet_pt >= 30 && abs(Jet_eta) <= 4.7 && Jet_jetId >= 2 && ((Jet_puId >= 4 && Jet_pt <= 50) || (Jet_pt > 50))].size() >= 0",
                nminjets=0, nmaxjets=0, add_to_jet_pt=10,
                add_to_leading_pt=18, add_to_subleading_pt=8),
            Category("htt_1jet", Label(root="1 jet cat."),
                selection=("Jet_pt[Jet_pt >= 30 && abs(Jet_eta) <= 4.7 && Jet_jetId >= 2 && "
                    "((Jet_puId >= 4 && Jet_pt <= 50) || (Jet_pt > 50))].size() >= 1"), 
                nminjets=1, nmaxjets=1, add_to_jet_pt=10,
                add_to_leading_pt=18, add_to_subleading_pt=8),
            Category("htt_1jet_highPt", Label(root="1 jet, High pt cat."),
                parent_category="htt_1jet",
                selection=("Jet_pt[Jet_pt >= 70 && abs(Jet_eta) <= 4.7 && Jet_jetId >= 2 && "
                    "((Jet_puId >= 4 && Jet_pt <= 50) || (Jet_pt > 50))].size() >= 1"), 
                nminjets=1, nmaxjets=1, add_to_jet_pt=50,
                add_to_leading_pt=18, add_to_subleading_pt=8),
            Category("htt_2jet", Label(root="2 jet cat."),
                selection=("Jet_pt[Jet_pt >= 30 && abs(Jet_eta) <= 4.7 && Jet_jetId >= 2 && "
                    "((Jet_puId >= 4 && Jet_pt <= 50) || (Jet_pt > 50))].size() >= 2"), 
                nminjets=2, nmaxjets=999, add_to_jet_pt=10,
                add_to_leading_pt=18, add_to_subleading_pt=8)
        ]
        return ObjectCollection(categories)

    def add_processes(self):
        processes = [
            Process("ggf_lo", Label(root="ggHH SM LO"), color=(0, 0, 0)),
            Process("ggf_sm", Label(root="ggHH SM"), color=(0, 0, 0)),
            Process("vbf_sm", Label(root="VBFHH SM"), color=(0, 0, 0)),
            Process("htautau_ggf", Label(root="H #rightarrow #tau#tau, ggH SM"), color=(0, 0, 0)),
            Process("htautau_vbf", Label(root="H #rightarrow #tau#tau, VBF SM"), color=(0, 0, 0)),
            Process("tt_fh", Label(root="t#bar{t}, FH"), color=(0, 0, 0)),
            Process("tt_dl", Label(root="t#bar{t}, DL"), color=(0, 0, 0)),
            Process("wjets", Label(root="W + Jets"), color=(0, 0, 0)),
            Process("dy", Label(root="DY"), color=(0, 0, 0)),
            # rate computation
            Process("nu", Label(root="Run 3 #nu gun"), color=(255, 255, 255)),
            Process("zero_bias", Label(root="zero bias"), color=(255, 255, 255)),
            Process("run2_zero_bias", Label(root="Run 2 - Zero Bias"), color=(255, 255, 255)),
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
                self.processes.get("tt_fh"),
                skipFiles=[
                    "/eos/user/j/jleonhol/HH/TTToHadronic_TuneCP5_13TeV-powheg-pythia8/NANO_NANO_3-7.root"
                ]),
            Dataset("dy",
                "/eos/user/j/jleonhol/HH/DYJetsToLL_M-50_TuneCP5_13TeV-madgraphMLM-pythia8",
                self.processes.get("dy")),
            Dataset("nu",
                # old path
                # "/eos/cms/store/group/dpg_trigger/comm_trigger/L1Trigger/stempl/condor/menu_Nu_11_0_X_1614189426/",
                # new path after July 2nd, 2021
                "/eos/cms/store/group/dpg_trigger/comm_trigger/L1Trigger/bundocka/condor/reHcalTP_Nu_11_2_105p20p1_1623921599",
                self.processes.get("nu"),
                # old files to skip
                # skipFiles=[
                #     "/eos/cms/store/group/dpg_trigger/comm_trigger/L1Trigger/stempl/condor/menu_Nu_11_0_X_1614189426//44.root"],
                skipFiles=[
                    "/eos/cms/store/group/dpg_trigger/comm_trigger/L1Trigger/bundocka/condor/reHcalTP_Nu_11_2_105p20p1_1623921599/{}.root".format(i)
                    for i in [299, 334, 427, 570, 1104, 1236, 1402, 1754, 1921, 2032, 2033, 2034,
                    2035, 2036, 2037, 2038, 2039, 2040, 2041, 2042, 2043, 2044, 2045, 2046, 2047,
                    2048, 2049, 2050, 2051, 2052, 2053, 2054, 2055, 2056, 2057, 2058, 2059, 2060,
                    2061, 2062, 2063, 2064, 2065, 2066, 2067, 2068, 2069, 2070, 2071, 2072, 2073,
                    2074, 2075, 2076, 2077, 2078, 2079, 2080, 2081, 2082, 2083, 2084, 2085, 2086,
                    2087, 2088, 2089, 2090, 2091, 2092, 2093, 2094, 2095, 2096, 2097, 2098, 2099,
                    2100, 2101, 2102, 2103, 2104, 2105, 2106, 2107, 2108, 2109, 2110, 2111, 2112,
                    2113, 2114, 2115, 2116, 2117, 2118, 2119, 2120, 2121, 2122, 2123, 2124, 2125,
                    2126, 2127, 2128, 2129, 2130, 2131, 2132, 2133, 2134, 2135, 2136, 2137, 2138,
                    2139, 2140, 2141, 2142, 2143, 2144, 2145, 2146, 2147, 2148, 2149, 2150, 2151,
                    2152, 2153, 2154, 2155, 2156, 2157, 2158, 2159, 2160, 2161, 2162, 2163, 2164,
                    2165, 2166, 2167, 2168, 2169, 2170, 2171, 2172, 2173, 2174, 2175, 2176, 2177,
                    2178, 2179, 2180, 2181, 2182, 2183, 2184, 2185, 2186, 2187, 2188, 2189, 2190,
                    2191, 2192, 2193, 2194, 2195, 2196, 2197, 2198, 2199, 2200, 2201, 2202, 2203,
                    2204, 2205, 2206, 2207, 2208, 2209, 2210, 2211, 2212, 2213, 2214, 2215, 2216,
                    2217, 2218, 2219, 2220, 2221, 2222, 2223, 2224, 2225, 2226, 2227, 2228, 2229,
                    2230, 2231, 2232, 2233, 2234, 2235, 2236, 2237, 2238, 2239, 2240, 2241, 2242,
                    2243, 2244, 2245, 2246, 2247, 2248, 2249, 2250, 2251, 2252, 2253, 2254, 2255,
                    2256, 2257, 2258, 2259, 2260, 2261, 2262, 2263, 2264, 2265, 2266, 2267, 2268,
                    2269, 2270, 2271, 2272, 2273, 2274, 2275, 2276, 2277, 2278, 2279, 2280, 2281,
                    2282, 2283, 2284, 2285, 2286, 2287, 2288, 2289, 2290, 2291, 2292, 2293, 2294,
                    2295, 2296, 2297, 2298, 2299, 2300, 2301, 2302, 2303, 2304, 2305, 2306, 2307,
                    2308, 2309, 2310, 2311, 2312, 2313, 2314, 2315, 2316, 2317, 2318, 2319, 2320,
                    2321, 2322, 2323, 2324, 2325, 2326, 2327, 2328, 2329, 2330, 2331, 2332, 2333,
                    2334, 2335, 2336, 2337, 2338, 2339, 2340, 2341, 2342, 2343, 2344, 2345, 2346,
                    2347, 2348, 2349, 2350, 2351, 2352, 2353, 2354, 2355, 2356, 2357, 2358, 2359,
                    2360, 2361, 2362, 2363, 2364, 2365, 2366, 2367, 2368, 2369, 2370, 2371, 2372,
                    2373, 2374, 2375, 2376, 2377, 2378, 2379, 2380, 2381, 2382, 2383, 2384, 2385,
                    2386, 2387, 2388, 2389, 2390, 2391, 2392, 2393, 2394, 2395, 2396, 2397, 2398,
                    2399, 2400, 2401, 2402, 2403, 2404, 2405, 2406, 2407, 2408, 2409, 2410, 2411,
                    2412, 2413, 2414, 2415, 2416, 2417, 2418, 2419, 2420, 2421, 2422, 2423, 2424,
                    2425, 2426, 2427, 2428, 2429, 2430, 2431, 2432, 2433, 2434, 2435, 2436, 2437,
                    2438, 2439, 2440, 2441, 2442, 2443, 2444, 2445, 2446, 2447, 2448, 2449, 2450,
                    2451, 2452, 2453, 2454, 2455, 2456, 2457, 2458, 2459, 2460, 2461, 2462, 2463,
                    2464, 2465, 2466, 2467, 2468, 2469, 2470, 2471, 2472, 2473, 2474, 2475, 2476,
                    2477, 2478, 2479, 2480, 2481, 2482, 2483, 2484, 2485, 2486, 2487, 2488, 2489,
                    2490, 2491, 2492, 2493, 2494, 2495, 2496, 2497, 2498]],
                label="Run 3 MC",
                rate_scaling=1.,
                treename="l1UpgradeEmuTree/L1UpgradeTree"),
            Dataset("zero_bias",
                "/eos/cms/store/group/dpg_trigger/comm_trigger/L1Trigger/TEAshiftNtuples/ZeroBias2018D-week36-l1t-integration-v100p0-CMSSW-10_2_1/ZeroBias/",
                self.processes.get("zero_bias"),
                label="zero bias 2018D",
                rate_scaling=2. / 1.587),
            Dataset("run322599",
                "/eos/home-j/jleonhol/HH/run322599/",
                self.processes.get("run2_zero_bias"),
                label="322599",
                rate_scaling=2. / 1.803),
            Dataset("run323755",
                "/eos/home-j/jleonhol/HH/run323755/",
                self.processes.get("run2_zero_bias"),
                label="323755",
                rate_scaling=2. / 1.764),
            Dataset("run322079",
                "/eos/cms/store/group/dpg_trigger/comm_trigger/L1Trigger/TEAshiftNtuples/ZeroBias2018D-week36-l1t-integration-v100p0-CMSSW-10_2_1/ZeroBias/"
                "crab_ZeroBias2018D-week36-l1t-integration-v100p0-CMSSW-10_2_1__322079_ZeroBias_Run2018D-v1/180908_184351/0000/",
                self.processes.get("run2_zero_bias"),
                label="322079",
                rate_scaling=2. / 1.8135),
        ]
        return ObjectCollection(datasets)
    
    def add_versions(self):
        versions = {}
        return versions


config = Config("base", year=2018, ecm=13, lumi=59741)
