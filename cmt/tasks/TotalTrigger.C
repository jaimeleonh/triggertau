#define TotalTrigger_cxx
#include "TotalTrigger.h"
#include <TH2.h>
#include <TStyle.h>
#include <TCanvas.h>
#include <math.h>

void TotalTrigger::TotalLoop()
{
    if (fChain == 0) return;
    
    Long64_t nentries = fChain->GetEntriesFast();
    
    Long64_t nbytes = 0, nb = 0;
    
    
    int nx = maxx - minx;
    int ny = maxy - miny;
    int nz = maxz - minz;
    int ndz = maxdz - mindz;
    
    std::vector <TH3F*> histos;
    for (int x = 0; x < nx; x++) {
        for (int xp = x; xp < x + 1; xp++) {
            for (int y = 0; y < ny; y++) {
                for (int yp = y; yp < y + 1; yp++) {
                    for (int dy = 0; dy < ny; dy++) {
                        for (int dyp = dy; dyp < dy + 1; dyp++) {
                            histos.push_back(new TH3F(
                                ("histo_ditau_" + std::to_string(minx + x) + "_" + std::to_string(minx + xp)
                                    + "__ditau_" + std::to_string(miny + y) + "_" + std::to_string(miny + yp) + "_jet"
                                + "__ditau_" + std::to_string(miny + dy) + "_" + std::to_string(miny + dyp) + "_dijet").c_str(),
                            "; zz; dzz; dzzp", nz, minz, maxz, ndz, mindz, maxdz, ndz, mindz, maxdz));
                        }
                    }
                }
            }
        }
    }

    for (Long64_t jentry=0; jentry<nentries;jentry++) {                   
        Long64_t ientry = LoadTree(jentry);
        if (ientry < 0) break;
        nb = fChain->GetEntry(jentry);   nbytes += nb;
        int index = -1;
        for (int x = 0; x < nx; x++) {
            for (int xp = x; xp < x + 1; xp++) {
                for (int y = 0; y < ny; y++) {
                    for (int yp = y; yp < y + 1; yp++) {
                        for (int dy = 0; dy < ny; dy++) {
                            for (int dyp = dy; dyp < dy + 1; dyp++) {
                                index++;
                                for (int z = 0; z < nz; z++) {
                                    for (int dz = 0; dz < ndz; dz++) {
                                        for (int dzp = 0; dzp < dz + 1; dzp++) {
                                            if (// ditau
                                                (leading_l1tau_pt >= minx + x &&
                                                    subleading_l1tau_pt >= minx + xp &&
                                                    leading_tau_pt >= minx + x + add_to_leading_tau &&
                                                subleading_tau_pt >= minx + xp + add_to_subleading_tau) ||
                                                // ditaujet
                                                (leading_l1tau_pt >= miny + y &&
                                                    subleading_l1tau_pt >= miny + yp &&
                                                    leading_tau_pt >= miny + y + add_to_leading_tau &&
                                                    subleading_tau_pt >= miny + yp + add_to_subleading_tau &&
                                                    leading_l1jet_pt >= minz + z &&
                                                leading_jet_pt >= minz + z + add_to_jet) ||
                                                // ditaudijet
                                                (leading_l1tau_pt >= miny + dy &&
                                                    subleading_l1tau_pt >= miny + dyp &&
                                                    leading_tau_pt >= miny + dy + add_to_leading_tau &&
                                                    subleading_tau_pt >= miny + dyp + add_to_subleading_tau &&
                                                    leading_l1jet_pt >= mindz + dz &&
                                                    leading_jet_pt >= mindz + dz + add_to_jet &&
                                                    subleading_l1jet_pt >= mindz + dzp &&
                                                subleading_jet_pt >= mindz + dzp + add_to_jet))
                                                histos.at(index)->Fill(minz + z, mindz + dz, mindz + dzp);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    
    outfile->cd();
    outfile->Write();
    
}

void TotalTrigger::RateTotalLoop()
{
    
    if (fChain == 0) return;
    
    Long64_t nentries = fChain->GetEntriesFast();
    
    Long64_t nbytes = 0, nb = 0;
    
    int nx = maxx - minx;
    int ny = maxy - miny;
    int nz = maxz - minz;
    int ndz = maxdz - mindz;
    
    std::vector <TH3F*> histos;
    for (int x = 0; x < nx; x++) {
        for (int xp = x; xp < x + 1; xp++) {
            for (int y = 0; y < ny; y++) {
                for (int yp = y; yp < y + 1; yp++) {
                    for (int dy = 0; dy < ny; dy++) {
                        for (int dyp = dy; dyp < dy + 1; dyp++) {
                            histos.push_back(new TH3F(
                                ("histo_ditau_" + std::to_string(minx + x) + "_" + std::to_string(minx + xp)
                                    + "__ditau_" + std::to_string(miny + y) + "_" + std::to_string(miny + yp) + "_jet" // + std::to_string(20 + z) 
                                + "__ditau_" + std::to_string(miny + dy) + "_" + std::to_string(miny + dyp) + "_dijet").c_str(),
                            "; zz; dzz; dzzp", nz, minz, maxz, ndz, mindz, maxdz, ndz, mindz, maxdz));
                        }
                    }
                }
            }
        }
    }
    
    for (Long64_t jentry=0; jentry<nentries;jentry++) {
        // std::cout << jentry << std::endl;                                 
        Long64_t ientry = LoadTree(jentry);
        if (ientry < 0) break;
        nb = fChain->GetEntry(jentry);   nbytes += nb;
        int index = -1;
        for (int x = 0; x < nx; x++) {
            for (int xp = x; xp < x + 1; xp++) {
                for (int y = 0; y < ny; y++) {
                    for (int yp = y; yp < y + 1; yp++) {
                        for (int dy = 0; dy < ny; dy++) {
                            for (int dyp = dy; dyp < dy + 1; dyp++) {
                                index++;
                                for (int z = 0; z < nz; z++) {
                                    for (int dz = 0; dz < ndz; dz++) {
                                        for (int dzp = 0; dzp < dz + 1; dzp++) {
                                            if (// ditau
                                                (leading_l1tau_pt >= minx + x &&
                                                subleading_l1tau_pt >= minx + xp) ||
                                                // ditaujet
                                                (leading_l1tau_pt >= miny + y &&
                                                    subleading_l1tau_pt >= miny + yp &&
                                                leading_l1jet_pt >= minz + z) ||
                                                // ditaudijet
                                                (leading_l1tau_pt >= miny + dy &&
                                                    subleading_l1tau_pt >= miny + dyp &&
                                                    leading_l1jet_pt >= mindz + dz &&
                                                subleading_l1jet_pt >= mindz + dzp))
                                                histos.at(index)->Fill(minz + z, mindz + dz, mindz + dzp);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    
    outfile->cd();
    outfile->Write();
    
}


void TotalTrigger::AsymmLoop()
{
    if (fChain == 0) return;
    
    Long64_t nentries = fChain->GetEntriesFast();
    
    Long64_t nbytes = 0, nb = 0;
    
    
    int nx = maxx - minx;
    int ny = maxy - miny;
    int nz = maxz - minz;
    
    std::vector <TH1F*> histos;
    for (int x = 0; x < nx; x++) {
        for (int xp = 0; xp < x + 1; xp++) {
            for (int y = 0; y < ny; y++) {
                for (int yp = 0; yp < y + 1; yp++) {
                    histos.push_back(new TH1F(
                        ("histo_ditau_" + std::to_string(minx + x) + "_" + std::to_string(minx + xp)
                            + "__ditau_" + std::to_string(miny + y) + "_" + std::to_string(miny + yp) + "_jet").c_str(),
                    "; zz; Events", nz, minz, maxz));
                }
            }
        }
    }
    
    for (Long64_t jentry=0; jentry<nentries;jentry++) {                   
        Long64_t ientry = LoadTree(jentry);
        if (ientry < 0) break;
        nb = fChain->GetEntry(jentry);   nbytes += nb;
        int index = -1;
        for (int x = 0; x < nx; x++) {
            for (int xp = 0; xp < x + 1; xp++) {
                for (int y = 0; y < ny; y++) {
                    for (int yp = 0; yp < y + 1; yp++) {
                        index++;
                        for (int z = 0; z < nz; z++) {
                            if (// ditau
                                (leading_l1tau_pt >= minx + x &&
                                    subleading_l1tau_pt >= minx + xp &&
                                    leading_tau_pt >= minx + x + add_to_leading_tau &&
                                subleading_tau_pt >= minx + xp + add_to_subleading_tau) ||
                                // ditaujet
                                (leading_l1tau_pt >= miny + y &&
                                    subleading_l1tau_pt >= miny + yp &&
                                    leading_tau_pt >= miny + y + add_to_leading_tau &&
                                    subleading_tau_pt >= miny + yp + add_to_subleading_tau &&
                                    leading_l1jet_pt_map[miny + y] >= minz + z &&
                                leading_jet_pt_map[miny + y] >= minz + z + add_to_jet))
                                histos.at(index)->Fill(minz + z);
                        }
                    }
                }
            }
        }
    }
    
    outfile->cd();
    outfile->Write();
    
}


void TotalTrigger::AsymmTriTauLoop()
{
    if (fChain == 0) return;
    
    Long64_t nentries = fChain->GetEntriesFast();
    
    Long64_t nbytes = 0, nb = 0;
    
    
    int nx = maxx - minx;
    int ny = maxy - miny;
    int nz = maxz - minz;
    
    std::vector <TH1F*> histos;
    for (int x = 0; x < nx; x++) {
        for (int xp = 0; xp < x + 1; xp++) {
            for (int y = 0; y < ny; y++) {
                for (int yp = 0; yp < y + 1; yp++) {
                    for (int ytri = 0; ytri < ny; ytri++) {
                        histos.push_back(new TH1F(
                            ("histo_ditau_" + std::to_string(minx + x) + "_" + std::to_string(minx + xp)
                                + "__ditau_" + std::to_string(miny + y) + "_" + std::to_string(miny + yp)
                            + "__tritau_" + std::to_string(miny + ytri) + "_jet").c_str(),
                        "; zz; Events", nz, minz, maxz));
                    }
                }
            }
        }
    }
    
    for (Long64_t jentry=0; jentry<nentries;jentry++) {                   
        Long64_t ientry = LoadTree(jentry);
        if (ientry < 0) break;
        nb = fChain->GetEntry(jentry);   nbytes += nb;
        int index = -1;
        for (int x = 0; x < nx; x++) {
            for (int xp = 0; xp < x + 1; xp++) {
                for (int y = 0; y < ny; y++) {
                    for (int yp = 0; yp < y + 1; yp++) {
                        for (int ytri = 0; ytri < ny; ytri++) {
                            index++;
                            for (int z = 0; z < nz; z++) {
                                if (// ditau
                                    (leading_l1tau_pt >= minx + x &&
                                        subleading_l1tau_pt >= minx + xp &&
                                        leading_tau_pt >= minx + x + add_to_leading_tau &&
                                    subleading_tau_pt >= minx + xp + add_to_subleading_tau) ||
                                    // ditaujet
                                    (leading_l1tau_pt >= miny + y &&
                                        subleading_l1tau_pt >= miny + yp &&
                                        leading_tau_pt >= miny + y + add_to_leading_tau &&
                                        subleading_tau_pt >= miny + yp + add_to_subleading_tau &&
                                        leading_l1jet_pt_map[miny + y] >= minz + z &&
                                    leading_jet_pt_map[miny + y] >= minz + z + add_to_jet) ||
                                    // tritau
                                    // we assume we misidentified the L1 jet as a tau, so we apply the requirements
                                    // on the subsubleading L1 tau and the leading offline jet
                                    (leading_l1tau_pt >= miny + ytri &&
                                        subleading_l1tau_pt >= miny + ytri &&
                                        subsubleading_l1tau_pt >= miny + ytri &&
                                        leading_tau_pt >= miny + ytri + add_to_subleading_tau &&
                                        subleading_tau_pt >= miny + ytri + add_to_subleading_tau &&
                                    leading_jet_pt_map[miny + y] >= miny + ytri + add_to_jet))
                                    histos.at(index)->Fill(minz + z);
                            }
                        }
                    }
                }
            }
        }
    }
    
    outfile->cd();
    outfile->Write();
    
}





void TotalTrigger::AsymmVBFLoop()
{
    if (fChain == 0) return;
    
    Long64_t nentries = fChain->GetEntriesFast();
    
    Long64_t nbytes = 0, nb = 0;
    
    
    int nx = maxx - minx;
    int ny = maxy - miny;
    int nz = maxz - minz;
    
    std::vector <TH1F*> histos;
    for (int x = 0; x < nx; x++) {
        for (int xp = 0; xp < x + 1; xp++) {
            for (int y = 0; y < ny; y++) {
                for (int yp = 0; yp < y + 1; yp++) {
                    histos.push_back(new TH1F(
                        ("histo_ditau_" + std::to_string(minx + x) + "_" + std::to_string(minx + xp)
                        + "__ditau_" + std::to_string(miny + y) + "_" + std::to_string(miny + yp) + "_jet").c_str(),
                    "; zz; Events", nz, minz, maxz));
                }
            }
        }
    }
    
    for (Long64_t jentry=0; jentry<nentries;jentry++) {                   
        Long64_t ientry = LoadTree(jentry);
        if (ientry < 0) break;
        nb = fChain->GetEntry(jentry);   nbytes += nb;
        int index = -1;
        // VBF trigger 90/100, 30/40, 620/700
        bool vbftrigger = (leading_l1jet_pt >= 90 &&
            subleading_l1jet_pt >= 30 &&
            leading_jet_pt >= 90 + add_to_jet &&
            subleading_jet_pt >= 30 + add_to_jet &&
            sqrt(2 * leading_l1jet_pt * subleading_l1jet_pt * (cosh(leading_l1jet_eta - subleading_l1jet_eta) - cos(leading_l1jet_phi - subleading_l1jet_phi))) >= 620 &&
        sqrt(2 * (leading_jet_pt + add_to_jet) * (subleading_jet_pt + add_to_jet) * (cosh(leading_jet_eta - subleading_jet_eta) - cos(leading_jet_phi - subleading_jet_phi))) >= 700);
        for (int x = 0; x < nx; x++) {
            for (int xp = 0; xp < x + 1; xp++) {
                for (int y = 0; y < ny; y++) {
                    for (int yp = 0; yp < y + 1; yp++) {
                        index++;
                        for (int z = 0; z < nz; z++) {
                            if (// ditau
                                (leading_l1tau_pt >= minx + x &&
                                    subleading_l1tau_pt >= minx + xp &&
                                    leading_tau_pt >= minx + x + add_to_leading_tau &&
                                subleading_tau_pt >= minx + xp + add_to_subleading_tau) ||
                                // ditaujet
                                (leading_l1tau_pt >= miny + y &&
                                    subleading_l1tau_pt >= miny + yp &&
                                    leading_tau_pt >= miny + y + add_to_leading_tau &&
                                    subleading_tau_pt >= miny + yp + add_to_subleading_tau &&
                                    leading_l1jet_pt >= minz + z &&
                                leading_jet_pt >= minz + z + add_to_jet) ||
                                // VBF trigger 90/100, 30/40, 620/700
                            vbftrigger)
                            histos.at(index)->Fill(minz + z);
                        }
                    }
                }
            }
        }
    }
    
    outfile->cd();
    outfile->Write();
    
}


void TotalTrigger::AsymmKetiLoop()
{
    if (fChain == 0) return;
    
    Long64_t nentries = fChain->GetEntriesFast();
    
    Long64_t nbytes = 0, nb = 0;
    
    
    int nx = maxx - minx;
    int ny = maxy - miny;
    int nz = maxz - minz;
    
    std::vector <TH1F*> histos;
    for (int x = 0; x < nx; x++) {
        for (int xp = 0; xp < x + 1; xp++) {
            for (int y = 0; y < ny; y++) {
                for (int yp = 0; yp < y + 1; yp++) {
                    histos.push_back(new TH1F(
                        ("histo_ditau_" + std::to_string(minx + x) + "_" + std::to_string(minx + xp)
                        + "__ditau_" + std::to_string(miny + y) + "_" + std::to_string(miny + yp) + "_jet").c_str(),
                    "; zz; Events", nz, minz, maxz));
                }
            }
        }
    }
    
    for (Long64_t jentry=0; jentry<nentries;jentry++) {                   
        Long64_t ientry = LoadTree(jentry);
        if (ientry < 0) break;
        nb = fChain->GetEntry(jentry);   nbytes += nb;
        int index = -1;
        // VBF trigger 90/100, 30/40, 620/700
        bool vbftrigger = (leading_l1jet_pt >= 35 &&
            subleading_l1jet_pt >= 35 &&
            leading_jet_pt >= 35 + add_to_jet &&
            subleading_jet_pt >= 35 + add_to_jet &&
            sqrt(2 * leading_l1jet_pt * subleading_l1jet_pt * (cosh(leading_l1jet_eta - subleading_l1jet_eta) - cos(leading_l1jet_phi - subleading_l1jet_phi))) >= 450 &&
            sqrt(2 * (leading_jet_pt + add_to_jet) * (subleading_jet_pt + add_to_jet) * (cosh(leading_jet_eta - subleading_jet_eta) - cos(leading_jet_phi - subleading_jet_phi))) >= 550 &&
            leading_l1tau_pt >= 45 &&
        leading_tau_pt >= 45 + add_to_leading_tau);
        for (int x = 0; x < nx; x++) {
            for (int xp = 0; xp < x + 1; xp++) {
                for (int y = 0; y < ny; y++) {
                    for (int yp = 0; yp < y + 1; yp++) {
                        index++;
                        for (int z = 0; z < nz; z++) {
                            if (// ditau
                                (leading_l1tau_pt >= minx + x &&
                                    subleading_l1tau_pt >= minx + xp &&
                                    leading_tau_pt >= minx + x + add_to_leading_tau &&
                                subleading_tau_pt >= minx + xp + add_to_subleading_tau) ||
                                // ditaujet
                                (leading_l1tau_pt >= miny + y &&
                                    subleading_l1tau_pt >= miny + yp &&
                                    leading_tau_pt >= miny + y + add_to_leading_tau &&
                                    subleading_tau_pt >= miny + yp + add_to_subleading_tau &&
                                    leading_l1jet_pt >= minz + z &&
                                leading_jet_pt >= minz + z + add_to_jet) ||
                                // VBF trigger 90/100, 30/40, 620/700
                            vbftrigger)
                            histos.at(index)->Fill(minz + z);
                        }
                    }
                }
            }
        }
    }
    
    outfile->cd();
    outfile->Write();
    
}




void TotalTrigger::RateAsymmLoop()
{
    if (fChain == 0) return;
    
    Long64_t nentries = fChain->GetEntriesFast();
    
    Long64_t nbytes = 0, nb = 0;
    
    
    int nx = maxx - minx;
    int ny = maxy - miny;
    int nz = maxz - minz;
    
    std::vector <TH1F*> histos;
    for (int x = 0; x < nx; x++) {
        for (int xp = 0; xp < x + 1; xp++) {
            for (int y = 0; y < ny; y++) {
                for (int yp = 0; yp < y + 1; yp++) {
                    histos.push_back(new TH1F(
                        ("histo_ditau_" + std::to_string(minx + x) + "_" + std::to_string(minx + xp)
                        + "__ditau_" + std::to_string(miny + y) + "_" + std::to_string(miny + yp) + "_jet").c_str(),
                    "; zz; Events", nz, minz, maxz));
                }
            }
        }
    }
    
    std::vector <TH1F*> histos_ditaujet;
    for (int y = 0; y < ny; y++) {
        for (int yp = 0; yp < y + 1; yp++) {
            histos_ditaujet.push_back(new TH1F(
                ("histo_ditau_" + std::to_string(miny + y) + "_" + std::to_string(miny + yp) + "_jet").c_str(),
            "; zz; Events", nz, minz, maxz));
        }
    }  
    
    for (Long64_t jentry=0; jentry<nentries;jentry++) {                   
        Long64_t ientry = LoadTree(jentry);
        if (ientry < 0) break;
        nb = fChain->GetEntry(jentry);   nbytes += nb;
        int index = -1;
        for (int x = 0; x < nx; x++) {
            for (int xp = 0; xp < x + 1; xp++) {
                for (int y = 0; y < ny; y++) {
                    for (int yp = 0; yp < y + 1; yp++) {
                        index++;
                        for (int z = 0; z < nz; z++) {
                            if (// ditau
                                (leading_l1tau_pt >= minx + x &&
                                subleading_l1tau_pt >= minx + xp) ||
                                // ditaujet
                                (leading_l1tau_pt >= miny + y &&
                                    subleading_l1tau_pt >= miny + yp &&
                                leading_l1jet_pt_map[miny + y] >= minz + z)) 
                                {
                                    histos.at(index)->Fill(minz + z, weight);
                                }
                        }
                    }
                }
            }
        }
    }
    
    for (Long64_t jentry=0; jentry<nentries;jentry++) {                   
        Long64_t ientry = LoadTree(jentry);
        if (ientry < 0) break;
        nb = fChain->GetEntry(jentry);   nbytes += nb;
        int index = -1;
        for (int y = 0; y < ny; y++) {
            for (int yp = 0; yp < y + 1; yp++) {
                index++;
                for (int z = 0; z < nz; z++) {
                    if (// ditaujet
                        (leading_l1tau_pt >= miny + y &&
                            subleading_l1tau_pt >= miny + yp &&
                        leading_l1jet_pt >= minz + z))
                        {
                            histos_ditaujet.at(index)->Fill(minz + z, weight);
                        }
                }
            }
        }
    }
    
    outfile->cd();
    outfile->Write();
    
}


void TotalTrigger::RateAsymmTriTauLoop()
{
    if (fChain == 0) return;
    
    Long64_t nentries = fChain->GetEntriesFast();
    
    Long64_t nbytes = 0, nb = 0;
    
    
    int nx = maxx - minx;
    int ny = maxy - miny;
    int nz = maxz - minz;
    
    std::vector <TH1F*> histos;
    for (int x = 0; x < nx; x++) {
        for (int xp = 0; xp < x + 1; xp++) {
            for (int y = 0; y < ny; y++) {
                for (int yp = 0; yp < y + 1; yp++) {
                    for (int ytri = 0; ytri < ny; ytri++) {
                        histos.push_back(new TH1F(
                            ("histo_ditau_" + std::to_string(minx + x) + "_" + std::to_string(minx + xp)
                                + "__ditau_" + std::to_string(miny + y) + "_" + std::to_string(miny + yp)
                            + "__tritau_" + std::to_string(miny + ytri) + "_jet").c_str(),
                        "; zz; Events", nz, minz, maxz));
                    }
                }
            }
        }
    }
    
    for (Long64_t jentry=0; jentry<nentries;jentry++) {                   
        Long64_t ientry = LoadTree(jentry);
        if (ientry < 0) break;
        nb = fChain->GetEntry(jentry);   nbytes += nb;
        int index = -1;
        for (int x = 0; x < nx; x++) {
            for (int xp = 0; xp < x + 1; xp++) {
                for (int y = 0; y < ny; y++) {
                    for (int yp = 0; yp < y + 1; yp++) {
                        for (int ytri = 0; ytri < ny; ytri++) {
                            index++;
                            for (int z = 0; z < nz; z++) {
                                if (// ditau
                                    (leading_l1tau_pt >= minx + x &&
                                    subleading_l1tau_pt >= minx + xp) ||
                                    // ditaujet
                                    (leading_l1tau_pt >= miny + y &&
                                        subleading_l1tau_pt >= miny + yp &&
                                    leading_l1jet_pt_map[miny + y] >= minz + z) ||
                                    (leading_l1tau_pt >= miny + ytri &&
                                        subleading_l1tau_pt >= miny + ytri &&
                                    subsubleading_l1tau_pt >= miny + ytri))
                                    histos.at(index)->Fill(minz + z, weight);
                            }
                        }
                    }
                }
            }
        }
    }
    outfile->cd();
    outfile->Write();
    
}


void TotalTrigger::RateAsymmVBFLoop()
{
    if (fChain == 0) return;
    
    Long64_t nentries = fChain->GetEntriesFast();
    
    Long64_t nbytes = 0, nb = 0;
    
    
    int nx = maxx - minx;
    int ny = maxy - miny;
    int nz = maxz - minz;
    
    std::vector <TH1F*> histos;
    for (int x = 0; x < nx; x++) {
        for (int xp = 0; xp < x + 1; xp++) {
            for (int y = 0; y < ny; y++) {
                for (int yp = 0; yp < y + 1; yp++) {
                    histos.push_back(new TH1F(
                        ("histo_ditau_" + std::to_string(minx + x) + "_" + std::to_string(minx + xp)
                        + "__ditau_" + std::to_string(miny + y) + "_" + std::to_string(miny + yp) + "_jet").c_str(),
                    "; zz; Events", nz, minz, maxz));
                }
            }
        }
    }
    
    for (Long64_t jentry=0; jentry<nentries;jentry++) {                   
        Long64_t ientry = LoadTree(jentry);
        if (ientry < 0) break;
        nb = fChain->GetEntry(jentry);   nbytes += nb;
        bool vbftrigger = (leading_l1jet_pt >= 90 &&
            subleading_l1jet_pt >= 30 &&
        sqrt(2 * leading_l1jet_pt * subleading_l1jet_pt * (cosh(leading_l1jet_eta - subleading_l1jet_eta) - cos(leading_l1jet_phi - subleading_l1jet_phi))) >= 620);
        int index = -1;
        for (int x = 0; x < nx; x++) {
            for (int xp = 0; xp < x + 1; xp++) {
                for (int y = 0; y < ny; y++) {
                    for (int yp = 0; yp < y + 1; yp++) {
                        index++;
                        for (int z = 0; z < nz; z++) {
                            if (// ditau
                                (leading_l1tau_pt >= minx + x &&
                                subleading_l1tau_pt >= minx + xp) ||
                                // ditaujet
                                (leading_l1tau_pt >= miny + y &&
                                    subleading_l1tau_pt >= miny + yp &&
                                leading_l1jet_pt >= minz + z) ||
                                // VBF trigger
                            vbftrigger)
                            histos.at(index)->Fill(minz + z);
                        }
                    }
                }
            }
        }
    }
    
    outfile->cd();
    outfile->Write();
    
}


void TotalTrigger::RateAsymmKetiLoop()
{
    if (fChain == 0) return;
    
    Long64_t nentries = fChain->GetEntriesFast();
    
    Long64_t nbytes = 0, nb = 0;
    
    
    int nx = maxx - minx;
    int ny = maxy - miny;
    int nz = maxz - minz;
    
    std::vector <TH1F*> histos;
    for (int x = 0; x < nx; x++) {
        for (int xp = 0; xp < x + 1; xp++) {
            for (int y = 0; y < ny; y++) {
                for (int yp = 0; yp < y + 1; yp++) {
                    histos.push_back(new TH1F(
                        ("histo_ditau_" + std::to_string(minx + x) + "_" + std::to_string(minx + xp)
                        + "__ditau_" + std::to_string(miny + y) + "_" + std::to_string(miny + yp) + "_jet").c_str(),
                    "; zz; Events", nz, minz, maxz));
                }
            }
        }
    }
    
    for (Long64_t jentry=0; jentry<nentries;jentry++) {                   
        Long64_t ientry = LoadTree(jentry);
        if (ientry < 0) break;
        nb = fChain->GetEntry(jentry);   nbytes += nb;
        bool vbftrigger = (leading_l1jet_pt >= 35 &&
            subleading_l1jet_pt >= 35 &&
            sqrt(2 * leading_l1jet_pt * subleading_l1jet_pt * (cosh(leading_l1jet_eta - subleading_l1jet_eta) - cos(leading_l1jet_phi - subleading_l1jet_phi))) >= 450 &&
        leading_l1tau_pt >= 45);
        int index = -1;
        for (int x = 0; x < nx; x++) {
            for (int xp = 0; xp < x + 1; xp++) {
                for (int y = 0; y < ny; y++) {
                    for (int yp = 0; yp < y + 1; yp++) {
                        index++;
                        for (int z = 0; z < nz; z++) {
                            if (// ditau
                                (leading_l1tau_pt >= minx + x &&
                                subleading_l1tau_pt >= minx + xp) ||
                                // ditaujet
                                (leading_l1tau_pt >= miny + y &&
                                    subleading_l1tau_pt >= miny + yp &&
                                leading_l1jet_pt >= minz + z) ||
                                // VBF trigger
                            vbftrigger)
                            histos.at(index)->Fill(minz + z);
                        }
                    }
                }
            }
        }
    }
    
    outfile->cd();
    outfile->Write();
    
}




void TotalTrigger::AsymmDiJetLoop()
{
    if (fChain == 0) return;
    
    Long64_t nentries = fChain->GetEntriesFast();
    
    Long64_t nbytes = 0, nb = 0;
    
    int nx = maxx - minx;
    int ny = maxy - miny;
    int ndz = maxdz - mindz;
    
    std::vector <TH2F*> histos;
    for (int x = 0; x < nx; x++) {
        for (int xp = 0; xp < x + 1; xp++) {
            for (int dy = 0; dy < ny; dy++) {
                for (int dyp = 0; dyp < dy + 1; dyp++) {
                    histos.push_back(new TH2F(
                        ("histo_ditau_" + std::to_string(minx + x) + "_" + std::to_string(minx + xp)
                        + "__ditau_" + std::to_string(miny + dy) + "_" + std::to_string(miny + dyp) + "_dijet").c_str(),
                    "; dzz; dzzp; Events", ndz, mindz, maxdz, ndz, mindz, maxdz));
                }
            }
        }
    }
    
    for (Long64_t jentry=0; jentry<nentries;jentry++) {                   
        Long64_t ientry = LoadTree(jentry);
        if (ientry < 0) break;
        nb = fChain->GetEntry(jentry);   nbytes += nb;
        int index = -1;
        for (int x = 0; x < nx; x++) {
            for (int xp = 0; xp < x + 1; xp++) {
                for (int dy = 0; dy < ny; dy++) {
                    for (int dyp = 0; dyp < dy + 1; dyp++) {
                        index++;
                        for (int dz = 0; dz < ndz; dz++) {
                            for (int dzp = 0; dzp < dz + 1; dzp++) {
                                if (// ditau
                                    (leading_l1tau_pt >= minx + x &&
                                        subleading_l1tau_pt >= minx + xp &&
                                        leading_tau_pt >= minx + x + add_to_leading_tau &&
                                    subleading_tau_pt >= minx + xp + add_to_subleading_tau) ||
                                    // ditaudijet
                                    (leading_l1tau_pt >= miny + dy &&
                                        subleading_l1tau_pt >= miny + dyp &&
                                        leading_tau_pt >= miny + dy + add_to_leading_tau &&
                                        subleading_tau_pt >= miny + dyp + add_to_subleading_tau &&
                                        leading_l1jet_pt >= mindz + dz &&
                                        leading_jet_pt >= mindz + dz + add_to_jet &&
                                        subleading_l1jet_pt >= mindz + dzp &&
                                    subleading_jet_pt >= mindz + dzp + add_to_jet))
                                    histos.at(index)->Fill(mindz + dz, mindz + dzp);
                            }
                        }
                    }
                }
            }
        }
    }
    
    outfile->cd();
    outfile->Write();
    
}


void TotalTrigger::AsymmVBFDiJetLoop()
{
    if (fChain == 0) return;
    
    Long64_t nentries = fChain->GetEntriesFast();
    
    Long64_t nbytes = 0, nb = 0;
    
    int nx = maxx - minx;
    int ny = maxy - miny;
    int ndz = maxdz - mindz;
    
    std::vector <TH2F*> histos;
    for (int x = 0; x < nx; x++) {
        for (int xp = 0; xp < x + 1; xp++) {
            for (int dy = 0; dy < ny; dy++) {
                for (int dyp = 0; dyp < dy + 1; dyp++) {
                    histos.push_back(new TH2F(
                        ("histo_ditau_" + std::to_string(minx + x) + "_" + std::to_string(minx + xp)
                        + "__ditau_" + std::to_string(miny + dy) + "_" + std::to_string(miny + dyp) + "_dijet").c_str(),
                    "; dzz; dzzp; Events", ndz, mindz, maxdz, ndz, mindz, maxdz));
                }
            }
        }
    }
    
    for (Long64_t jentry=0; jentry<nentries;jentry++) {                   
        Long64_t ientry = LoadTree(jentry);
        if (ientry < 0) break;
        nb = fChain->GetEntry(jentry);   nbytes += nb;
        // VBF trigger 90/100, 30/40, 620/700
        bool vbftrigger = (leading_l1jet_pt >= 90 &&
            subleading_l1jet_pt >= 30 &&
            leading_jet_pt >= 90 + add_to_jet &&
            subleading_jet_pt >= 30 + add_to_jet &&
            sqrt(2 * leading_l1jet_pt * subleading_l1jet_pt * (cosh(leading_l1jet_eta - subleading_l1jet_eta) - cos(leading_l1jet_phi - subleading_l1jet_phi))) >= 620 &&
        sqrt(2 * (leading_jet_pt + add_to_jet) * (subleading_jet_pt + add_to_jet) * (cosh(leading_jet_eta - subleading_jet_eta) - cos(leading_jet_phi - subleading_jet_phi))) >= 700);
        int index = -1;
        for (int x = 0; x < nx; x++) {
            for (int xp = 0; xp < x + 1; xp++) {
                for (int dy = 0; dy < ny; dy++) {
                    for (int dyp = 0; dyp < dy + 1; dyp++) {
                        index++;
                        for (int dz = 0; dz < ndz; dz++) {
                            for (int dzp = 0; dzp < dz + 1; dzp++) {
                                if (// ditau
                                    (leading_l1tau_pt >= minx + x &&
                                        subleading_l1tau_pt >= minx + xp &&
                                        leading_tau_pt >= minx + x + add_to_leading_tau &&
                                    subleading_tau_pt >= minx + xp + add_to_subleading_tau) ||
                                    // ditaudijet
                                    (leading_l1tau_pt >= miny + dy &&
                                        subleading_l1tau_pt >= miny + dyp &&
                                        leading_tau_pt >= miny + dy + add_to_leading_tau &&
                                        subleading_tau_pt >= miny + dyp + add_to_subleading_tau &&
                                        leading_l1jet_pt >= mindz + dz &&
                                        leading_jet_pt >= mindz + dz + add_to_jet &&
                                        subleading_l1jet_pt >= mindz + dzp &&
                                    subleading_jet_pt >= mindz + dzp + add_to_jet) ||
                                vbftrigger)
                                histos.at(index)->Fill(mindz + dz, mindz + dzp);
                            }
                        }
                    }
                }
            }
        }
    }
    
    outfile->cd();
    outfile->Write();
    
}


void TotalTrigger::AsymmKetiDiJetLoop()
{
    if (fChain == 0) return;
    
    Long64_t nentries = fChain->GetEntriesFast();
    
    Long64_t nbytes = 0, nb = 0;
    
    int nx = maxx - minx;
    int ny = maxy - miny;
    int ndz = maxdz - mindz;
    
    std::vector <TH2F*> histos;
    for (int x = 0; x < nx; x++) {
        for (int xp = 0; xp < x + 1; xp++) {
            for (int dy = 0; dy < ny; dy++) {
                for (int dyp = 0; dyp < dy + 1; dyp++) {
                    histos.push_back(new TH2F(
                        ("histo_ditau_" + std::to_string(minx + x) + "_" + std::to_string(minx + xp)
                        + "__ditau_" + std::to_string(miny + dy) + "_" + std::to_string(miny + dyp) + "_dijet").c_str(),
                    "; dzz; dzzp; Events", ndz, mindz, maxdz, ndz, mindz, maxdz));
                }
            }
        }
    }
    
    for (Long64_t jentry=0; jentry<nentries;jentry++) {                   
        Long64_t ientry = LoadTree(jentry);
        if (ientry < 0) break;
        nb = fChain->GetEntry(jentry);   nbytes += nb;
        // VBF trigger 90/100, 30/40, 620/700
        bool vbftrigger = (leading_l1jet_pt >= 35 &&
            subleading_l1jet_pt >= 35 &&
            leading_jet_pt >= 35 + add_to_jet &&
            subleading_jet_pt >= 35 + add_to_jet &&
            sqrt(2 * leading_l1jet_pt * subleading_l1jet_pt * (cosh(leading_l1jet_eta - subleading_l1jet_eta) - cos(leading_l1jet_phi - subleading_l1jet_phi))) >= 450 &&
            sqrt(2 * (leading_jet_pt + add_to_jet) * (subleading_jet_pt + add_to_jet) * (cosh(leading_jet_eta - subleading_jet_eta) - cos(leading_jet_phi - subleading_jet_phi))) >= 550 &&
            leading_l1tau_pt >= 45 &&
        leading_tau_pt >= 45 + add_to_leading_tau);
        int index = -1;
        for (int x = 0; x < nx; x++) {
            for (int xp = 0; xp < x + 1; xp++) {
                for (int dy = 0; dy < ny; dy++) {
                    for (int dyp = 0; dyp < dy + 1; dyp++) {
                        index++;
                        for (int dz = 0; dz < ndz; dz++) {
                            for (int dzp = 0; dzp < dz + 1; dzp++) {
                                if (// ditau
                                    (leading_l1tau_pt >= minx + x &&
                                        subleading_l1tau_pt >= minx + xp &&
                                        leading_tau_pt >= minx + x + add_to_leading_tau &&
                                    subleading_tau_pt >= minx + xp + add_to_subleading_tau) ||
                                    // ditaudijet
                                    (leading_l1tau_pt >= miny + dy &&
                                        subleading_l1tau_pt >= miny + dyp &&
                                        leading_tau_pt >= miny + dy + add_to_leading_tau &&
                                        subleading_tau_pt >= miny + dyp + add_to_subleading_tau &&
                                        leading_l1jet_pt >= mindz + dz &&
                                        leading_jet_pt >= mindz + dz + add_to_jet &&
                                        subleading_l1jet_pt >= mindz + dzp &&
                                    subleading_jet_pt >= mindz + dzp + add_to_jet) ||
                                vbftrigger)
                                histos.at(index)->Fill(mindz + dz, mindz + dzp);
                            }
                        }
                    }
                }
            }
        }
    }
    
    outfile->cd();
    outfile->Write();
    
}



void TotalTrigger::RateAsymmDiJetLoop()
{
    if (fChain == 0) return;
    
    Long64_t nentries = fChain->GetEntriesFast();
    
    Long64_t nbytes = 0, nb = 0;
    
    int nx = maxx - minx;
    int ny = maxy - miny;
    int ndz = maxdz - mindz;
    
    std::vector <TH2F*> histos;
    for (int x = 0; x < nx; x++) {
        for (int xp = 0; xp < x + 1; xp++) {
            for (int dy = 0; dy < ny; dy++) {
                for (int dyp = 0; dyp < dy + 1; dyp++) {
                    histos.push_back(new TH2F(
                        ("histo_ditau_" + std::to_string(minx + x) + "_" + std::to_string(minx + xp)
                        + "__ditau_" + std::to_string(miny + dy) + "_" + std::to_string(miny + dyp) + "_dijet").c_str(),
                    "; dzz; dzzp; Events", ndz, mindz, maxdz, ndz, mindz, maxdz));
                }
            }
        }
    }
    
    for (Long64_t jentry=0; jentry<nentries;jentry++) {                   
        Long64_t ientry = LoadTree(jentry);
        if (ientry < 0) break;
        nb = fChain->GetEntry(jentry);   nbytes += nb;
        int index = -1;
        for (int x = 0; x < nx; x++) {
            for (int xp = 0; xp < x + 1; xp++) {
                for (int dy = 0; dy < ny; dy++) {
                    for (int dyp = 0; dyp < dy + 1; dyp++) {
                        index++;
                        for (int dz = 0; dz < ndz; dz++) {
                            for (int dzp = 0; dzp < dz + 1; dzp++) {
                                if (// ditau
                                    (leading_l1tau_pt >= minx + x &&
                                    subleading_l1tau_pt >= minx + xp) ||
                                    // ditaudijet
                                    (leading_l1tau_pt >= miny + dy &&
                                        subleading_l1tau_pt >= miny + dyp &&
                                        leading_l1jet_pt >= mindz + dz &&
                                    subleading_l1jet_pt >= mindz + dzp))
                                    histos.at(index)->Fill(mindz + dz, mindz + dzp);
                            }
                        }
                    }
                }
            }
        }
    }
    
    outfile->cd();
    outfile->Write();
    
}


void TotalTrigger::RateAsymmVBFDiJetLoop()
{
    if (fChain == 0) return;
    
    Long64_t nentries = fChain->GetEntriesFast();
    
    Long64_t nbytes = 0, nb = 0;
    
    int nx = maxx - minx;
    int ny = maxy - miny;
    int ndz = maxdz - mindz;
    
    std::vector <TH2F*> histos;
    for (int x = 0; x < nx; x++) {
        for (int xp = 0; xp < x + 1; xp++) {
            for (int dy = 0; dy < ny; dy++) {
                for (int dyp = 0; dyp < dy + 1; dyp++) {
                    histos.push_back(new TH2F(
                        ("histo_ditau_" + std::to_string(minx + x) + "_" + std::to_string(minx + xp)
                        + "__ditau_" + std::to_string(miny + dy) + "_" + std::to_string(miny + dyp) + "_dijet").c_str(),
                    "; dzz; dzzp; Events", ndz, mindz, maxdz, ndz, mindz, maxdz));
                }
            }
        }
    }
    
    for (Long64_t jentry=0; jentry<nentries;jentry++) {                   
        Long64_t ientry = LoadTree(jentry);
        if (ientry < 0) break;
        nb = fChain->GetEntry(jentry);   nbytes += nb;
        bool vbftrigger = (leading_l1jet_pt >= 90 &&
            subleading_l1jet_pt >= 30 &&
        sqrt(2 * leading_l1jet_pt * subleading_l1jet_pt * (cosh(leading_l1jet_eta - subleading_l1jet_eta) - cos(leading_l1jet_phi - subleading_l1jet_phi))) >= 620);
        int index = -1;
        for (int x = 0; x < nx; x++) {
            for (int xp = 0; xp < x + 1; xp++) {
                for (int dy = 0; dy < ny; dy++) {
                    for (int dyp = 0; dyp < dy + 1; dyp++) {
                        index++;
                        for (int dz = 0; dz < ndz; dz++) {
                            for (int dzp = 0; dzp < dz + 1; dzp++) {
                                if (// ditau
                                    (leading_l1tau_pt >= minx + x &&
                                    subleading_l1tau_pt >= minx + xp) ||
                                    // ditaudijet
                                    (leading_l1tau_pt >= miny + dy &&
                                        subleading_l1tau_pt >= miny + dyp &&
                                        leading_l1jet_pt >= mindz + dz &&
                                    subleading_l1jet_pt >= mindz + dzp) ||
                                vbftrigger)
                                histos.at(index)->Fill(mindz + dz, mindz + dzp);
                            }
                        }
                    }
                }
            }
        }
    }
    
    outfile->cd();
    outfile->Write();
    
}

void TotalTrigger::RateAsymmManfredLoop()
{
    if (fChain == 0) return;
    Long64_t nentries = fChain->GetEntriesFast();
    Long64_t nbytes = 0, nb = 0;

    int nx = maxx - minx;
    int ny = maxy - miny;
    int nz = maxz - minz;
    
    std::vector <TH1F*> histos;
    for (int x = 0; x < nx; x++) {
        for (int xp = 0; xp < x + 1; xp++) {
            for (int y = 0; y < ny; y++) {
                for (int yp = 0; yp < y + 1; yp++) {
                    histos.push_back(new TH1F(
                        ("histo_ditau_" + std::to_string(minx + x) + "_" + std::to_string(minx + xp)
                        + "__ditau_" + std::to_string(miny + y) + "_" + std::to_string(miny + yp) + "_jet").c_str(),
                    "; zz; Events", nz, minz, maxz));
                }
            }
        }
    }
    //std::cout << nTaus << std::endl;
    for (Long64_t jentry=0; jentry<nentries; jentry++) {                   
        Long64_t ientry = LoadTree(jentry);
        if (ientry < 0) break;
        nb = fChain->GetEntry(jentry);   nbytes += nb;

        std::vector <float> L1tau_pt, L1tau_eta, L1tau_phi;
        std::vector <float> L1jet_pt, L1jet_eta, L1jet_phi;
        for (UShort_t iL1Obj = 0; iL1Obj < nTaus; iL1Obj++) {
            // L1 taus
            //std::cout << tauEt->at(iL1Obj) << " " << tauIso->at(iL1Obj) << " " << fabs(tauEta->at(iL1Obj)) << " ";
            if (tauEt->at(iL1Obj) >= miny && tauIso->at(iL1Obj) == 1 && abs(tauEta->at(iL1Obj)) <= 2.1 && tauBx->at(iL1Obj) == 0) {
            //if (tauEt->at(iL1Obj) >= miny && tauIso->at(iL1Obj) == 1 && abs(tauEta->at(iL1Obj)) <= 2.1) {
                //std::cout << "IN!";
                L1tau_pt.push_back(tauEt->at(iL1Obj));
                L1tau_eta.push_back(tauEta->at(iL1Obj));
                L1tau_phi.push_back(tauPhi->at(iL1Obj));
            }
            //std::cout << std::endl;
        }
        for (UShort_t iL1Obj = 0; iL1Obj < nJets; iL1Obj++) {
            // L1 jets
            if (jetEt->at(iL1Obj) >= minz && jetBx->at(iL1Obj) == 0) {
            //if (jetEt->at(iL1Obj) >= minz) {
                L1jet_pt.push_back(jetEt->at(iL1Obj));
                L1jet_eta.push_back(jetEta->at(iL1Obj));
                L1jet_phi.push_back(jetPhi->at(iL1Obj));
            }
        }
        

        
        // Avoid looping if we have less than 2 L1 and offline taus or less than 1 L1 and offline jet
        if (L1tau_pt.size() < 2 || L1jet_pt.size() < 1)
            continue;
        
        Float_t leading_l1tau_pt_ = L1tau_pt[0];
        Float_t subleading_l1tau_pt_ = L1tau_pt[1];
        
        // build triplets (l1tau, l1tau, l1jet_or)
        std::vector <std::vector<float>> l1_triplets;
        for (size_t iLeadL1tau = 0; iLeadL1tau < L1tau_pt.size(); iLeadL1tau++) {
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
        
        //if (l1_triplets.size() > 0) {
            //std::cout << "************" << std::endl;
            //for (auto triplet: l1_triplets) {
                //std::cout << triplet[0] << " " << triplet[1] << " " << triplet[2] << std::endl;
            //}
        //}

        int index = -1;
        for (int x = 0; x < nx; x++) {
            for (int xp = 0; xp < x + 1; xp++) {
                for (int y = 0; y < ny; y++) {
                    for (int yp = 0; yp < y + 1; yp++) {
                        index++;
                        for (int z = 0; z < nz; z++) {
                            if (leading_l1tau_pt_ >= minx + x &&
                                    subleading_l1tau_pt_ >= minx + xp) {
                                histos.at(index)->Fill(minz + z, weight);
                            } else {
                                bool pass_l1_ditaujet = false;
                                for (auto triplet: l1_triplets) {
                                    if (triplet[0] >=  miny + y
                                            && triplet[1] >=  miny + yp
                                            && triplet[2] >=  minz + z) {
                                        pass_l1_ditaujet = true;
                                        break;
                                    }
                                }                               
                                if (pass_l1_ditaujet)
                                    histos.at(index)->Fill(minz + z, weight);
                            }
                        } //z
                    } //yp
                } //y
            } //xp
        } //x
        
        
    }

    outfile->cd();
    outfile->Write();

}




void TotalTrigger::RateAsymmKetiDiJetLoop()
{
    if (fChain == 0) return;
    
    Long64_t nentries = fChain->GetEntriesFast();
    
    Long64_t nbytes = 0, nb = 0;
    
    int nx = maxx - minx;
    int ny = maxy - miny;
    int ndz = maxdz - mindz;
    
    std::vector <TH2F*> histos;
    for (int x = 0; x < nx; x++) {
        for (int xp = 0; xp < x + 1; xp++) {
            for (int dy = 0; dy < ny; dy++) {
                for (int dyp = 0; dyp < dy + 1; dyp++) {
                    histos.push_back(new TH2F(
                        ("histo_ditau_" + std::to_string(minx + x) + "_" + std::to_string(minx + xp)
                        + "__ditau_" + std::to_string(miny + dy) + "_" + std::to_string(miny + dyp) + "_dijet").c_str(),
                    "; dzz; dzzp; Events", ndz, mindz, maxdz, ndz, mindz, maxdz));
                }
            }
        }
    }
    
    for (Long64_t jentry=0; jentry<nentries;jentry++) {                   
        Long64_t ientry = LoadTree(jentry);
        if (ientry < 0) break;
        nb = fChain->GetEntry(jentry);   nbytes += nb;
        bool vbftrigger = (leading_l1jet_pt >= 35 &&
            subleading_l1jet_pt >= 35 &&
            sqrt(2 * leading_l1jet_pt * subleading_l1jet_pt * (cosh(leading_l1jet_eta - subleading_l1jet_eta) - cos(leading_l1jet_phi - subleading_l1jet_phi))) >= 450 &&
        leading_l1tau_pt >= 45);
        int index = -1;
        for (int x = 0; x < nx; x++) {
            for (int xp = 0; xp < x + 1; xp++) {
                for (int dy = 0; dy < ny; dy++) {
                    for (int dyp = 0; dyp < dy + 1; dyp++) {
                        index++;
                        for (int dz = 0; dz < ndz; dz++) {
                            for (int dzp = 0; dzp < dz + 1; dzp++) {
                                if (// ditau
                                    (leading_l1tau_pt >= minx + x &&
                                    subleading_l1tau_pt >= minx + xp) ||
                                    // ditaudijet
                                    (leading_l1tau_pt >= miny + dy &&
                                        subleading_l1tau_pt >= miny + dyp &&
                                        leading_l1jet_pt >= mindz + dz &&
                                    subleading_l1jet_pt >= mindz + dzp) ||
                                vbftrigger)
                                histos.at(index)->Fill(mindz + dz, mindz + dzp);
                            }
                        }
                    }
                }
            }
        }
    }
    
    outfile->cd();
    outfile->Write();
    
}


void TotalTrigger::AsymmManfredLoop()
{
    if (fChain == 0) return;
    Long64_t nentries = fChain->GetEntriesFast();
    Long64_t nbytes = 0, nb = 0;

    int nx = maxx - minx;
    int ny = maxy - miny;
    int nz = maxz - minz;
    
    std::vector <TH1F*> histos;
    for (int x = 0; x < nx; x++) {
        for (int xp = 0; xp < x + 1; xp++) {
            for (int y = 0; y < ny; y++) {
                for (int yp = 0; yp < y + 1; yp++) {
                    histos.push_back(new TH1F(
                        ("histo_ditau_" + std::to_string(minx + x) + "_" + std::to_string(minx + xp)
                        + "__ditau_" + std::to_string(miny + y) + "_" + std::to_string(miny + yp) + "_jet").c_str(),
                    "; zz; Events", nz, minz, maxz));
                }
            }
        }
    }

    for (Long64_t jentry=0; jentry<nentries; jentry++) {
        //std::cout << jentry << std::endl;
        //if (jentry > 10) break;
        Long64_t ientry = LoadTree(jentry);
        if (ientry < 0) break;
        nb = fChain->GetEntry(jentry);   nbytes += nb;

        std::vector <float> L1tau_pt, L1tau_eta, L1tau_phi;
        std::vector <float> L1jet_pt, L1jet_eta, L1jet_phi;
        std::vector <float> tau_pt, tau_eta, tau_phi;
        std::vector <float> jet_pt, jet_eta, jet_phi;

        for (size_t iL1Obj = 0; iL1Obj < nL1Obj; iL1Obj++) {
            // L1 taus
            if (L1Obj_pt[iL1Obj] >= miny && L1Obj_iso[iL1Obj] == 1 && L1Obj_type[iL1Obj] == 1 && fabs(L1Obj_eta[iL1Obj]) <= 2.1) {
                L1tau_pt.push_back(L1Obj_pt[iL1Obj]);
                L1tau_eta.push_back(L1Obj_eta[iL1Obj]);
                L1tau_phi.push_back(L1Obj_phi[iL1Obj]);
                //std::cout << "L1Tau " << L1Obj_pt[iL1Obj] << " " << L1Obj_eta[iL1Obj] << " " << L1Obj_phi[iL1Obj] << std::endl;
            // L1 jets
            } else if (L1Obj_pt[iL1Obj] >= minz && L1Obj_type[iL1Obj] == 0) {
                L1jet_pt.push_back(L1Obj_pt[iL1Obj]);
                L1jet_eta.push_back(L1Obj_eta[iL1Obj]);
                L1jet_phi.push_back(L1Obj_phi[iL1Obj]);
                //std::cout << "L1Jet " << L1Obj_pt[iL1Obj] << " " << L1Obj_eta[iL1Obj] << " " << L1Obj_phi[iL1Obj] << std::endl;
            }
        }
        
        for (size_t iTau = 0; iTau < nTau; iTau++) {
            //std::cout << "Tau " << Tau_pt[iTau] << " " << Tau_eta[iTau] << " " << Tau_phi[iTau] << std::endl;
            if (fabs(Tau_eta[iTau]) < 2.1) {
                tau_pt.push_back(Tau_pt[iTau]);
                tau_eta.push_back(Tau_eta[iTau]);
                tau_phi.push_back(Tau_phi[iTau]);
                //std::cout << "Tau " << Tau_pt[iTau] << " " << Tau_eta[iTau] << " " << Tau_phi[iTau] << std::endl;
            }
        }
        

        for (size_t iJet = 0; iJet < nJet; iJet++) {
            // std::cout << iJet << " " << (fabs(Jet_eta[iJet]) <= 4.7) << " " <<  (Jet_jetId[iJet] >= 2) << " " << Jet_puId[iJet] << " " << Jet_pt[iJet] <<   std::endl;
            if (fabs(Jet_eta[iJet]) <= 4.7 && Jet_jetId[iJet] >= 2
                    && ((Jet_puId[iJet] >= 4 && Jet_pt[iJet] <= 50) || (Jet_pt[iJet] > 50))) {
                jet_pt.push_back(Jet_pt[iJet]);
                jet_eta.push_back(Jet_eta[iJet]);
                jet_phi.push_back(Jet_phi[iJet]);
                //std::cout << "Jet " << Jet_pt[iJet] << " " << Jet_eta[iJet] << " " << Jet_phi[iJet] << std::endl;
            }
        }
        
        // Avoid looping if we have less than 2 L1 and offline taus or less than 1 L1 and offline jet
        // std::cout << L1tau_pt.size() << " " << tau_pt.size() << " " << L1jet_pt.size() << " " << jet_pt.size() << std::endl;
        if (L1tau_pt.size() < 2 || tau_pt.size() < 2 || L1jet_pt.size() < 1 || jet_pt.size() < 1)
            continue;
        
        Float_t leading_l1tau_pt_ = L1tau_pt[0];
        Float_t subleading_l1tau_pt_ = L1tau_pt[1];
        Float_t leading_tau_pt_ = tau_pt[0];
        Float_t subleading_tau_pt_ = tau_pt[1];
        
        if (leading_tau_pt_ < min_leading_tau_pt || subleading_tau_pt_ < min_subleading_tau_pt)
            continue;
        
        // build triplets (l1tau, l1tau, l1jet_or)
        // std::cout << "Looping" << std::endl;
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
                    //std::cout << "Pushing back L1 triplet " << L1tau_pt[iLeadL1tau] << " " << L1tau_pt[iSubleadL1tau] << " " << L1jet_pt[iLeadL1jet] << std::endl;
                    l1_triplets.push_back(std::vector<float>({L1tau_pt[iLeadL1tau], L1tau_pt[iSubleadL1tau], L1jet_pt[iLeadL1jet]}));               
                    break;
                }
            }
        }
        // build triplets (tau, tau, jet_or)
        std::vector <std::vector<float>> off_triplets;
        for (size_t iLeadtau = 0; iLeadtau < tau_pt.size() - 1; iLeadtau++) {
            for (size_t iSubleadtau = iLeadtau + 1; iSubleadtau < tau_pt.size(); iSubleadtau++) {
                for (size_t iLeadjet = 0; iLeadjet < jet_pt.size(); iLeadjet++) {
                    Double_t deta_lead = jet_eta[iLeadjet] - tau_eta[iLeadtau];
                    Double_t dphi_lead = Phi_mpi_pi(jet_phi[iLeadjet] - tau_phi[iLeadtau]);
                    Double_t dr_lead = TMath::Sqrt(deta_lead * deta_lead + dphi_lead * dphi_lead);
                    
                    Double_t deta_sublead = jet_eta[iLeadjet] - tau_eta[iSubleadtau];
                    Double_t dphi_sublead = Phi_mpi_pi(jet_phi[iLeadjet] - tau_phi[iSubleadtau]);
                    Double_t dr_sublead = TMath::Sqrt(deta_sublead * deta_sublead + dphi_sublead * dphi_sublead);
                    
                    //std::cout << jet_pt[iLeadjet] << " " << dr_lead << " " << dr_sublead << std::endl;
                    
                    if (dr_lead < 0.5 || dr_sublead < 0.5) continue;
                    //std::cout << "Pushing back Off triplet " << tau_pt[iLeadtau] << " " << tau_pt[iSubleadtau] << " " << jet_pt[iLeadjet] << std::endl;
                    off_triplets.push_back(std::vector<float>({tau_pt[iLeadtau], tau_pt[iSubleadtau], jet_pt[iLeadjet]}));              
                    break;
                }
                
            }
        }
        
        //std::cout << "*** PREVIOUS CODE ***" << std::endl;
        //std::cout << "L1tau " << leading_l1tau_pt << " " << leading_l1tau_eta << " " << leading_l1tau_phi << std::endl;
        //std::cout << "L1tau " << subleading_l1tau_pt << " " << subleading_l1tau_eta << " " << subleading_l1tau_phi << std::endl;
        //std::cout << "L1jet " << leading_l1jet_pt_map[20] << " " << leading_l1jet_eta_map[20] << " " << leading_l1jet_phi_map[20] << std::endl;
        //std::cout << "tau " << leading_tau_pt << " " << leading_tau_eta << " " << leading_tau_phi << std::endl;
        //std::cout << "tau " << subleading_tau_pt << " " << subleading_tau_eta << " " << subleading_tau_phi << std::endl;
        //std::cout << "jet " << leading_jet_pt_map[20] << " " << leading_jet_eta_map[20] << " " << leading_jet_phi_map[20] << std::endl;

        int index = -1;
        for (int x = 0; x < nx; x++) {
            for (int xp = 0; xp < x + 1; xp++) {
                for (int y = 0; y < ny; y++) {
                    for (int yp = 0; yp < y + 1; yp++) {
                        index++;
                        for (int z = 0; z < nz; z++) {
                            //std::cout << minx + x << " " << minx + xp << " " << miny+ y << " " << miny + yp << " " << minz + z << std::endl;
                            if (leading_l1tau_pt_ >= minx + x &&
                                    subleading_l1tau_pt_ >= minx + xp &&
                                    leading_tau_pt_ >= minx + x + add_to_leading_tau &&
                                    subleading_tau_pt_ >= minx + xp + add_to_subleading_tau) {
                                //std::cout << "Pass ditau" << std::endl;
                                histos.at(index)->Fill(minz + z);
                            } else {
                                bool pass_l1_ditaujet = false;
                                for (auto triplet: l1_triplets) {
                                    if (triplet[0] >=  miny + y
                                            && triplet[1] >=  miny + yp
                                            && triplet[2] >=  minz + z) {
                                        pass_l1_ditaujet = true;
                                        break;
                                    }
                                }
                                bool pass_ditaujet = false;
                                for (auto triplet: off_triplets) {
                                    if (triplet[0] >=  miny + y + add_to_leading_tau
                                            && triplet[1] >=  miny + yp + add_to_subleading_tau
                                            && triplet[2] >=  minz + z + add_to_jet) {
                                        pass_ditaujet = true;
                                        break;
                                    }
                                }
                                
                                if (pass_l1_ditaujet && pass_ditaujet) {
                                    //std::cout << "Pass ditaujet" << std::endl;
                                    histos.at(index)->Fill(minz + z);
                                }
                            }
                        } //z
                    } //yp
                } //y
            } //xp
        } //x
        
        
    }

    outfile->cd();
    outfile->Write();

}


// Plotting

void TotalTrigger::L1TauOfflineJetLoop()
{
    if (fChain == 0) return;
    Long64_t nentries = fChain->GetEntriesFast();
    Long64_t nbytes = 0, nb = 0;
    int ny = maxy - miny;

    TH1F* turnon_passed = new TH1F("turnon_passed", "; offline jet pt [GeV]; Events", 60, 30, 210);
    TH1F* turnon_total = new TH1F("turnon_total", "; offline jet pt [GeV]; Events", 60, 30, 210);
    TH2F* efficiency_passed = new TH2F("eff_passed", "; L1 tau pt [GeV]; offline jet pt [GeV]", ny, miny, maxy, 60, 30, 210);
    TH2F* efficiency_total = new TH2F("eff_total", "; L1 tau pt [GeV]; offline jet pt [GeV]", ny, miny, maxy, 60, 30, 210);
    std::map <int, TH1*> histos_1D_passed;
    std::map <int, TH1*> histos_1D_total;
    
    for (int y = 0; y < ny; y++) {
        histos_1D_passed[miny + y] = new TH1F(("eff_" + std::to_string(miny + y) + "_passed").c_str(), "; offline jet pt [GeV]; Events", 60, 30, 210);
        histos_1D_total[miny + y] = new TH1F(("eff_" + std::to_string(miny + y) + "_total").c_str(), "; offline jet pt [GeV]; Events", 60, 30, 210);
    }    

    for (Long64_t jentry=0; jentry<nentries; jentry++) {                   
        Long64_t ientry = LoadTree(jentry);
        if (ientry < 0) break;
        nb = fChain->GetEntry(jentry);   nbytes += nb;

        std::vector <float> L1tau_pt;
        std::vector <float> L1tau_eta;
        std::vector <float> L1tau_phi;

        for (size_t iL1Obj = 0; iL1Obj < nL1Obj; iL1Obj++) {
            if (L1Obj_iso[iL1Obj] == 1 && L1Obj_type[iL1Obj] == 1) {
                L1tau_pt.push_back(L1Obj_pt[iL1Obj]);
                L1tau_eta.push_back(L1Obj_eta[iL1Obj]);
                L1tau_phi.push_back(L1Obj_phi[iL1Obj]);
            }
        }

        for (size_t iJet = 0; iJet < nJet; iJet++) {
            for (size_t iL1Tau = 0; iL1Tau < L1tau_pt.size(); iL1Tau++) {
                Double_t deta = Jet_eta[iJet] - L1tau_eta[iL1Tau];
                Double_t dphi = Phi_mpi_pi(Jet_phi[iJet] - L1tau_phi[iL1Tau]);
                Double_t dr = TMath::Sqrt(deta * deta + dphi * dphi);
                if (dr < 0.5) { // matched!
                    turnon_passed->Fill(Jet_pt[iJet]);
                    for (int y = 0; y < ny; y++) {
                        if (L1tau_pt[iL1Tau] >= miny + y) {
                            histos_1D_passed[miny + y]->Fill(Jet_pt[iJet]);
                            efficiency_passed->Fill(miny + y, Jet_pt[iJet]);
                        }
                    }

                    break;
                }
            }
            turnon_total->Fill(Jet_pt[iJet]);
            for (int y = 0; y < ny; y++) {
                histos_1D_total[miny + y]->Fill(Jet_pt[iJet]);
                efficiency_total->Fill(miny + y, Jet_pt[iJet]);
            }
        }
    }

    outfile->cd();
    outfile->Write();

}



