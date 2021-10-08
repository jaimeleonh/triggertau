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
                if ((miny + y == 26) && (miny + yp == 26) && (minz + z == 55))
                    std::cout << eventnumber << std::endl;
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