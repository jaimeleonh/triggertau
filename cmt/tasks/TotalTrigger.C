#define TotalTrigger_cxx
#include "TotalTrigger.h"
#include <TH2.h>
#include <TStyle.h>
#include <TCanvas.h>

void TotalTrigger::Loop()
{
//   In a ROOT session, you can do:
//      root> .L TotalTrigger.C
//      root> TotalTrigger t
//      root> t.GetEntry(12); // Fill t data members with entry number 12
//      root> t.Show();       // Show values of entry 12
//      root> t.Show(16);     // Read and show values of entry 16
//      root> t.Loop();       // Loop on all entries
//

//     This is the loop skeleton where:
//    jentry is the global entry number in the chain
//    ientry is the entry number in the current Tree
//  Note that the argument to GetEntry must be:
//    jentry for TChain::GetEntry
//    ientry for TTree::GetEntry and TBranch::GetEntry
//
//       To read only selected branches, Insert statements like:
// METHOD1:
//    fChain->SetBranchStatus("*",0);  // disable all branches
//    fChain->SetBranchStatus("branchname",1);  // activate branchname
// METHOD2: replace line
//    fChain->GetEntry(jentry);       //read all branches
//by  b_branchname->GetEntry(ientry); //read only this branch
  if (fChain == 0) return;

  Long64_t nentries = fChain->GetEntriesFast();

  Long64_t nbytes = 0, nb = 0;

  
  int nx = maxx - minx;
  int ny = maxy - miny;
  int nz = maxz - minz;
  int ndz = maxdz - mindz;

  std::vector <TH2F*> histos;
  for (int x = 0; x < nx; x++) {
    for (int xp = x; xp < x + 1; xp++) {
      for (int y = 0; y < ny; y++) {
        for (int yp = y; yp < y + 1; yp++) {
          for (int dy = 0; dy < ny; dy++) {
            for (int dyp = dy; dyp < dy + 1; dyp++) {
              histos.push_back(new TH2F(
                  ("histo_ditau_" + std::to_string(minx + x) + "_" + std::to_string(minx + xp)
                     + "__ditau_" + std::to_string(miny + y) + "_" + std::to_string(miny + yp) + "_jet" // + std::to_string(20 + z) 
                     + "__ditau_" + std::to_string(miny + dy) + "_" + std::to_string(miny + dyp) + "_dijet").c_str(),
                  //"; zz; dzz; dzzp", 140, 20, 160, 80, 0, 80, 80, 0, 80);
                  "; zz; dzz; Events", nz, minz, maxz, ndz, mindz, maxdz));
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
                    for (int dzp = dz; dzp < dz + 1; dzp++) {
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
                        histos.at(index)->Fill(minz + z, mindz + dz);
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

void TotalTrigger::RateLoop()
{
//   In a ROOT session, you can do:
//      root> .L TotalTrigger.C
//      root> TotalTrigger t
//      root> t.GetEntry(12); // Fill t data members with entry number 12
//      root> t.Show();       // Show values of entry 12
//      root> t.Show(16);     // Read and show values of entry 16
//      root> t.Loop();       // Loop on all entries
//

//     This is the loop skeleton where:
//    jentry is the global entry number in the chain
//    ientry is the entry number in the current Tree
//  Note that the argument to GetEntry must be:
//    jentry for TChain::GetEntry
//    ientry for TTree::GetEntry and TBranch::GetEntry
//
//       To read only selected branches, Insert statements like:
// METHOD1:
//    fChain->SetBranchStatus("*",0);  // disable all branches
//    fChain->SetBranchStatus("branchname",1);  // activate branchname
// METHOD2: replace line
//    fChain->GetEntry(jentry);       //read all branches
//by  b_branchname->GetEntry(ientry); //read only this branch
  if (fChain == 0) return;

  Long64_t nentries = fChain->GetEntriesFast();

  Long64_t nbytes = 0, nb = 0;
   
  //uint16_t pass[8][1][13][1][140][13][1][80][1];
  
  int nx = maxx - minx;
  int ny = maxy - miny;
  int nz = maxz - minz;
  int ndz = maxdz - mindz;
  
  // uint16_t pass[nx][ny][nz][ny][ndz];
  // for (int x = 0; x < nx; x++) {
    // for (int xp = x; xp < x + 1; xp++) {
      // for (int y = 0; y < ny; y++) {
        // for (int yp = y; yp < y + 1; yp++) {
          // for (int z = 0; z < nz; z++) {
            // for (int dy = 0; dy < ny; dy ++) {
              // for (int dyp = dy; dyp < dy + 1; dyp++) {
                // for (int dz = 0; dz < nz; dz++) {
                  // for (int dzp = dz; dzp < dz + 1; dzp++) {
                    // // pass[x][xp - x][y][yp - y][z][dy][dyp - dy][dz][dzp - dz] = 0;
                    // pass[x][y][z][dy][dz] = 0;
                    // // std::cout << x << " ";
                    // // // std::cout << xp << " ";
                     // // std::cout << y << " ";
                    // // // std::cout << yp << " ";
                    // // std::cout << z << " ";
                    // // std::cout << dy << " ";
                    // // // std::cout << dyp << " ";
                    // // std::cout << dz << std::endl;
                    // // // std::cout << dzp << 
                  // }
                // }
              // }
            // }
          // }
        // }
      // }
    // }
  // }
  
std::vector <TH2F*> histos;
  for (int x = 0; x < nx; x++) {
    for (int xp = x; xp < x + 1; xp++) {
      for (int y = 0; y < ny; y++) {
        for (int yp = y; yp < y + 1; yp++) {
          for (int dy = 0; dy < ny; dy++) {
            for (int dyp = dy; dyp < dy + 1; dyp++) {
              histos.push_back(new TH2F(
                  ("histo_ditau_" + std::to_string(minx + x) + "_" + std::to_string(minx + xp)
                     + "__ditau_" + std::to_string(miny + y) + "_" + std::to_string(miny + yp) + "_jet" // + std::to_string(20 + z) 
                     + "__ditau_" + std::to_string(miny + dy) + "_" + std::to_string(miny + dyp) + "_dijet").c_str(),
                  //"; zz; dzz; dzzp", 140, 20, 160, 80, 0, 80, 80, 0, 80);
                  "; zz; dzz; Events", nz, minz, maxz, ndz, mindz, maxdz));
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
                    for (int dzp = dz; dzp < dz + 1; dzp++) {
                      // std::cout << x << " ";
                      // std::cout << y << " ";
                      // std::cout << z << " ";
                      // std::cout << dy << " ";
                      // std::cout << dz <<  std::endl;
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
                        //pass[x][y][z][dy][dz] += 1;
                        histos.at(index)->Fill(minz + z, mindz + dz);
                        //pass[x][xp - x][y][yp - y][z][dy][dyp - dy][dz][dzp - dz] += 1;
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
  
  //for histo
  
  outfile->cd();
  outfile->Write();
  //delete hist;
  
   // for (int x = 0; x < nx; x++) {
      // for (int xp = x; xp < x + 1; xp++) {
         // for (int y = 0; y < ny; y++) {
            // for (int yp = y; yp < y + 1; yp++) {
               // for (int dy = 0; dy < ny; dy++) {
                  // for (int dyp = dy; dyp < dy + 1; dyp++) {
                    // TH2F *hist = new TH2F(
                      // ("histo_ditau_" + std::to_string(minx + x) + "_" + std::to_string(minx + xp)
                         // + "__ditau_" + std::to_string(miny + y) + "_" + std::to_string(miny + yp) + "_jet" // + std::to_string(20 + z) 
                         // + "__ditau_" + std::to_string(miny + dy) + "_" + std::to_string(miny + dyp) + "_dijet").c_str(),
                      // //"; zz; dzz; dzzp", 140, 20, 160, 80, 0, 80, 80, 0, 80);
                      // "; zz; dzz; Events", nz, minz, maxz, ndz, mindz, maxdz);
                    // for (int z = 0; z < nz; z++) {
                      // for (int dz = 0; dz < ndz; dz++) {
                        // for (int dzp = dz; dzp < dz + 1; dzp++) {
                          // std::cout << x << std::endl;
                          // //std::cout << pass[x][y][z][dy][dz] << std::endl;
                          // //hist->Fill(20 + dz, 20 + dzp, pass[x][y][z][dy][dz]);
                          // hist->Fill(minz + z, mindz + dz, pass[x][y][z][dy][dz]);
                          // // hist->Fill(20 + z, 20 + dz, 20 + dzp, pass[x][y][z][dy][dz]);
                          // //hist->SetBinContent(z, dz, dzp, pass[x][xp - x][y][yp - y][z][dy][dyp - dy][dz][dzp - dz]);
                           // // std::cout << x << " ";
                           // // std::cout << y << " " << z << " ";
                           // // std::cout << dy << " " << dyp << " ";
                           // // std::cout << dz << " " << dzp << std::endl; 
                        // }
                      // }
                    // }
                    // outfile->cd();
                    // outfile->Write();
                    // delete hist;
                  // }
               // }
            // }
         // }
      // }
   // }
}
