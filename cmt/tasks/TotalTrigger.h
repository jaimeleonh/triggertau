//////////////////////////////////////////////////////////
// This class has been automatically generated on
// Thu Jun 24 09:36:21 2021 by ROOT version 6.18/04
// from TTree Events/Events
// found on file: /eos/user/j/jleonhol/cmt/Skim/base_config/ggf_sm/cat_bbtt/prod_2206/NANO_NANO_1.root
//////////////////////////////////////////////////////////

#ifndef TotalTrigger_h
#define TotalTrigger_h

#include <TROOT.h>
#include <TChain.h>
#include <TFile.h>
#include "TH3.h"

#include <unistd.h>
#include <iostream>

// Header file for the classes stored in the TTree if any.

class TotalTrigger {
public :
   TTree          *fChain;   //!pointer to the analyzed TTree or TChain
   Int_t           fCurrent; //!current Tree number in a TChain
   TFile          *outfile;

   int minx = -1, maxx = -1, miny = -1, maxy = -1, minz = -1, maxz = -1, mindz = -1, maxdz = -1;
   int add_to_leading_tau = -1, add_to_subleading_tau = -1, add_to_jet = -1;

// Fixed size dimensions of array or collections stored in the TTree if any.

   // Declaration of leaf types
   UInt_t          nL1Obj;
   Float_t         L1Obj_pt[24];   //[nL1Obj]
   Float_t         L1Obj_eta[24];   //[nL1Obj]
   Float_t         L1Obj_phi[24];   //[nL1Obj]
   Int_t           L1Obj_type[24];   //[nL1Obj]
   Float_t         leading_l1tau_pt;
   Float_t         leading_l1tau_eta;
   Float_t         leading_l1tau_phi;
   Float_t         subleading_l1tau_pt;
   Float_t         subleading_l1tau_eta;
   Float_t         subleading_l1tau_phi;
   
   Float_t         leading_l1jet_pt;
   Float_t         leading_l1jet_eta;
   Float_t         leading_l1jet_phi;
   Float_t         subleading_l1jet_pt;
   Float_t         subleading_l1jet_eta;
   Float_t         subleading_l1jet_phi;
   
   std::map<int, Float_t> leading_l1jet_pt_map;
   std::map<int, Float_t> leading_l1jet_eta_map;
   std::map<int, Float_t> leading_l1jet_phi_map;
   std::map<int, Float_t> subleading_l1jet_pt_map;
   std::map<int, Float_t> subleading_l1jet_eta_map;
   std::map<int, Float_t> subleading_l1jet_phi_map;
   
   Float_t         leading_tau_pt;
   Float_t         leading_tau_eta;
   Float_t         leading_tau_phi;
   Float_t         subleading_tau_pt;
   Float_t         subleading_tau_eta;
   Float_t         subleading_tau_phi;
   
   Float_t         leading_jet_pt;
   Float_t         leading_jet_eta;
   Float_t         leading_jet_phi;
   Float_t         subleading_jet_pt;
   Float_t         subleading_jet_eta;
   Float_t         subleading_jet_phi;
   
   std::map<int, Float_t> leading_jet_pt_map;
   std::map<int, Float_t> leading_jet_eta_map;
   std::map<int, Float_t> leading_jet_phi_map;
   std::map<int, Float_t> subleading_jet_pt_map;
   std::map<int, Float_t> subleading_jet_eta_map;
   std::map<int, Float_t> subleading_jet_phi_map;
   
   Float_t         weight;
   Int_t           eventnumber;

   // List of branches
   TBranch        *b_nL1Obj;   //!
   TBranch        *b_L1Obj_pt;   //!
   TBranch        *b_L1Obj_eta;   //!
   TBranch        *b_L1Obj_phi;   //!
   TBranch        *b_L1Obj_type;   //!
   TBranch        *b_leading_l1tau_pt;   //!
   TBranch        *b_leading_l1tau_eta;   //!
   TBranch        *b_leading_l1tau_phi;   //!
   TBranch        *b_subleading_l1tau_pt;   //!
   TBranch        *b_subleading_l1tau_eta;   //!
   TBranch        *b_subleading_l1tau_phi;   //!
   TBranch        *b_leading_l1jet_pt;   //!
   TBranch        *b_leading_l1jet_eta;   //!
   TBranch        *b_leading_l1jet_phi;   //!
   TBranch        *b_subleading_l1jet_pt;   //!
   TBranch        *b_subleading_l1jet_eta;   //!
   TBranch        *b_subleading_l1jet_phi;   //!
   
   TBranch        *b_leading_l1jet_pt_tau_pt_20;
   TBranch        *b_leading_l1jet_eta_tau_pt_20;
   TBranch        *b_leading_l1jet_phi_tau_pt_20;
   TBranch        *b_subleading_l1jet_pt_tau_pt_20;
   TBranch        *b_subleading_l1jet_eta_tau_pt_20;
   TBranch        *b_subleading_l1jet_phi_tau_pt_20;
   TBranch        *b_leading_l1jet_pt_tau_pt_21;
   TBranch        *b_leading_l1jet_eta_tau_pt_21;
   TBranch        *b_leading_l1jet_phi_tau_pt_21;
   TBranch        *b_subleading_l1jet_pt_tau_pt_21;
   TBranch        *b_subleading_l1jet_eta_tau_pt_21;
   TBranch        *b_subleading_l1jet_phi_tau_pt_21;
   TBranch        *b_leading_l1jet_pt_tau_pt_22;
   TBranch        *b_leading_l1jet_eta_tau_pt_22;
   TBranch        *b_leading_l1jet_phi_tau_pt_22;
   TBranch        *b_subleading_l1jet_pt_tau_pt_22;
   TBranch        *b_subleading_l1jet_eta_tau_pt_22;
   TBranch        *b_subleading_l1jet_phi_tau_pt_22;
   TBranch        *b_leading_l1jet_pt_tau_pt_23;
   TBranch        *b_leading_l1jet_eta_tau_pt_23;
   TBranch        *b_leading_l1jet_phi_tau_pt_23;
   TBranch        *b_subleading_l1jet_pt_tau_pt_23;
   TBranch        *b_subleading_l1jet_eta_tau_pt_23;
   TBranch        *b_subleading_l1jet_phi_tau_pt_23;
   TBranch        *b_leading_l1jet_pt_tau_pt_24;
   TBranch        *b_leading_l1jet_eta_tau_pt_24;
   TBranch        *b_leading_l1jet_phi_tau_pt_24;
   TBranch        *b_subleading_l1jet_pt_tau_pt_24;
   TBranch        *b_subleading_l1jet_eta_tau_pt_24;
   TBranch        *b_subleading_l1jet_phi_tau_pt_24;
   TBranch        *b_leading_l1jet_pt_tau_pt_25;
   TBranch        *b_leading_l1jet_eta_tau_pt_25;
   TBranch        *b_leading_l1jet_phi_tau_pt_25;
   TBranch        *b_subleading_l1jet_pt_tau_pt_25;
   TBranch        *b_subleading_l1jet_eta_tau_pt_25;
   TBranch        *b_subleading_l1jet_phi_tau_pt_25;
   TBranch        *b_leading_l1jet_pt_tau_pt_26;
   TBranch        *b_leading_l1jet_eta_tau_pt_26;
   TBranch        *b_leading_l1jet_phi_tau_pt_26;
   TBranch        *b_subleading_l1jet_pt_tau_pt_26;
   TBranch        *b_subleading_l1jet_eta_tau_pt_26;
   TBranch        *b_subleading_l1jet_phi_tau_pt_26;
   TBranch        *b_leading_l1jet_pt_tau_pt_27;
   TBranch        *b_leading_l1jet_eta_tau_pt_27;
   TBranch        *b_leading_l1jet_phi_tau_pt_27;
   TBranch        *b_subleading_l1jet_pt_tau_pt_27;
   TBranch        *b_subleading_l1jet_eta_tau_pt_27;
   TBranch        *b_subleading_l1jet_phi_tau_pt_27;
   TBranch        *b_leading_l1jet_pt_tau_pt_28;
   TBranch        *b_leading_l1jet_eta_tau_pt_28;
   TBranch        *b_leading_l1jet_phi_tau_pt_28;
   TBranch        *b_subleading_l1jet_pt_tau_pt_28;
   TBranch        *b_subleading_l1jet_eta_tau_pt_28;
   TBranch        *b_subleading_l1jet_phi_tau_pt_28;
   TBranch        *b_leading_l1jet_pt_tau_pt_29;
   TBranch        *b_leading_l1jet_eta_tau_pt_29;
   TBranch        *b_leading_l1jet_phi_tau_pt_29;
   TBranch        *b_subleading_l1jet_pt_tau_pt_29;
   TBranch        *b_subleading_l1jet_eta_tau_pt_29;
   TBranch        *b_subleading_l1jet_phi_tau_pt_29;
   TBranch        *b_leading_l1jet_pt_tau_pt_30;
   TBranch        *b_leading_l1jet_eta_tau_pt_30;
   TBranch        *b_leading_l1jet_phi_tau_pt_30;
   TBranch        *b_subleading_l1jet_pt_tau_pt_30;
   TBranch        *b_subleading_l1jet_eta_tau_pt_30;
   TBranch        *b_subleading_l1jet_phi_tau_pt_30;
   TBranch        *b_leading_l1jet_pt_tau_pt_31;
   TBranch        *b_leading_l1jet_eta_tau_pt_31;
   TBranch        *b_leading_l1jet_phi_tau_pt_31;
   TBranch        *b_subleading_l1jet_pt_tau_pt_31;
   TBranch        *b_subleading_l1jet_eta_tau_pt_31;
   TBranch        *b_subleading_l1jet_phi_tau_pt_31;
   TBranch        *b_leading_l1jet_pt_tau_pt_32;
   TBranch        *b_leading_l1jet_eta_tau_pt_32;
   TBranch        *b_leading_l1jet_phi_tau_pt_32;
   TBranch        *b_subleading_l1jet_pt_tau_pt_32;
   TBranch        *b_subleading_l1jet_eta_tau_pt_32;
   TBranch        *b_subleading_l1jet_phi_tau_pt_32;
   
   TBranch        *b_leading_tau_pt;   //!
   TBranch        *b_leading_tau_eta;   //!
   TBranch        *b_leading_tau_phi;   //!
   TBranch        *b_subleading_tau_pt;   //!
   TBranch        *b_subleading_tau_eta;   //!
   TBranch        *b_subleading_tau_phi;   //!
   
   TBranch        *b_leading_jet_pt;   //!
   TBranch        *b_leading_jet_eta;   //!
   TBranch        *b_leading_jet_phi;   //!
   TBranch        *b_subleading_jet_pt;   //!
   TBranch        *b_subleading_jet_eta;   //!
   TBranch        *b_subleading_jet_phi;   //!
   TBranch        *b_leading_jet_pt_tau_pt_20;
   TBranch        *b_leading_jet_eta_tau_pt_20;
   TBranch        *b_leading_jet_phi_tau_pt_20;
   TBranch        *b_subleading_jet_pt_tau_pt_20;
   TBranch        *b_subleading_jet_eta_tau_pt_20;
   TBranch        *b_subleading_jet_phi_tau_pt_20;
   TBranch        *b_leading_jet_pt_tau_pt_21;
   TBranch        *b_leading_jet_eta_tau_pt_21;
   TBranch        *b_leading_jet_phi_tau_pt_21;
   TBranch        *b_subleading_jet_pt_tau_pt_21;
   TBranch        *b_subleading_jet_eta_tau_pt_21;
   TBranch        *b_subleading_jet_phi_tau_pt_21;
   TBranch        *b_leading_jet_pt_tau_pt_22;
   TBranch        *b_leading_jet_eta_tau_pt_22;
   TBranch        *b_leading_jet_phi_tau_pt_22;
   TBranch        *b_subleading_jet_pt_tau_pt_22;
   TBranch        *b_subleading_jet_eta_tau_pt_22;
   TBranch        *b_subleading_jet_phi_tau_pt_22;
   TBranch        *b_leading_jet_pt_tau_pt_23;
   TBranch        *b_leading_jet_eta_tau_pt_23;
   TBranch        *b_leading_jet_phi_tau_pt_23;
   TBranch        *b_subleading_jet_pt_tau_pt_23;
   TBranch        *b_subleading_jet_eta_tau_pt_23;
   TBranch        *b_subleading_jet_phi_tau_pt_23;
   TBranch        *b_leading_jet_pt_tau_pt_24;
   TBranch        *b_leading_jet_eta_tau_pt_24;
   TBranch        *b_leading_jet_phi_tau_pt_24;
   TBranch        *b_subleading_jet_pt_tau_pt_24;
   TBranch        *b_subleading_jet_eta_tau_pt_24;
   TBranch        *b_subleading_jet_phi_tau_pt_24;
   TBranch        *b_leading_jet_pt_tau_pt_25;
   TBranch        *b_leading_jet_eta_tau_pt_25;
   TBranch        *b_leading_jet_phi_tau_pt_25;
   TBranch        *b_subleading_jet_pt_tau_pt_25;
   TBranch        *b_subleading_jet_eta_tau_pt_25;
   TBranch        *b_subleading_jet_phi_tau_pt_25;
   TBranch        *b_leading_jet_pt_tau_pt_26;
   TBranch        *b_leading_jet_eta_tau_pt_26;
   TBranch        *b_leading_jet_phi_tau_pt_26;
   TBranch        *b_subleading_jet_pt_tau_pt_26;
   TBranch        *b_subleading_jet_eta_tau_pt_26;
   TBranch        *b_subleading_jet_phi_tau_pt_26;
   TBranch        *b_leading_jet_pt_tau_pt_27;
   TBranch        *b_leading_jet_eta_tau_pt_27;
   TBranch        *b_leading_jet_phi_tau_pt_27;
   TBranch        *b_subleading_jet_pt_tau_pt_27;
   TBranch        *b_subleading_jet_eta_tau_pt_27;
   TBranch        *b_subleading_jet_phi_tau_pt_27;
   TBranch        *b_leading_jet_pt_tau_pt_28;
   TBranch        *b_leading_jet_eta_tau_pt_28;
   TBranch        *b_leading_jet_phi_tau_pt_28;
   TBranch        *b_subleading_jet_pt_tau_pt_28;
   TBranch        *b_subleading_jet_eta_tau_pt_28;
   TBranch        *b_subleading_jet_phi_tau_pt_28;
   TBranch        *b_leading_jet_pt_tau_pt_29;
   TBranch        *b_leading_jet_eta_tau_pt_29;
   TBranch        *b_leading_jet_phi_tau_pt_29;
   TBranch        *b_subleading_jet_pt_tau_pt_29;
   TBranch        *b_subleading_jet_eta_tau_pt_29;
   TBranch        *b_subleading_jet_phi_tau_pt_29;
   TBranch        *b_leading_jet_pt_tau_pt_30;
   TBranch        *b_leading_jet_eta_tau_pt_30;
   TBranch        *b_leading_jet_phi_tau_pt_30;
   TBranch        *b_subleading_jet_pt_tau_pt_30;
   TBranch        *b_subleading_jet_eta_tau_pt_30;
   TBranch        *b_subleading_jet_phi_tau_pt_30;
   TBranch        *b_leading_jet_pt_tau_pt_31;
   TBranch        *b_leading_jet_eta_tau_pt_31;
   TBranch        *b_leading_jet_phi_tau_pt_31;
   TBranch        *b_subleading_jet_pt_tau_pt_31;
   TBranch        *b_subleading_jet_eta_tau_pt_31;
   TBranch        *b_subleading_jet_phi_tau_pt_31;
   TBranch        *b_leading_jet_pt_tau_pt_32;
   TBranch        *b_leading_jet_eta_tau_pt_32;
   TBranch        *b_leading_jet_phi_tau_pt_32;
   TBranch        *b_subleading_jet_pt_tau_pt_32;
   TBranch        *b_subleading_jet_eta_tau_pt_32;
   TBranch        *b_subleading_jet_phi_tau_pt_32;
   
   TBranch        *b_weight;   //!
   TBranch        *b_eventnumber;   //!

   TotalTrigger(
     const TString & inSample, const TString & outName, const TString & treename,
     int minx, int maxx,
     int miny, int maxy,
     int minz, int maxz,
     int mindz, int maxdz,
     int add_to_leading_tau, int add_to_subleading_tau, int add_to_jet);
   virtual ~TotalTrigger();
   virtual Int_t    Cut(Long64_t entry);
   virtual Int_t    GetEntry(Long64_t entry);
   virtual Long64_t LoadTree(Long64_t entry);
   virtual void     Init(TTree *tree);
   virtual void     TotalLoop();
   virtual void     AsymmLoop();
   virtual void     AsymmDiJetLoop();
   virtual void     AsymmVBFLoop();
   virtual void     AsymmVBFDiJetLoop();
   virtual void     AsymmKetiLoop();
   virtual void     AsymmKetiDiJetLoop();
   virtual void     RateTotalLoop();
   virtual void     RateAsymmLoop();
   virtual void     RateAsymmDiJetLoop();
   virtual void     RateAsymmVBFLoop();
   virtual void     RateAsymmVBFDiJetLoop();
   virtual void     RateAsymmKetiLoop();
   virtual void     RateAsymmKetiDiJetLoop();
   virtual Bool_t   Notify();
   virtual void     Show(Long64_t entry = -1);
};

#endif

#ifdef TotalTrigger_cxx
TotalTrigger::TotalTrigger(
  const TString & inSample, const TString & outName, const TString & treename,
  int minx, int maxx,
  int miny, int maxy,
  int minz, int maxz,
  int mindz, int maxdz,
  int add_to_leading_tau, int add_to_subleading_tau, int add_to_jet) : fChain(0) 
{
// if parameter tree is not specified (or zero), connect the file
// used to generate this class and read the Tree.
  TFile *f = TFile::Open(inSample, "READ");
  TTree *tree = (TTree*) f->Get(treename);
  Init(tree);
  outfile = TFile::Open(outName, "RECREATE");

  this->minx = minx;
  this->maxx = maxx;
  this->miny = miny;
  this->maxy = maxy;
  this->minz = minz;
  this->maxz = maxz;
  this->mindz = mindz;
  this->maxdz = maxdz;
  this->add_to_leading_tau = add_to_leading_tau;
  this->add_to_subleading_tau = add_to_subleading_tau;
  this->add_to_jet = add_to_jet;
}

TotalTrigger::~TotalTrigger()
{
  outfile->Close();
   if (!fChain) return;
   delete fChain->GetCurrentFile();
}

Int_t TotalTrigger::GetEntry(Long64_t entry)
{
// Read contents of entry.
   if (!fChain) return 0;
   return fChain->GetEntry(entry);
}
Long64_t TotalTrigger::LoadTree(Long64_t entry)
{
// Set the environment to read one entry
   if (!fChain) return -5;
   Long64_t centry = fChain->LoadTree(entry);
   if (centry < 0) return centry;
   if (fChain->GetTreeNumber() != fCurrent) {
      fCurrent = fChain->GetTreeNumber();
      Notify();
   }
   return centry;
}

void TotalTrigger::Init(TTree *tree)
{
   // The Init() function is called when the selector needs to initialize
   // a new tree or chain. Typically here the branch addresses and branch
   // pointers of the tree will be set.
   // It is normally not necessary to make changes to the generated
   // code, but the routine can be extended by the user if needed.
   // Init() will be called many times when running on PROOF
   // (once per file to be processed).

   // Set branch addresses and branch pointers
   if (!tree) return;
   fChain = tree;
   fCurrent = -1;
   fChain->SetMakeClass(1);

   // if (fChain->GetListOfBranches()->FindObject("nL1Obj"))
     // fChain->SetBranchAddress("nL1Obj", &nL1Obj, &b_nL1Obj);
   // if (fChain->GetListOfBranches()->FindObject("L1Obj_pt"))
     // fChain->SetBranchAddress("L1Obj_pt", L1Obj_pt, &b_L1Obj_pt);
   // if (fChain->GetListOfBranches()->FindObject("L1Obj_eta"))
     // fChain->SetBranchAddress("L1Obj_eta", L1Obj_eta, &b_L1Obj_eta);
   // if (fChain->GetListOfBranches()->FindObject("L1Obj_phi"))
     // fChain->SetBranchAddress("L1Obj_phi", L1Obj_phi, &b_L1Obj_phi);
   // if (fChain->GetListOfBranches()->FindObject("L1Obj_type"))
     // fChain->SetBranchAddress("L1Obj_type", L1Obj_type, &b_L1Obj_type);
   fChain->SetBranchAddress("leading_l1tau_pt", &leading_l1tau_pt, &b_leading_l1tau_pt);
   // fChain->SetBranchAddress("leading_l1tau_eta", &leading_l1tau_eta, &b_leading_l1tau_eta);
   // fChain->SetBranchAddress("leading_l1tau_phi", &leading_l1tau_phi, &b_leading_l1tau_phi);
   fChain->SetBranchAddress("subleading_l1tau_pt", &subleading_l1tau_pt, &b_subleading_l1tau_pt);
   // fChain->SetBranchAddress("subleading_l1tau_eta", &subleading_l1tau_eta, &b_subleading_l1tau_eta);
   // fChain->SetBranchAddress("subleading_l1tau_phi", &subleading_l1tau_phi, &b_subleading_l1tau_phi);
   if (fChain->GetListOfBranches()->FindObject("leading_l1jet_pt"))
      fChain->SetBranchAddress("leading_l1jet_pt", &leading_l1jet_pt, &b_leading_l1jet_pt);
   if (fChain->GetListOfBranches()->FindObject("leading_l1jet_eta"))
      fChain->SetBranchAddress("leading_l1jet_eta", &leading_l1jet_eta, &b_leading_l1jet_eta);
   if (fChain->GetListOfBranches()->FindObject("leading_l1jet_phi"))
      fChain->SetBranchAddress("leading_l1jet_phi", &leading_l1jet_phi, &b_leading_l1jet_phi);
   if (fChain->GetListOfBranches()->FindObject("subleading_l1jet_pt"))
      fChain->SetBranchAddress("subleading_l1jet_pt", &subleading_l1jet_pt, &b_subleading_l1jet_pt);
   if (fChain->GetListOfBranches()->FindObject("subleading_l1jet_eta"))
      fChain->SetBranchAddress("subleading_l1jet_eta", &subleading_l1jet_eta, &b_subleading_l1jet_eta);
   if (fChain->GetListOfBranches()->FindObject("subleading_l1jet_phi"))
      fChain->SetBranchAddress("subleading_l1jet_phi", &subleading_l1jet_phi, &b_subleading_l1jet_phi);
   if (fChain->GetListOfBranches()->FindObject("leading_l1jet_pt_tau_pt_20"))
      fChain->SetBranchAddress("leading_l1jet_pt_tau_pt_20", &leading_l1jet_pt_map[20], &b_leading_l1jet_pt_tau_pt_20);
   if (fChain->GetListOfBranches()->FindObject("leading_l1jet_eta_tau_pt_20"))
      fChain->SetBranchAddress("leading_l1jet_eta_tau_pt_20", &leading_l1jet_eta_map[20], &b_leading_l1jet_eta_tau_pt_20);
   if (fChain->GetListOfBranches()->FindObject("leading_l1jet_phi_tau_pt_20"))
      fChain->SetBranchAddress("leading_l1jet_phi_tau_pt_20", &leading_l1jet_phi_map[20], &b_leading_l1jet_phi_tau_pt_20);
   if (fChain->GetListOfBranches()->FindObject("subleading_l1jet_pt_tau_pt_20"))
      fChain->SetBranchAddress("subleading_l1jet_pt_tau_pt_20", &subleading_l1jet_pt_map[20], &b_subleading_l1jet_pt_tau_pt_20);
   if (fChain->GetListOfBranches()->FindObject("subleading_l1jet_eta_tau_pt_20"))
      fChain->SetBranchAddress("subleading_l1jet_eta_tau_pt_20", &subleading_l1jet_eta_map[20], &b_subleading_l1jet_eta_tau_pt_20);
   if (fChain->GetListOfBranches()->FindObject("subleading_l1jet_phi_tau_pt_20"))
      fChain->SetBranchAddress("subleading_l1jet_phi_tau_pt_20", &subleading_l1jet_phi_map[20], &b_subleading_l1jet_phi_tau_pt_20);
   if (fChain->GetListOfBranches()->FindObject("leading_l1jet_pt_tau_pt_21"))
      fChain->SetBranchAddress("leading_l1jet_pt_tau_pt_21", &leading_l1jet_pt_map[21], &b_leading_l1jet_pt_tau_pt_21);
   if (fChain->GetListOfBranches()->FindObject("leading_l1jet_eta_tau_pt_21"))
      fChain->SetBranchAddress("leading_l1jet_eta_tau_pt_21", &leading_l1jet_eta_map[21], &b_leading_l1jet_eta_tau_pt_21);
   if (fChain->GetListOfBranches()->FindObject("leading_l1jet_phi_tau_pt_21"))
      fChain->SetBranchAddress("leading_l1jet_phi_tau_pt_21", &leading_l1jet_phi_map[21], &b_leading_l1jet_phi_tau_pt_21);
   if (fChain->GetListOfBranches()->FindObject("subleading_l1jet_pt_tau_pt_21"))
      fChain->SetBranchAddress("subleading_l1jet_pt_tau_pt_21", &subleading_l1jet_pt_map[21], &b_subleading_l1jet_pt_tau_pt_21);
   if (fChain->GetListOfBranches()->FindObject("subleading_l1jet_eta_tau_pt_21"))
      fChain->SetBranchAddress("subleading_l1jet_eta_tau_pt_21", &subleading_l1jet_eta_map[21], &b_subleading_l1jet_eta_tau_pt_21);
   if (fChain->GetListOfBranches()->FindObject("subleading_l1jet_phi_tau_pt_21"))
      fChain->SetBranchAddress("subleading_l1jet_phi_tau_pt_21", &subleading_l1jet_phi_map[21], &b_subleading_l1jet_phi_tau_pt_21);
   if (fChain->GetListOfBranches()->FindObject("leading_l1jet_pt_tau_pt_22"))
      fChain->SetBranchAddress("leading_l1jet_pt_tau_pt_22", &leading_l1jet_pt_map[22], &b_leading_l1jet_pt_tau_pt_22);
   if (fChain->GetListOfBranches()->FindObject("leading_l1jet_eta_tau_pt_22"))
      fChain->SetBranchAddress("leading_l1jet_eta_tau_pt_22", &leading_l1jet_eta_map[22], &b_leading_l1jet_eta_tau_pt_22);
   if (fChain->GetListOfBranches()->FindObject("leading_l1jet_phi_tau_pt_22"))
      fChain->SetBranchAddress("leading_l1jet_phi_tau_pt_22", &leading_l1jet_phi_map[22], &b_leading_l1jet_phi_tau_pt_22);
   if (fChain->GetListOfBranches()->FindObject("subleading_l1jet_pt_tau_pt_22"))
      fChain->SetBranchAddress("subleading_l1jet_pt_tau_pt_22", &subleading_l1jet_pt_map[22], &b_subleading_l1jet_pt_tau_pt_22);
   if (fChain->GetListOfBranches()->FindObject("subleading_l1jet_eta_tau_pt_22"))
      fChain->SetBranchAddress("subleading_l1jet_eta_tau_pt_22", &subleading_l1jet_eta_map[22], &b_subleading_l1jet_eta_tau_pt_22);
   if (fChain->GetListOfBranches()->FindObject("subleading_l1jet_phi_tau_pt_22"))
      fChain->SetBranchAddress("subleading_l1jet_phi_tau_pt_22", &subleading_l1jet_phi_map[22], &b_subleading_l1jet_phi_tau_pt_22);
   if (fChain->GetListOfBranches()->FindObject("leading_l1jet_pt_tau_pt_23"))
      fChain->SetBranchAddress("leading_l1jet_pt_tau_pt_23", &leading_l1jet_pt_map[23], &b_leading_l1jet_pt_tau_pt_23);
   if (fChain->GetListOfBranches()->FindObject("leading_l1jet_eta_tau_pt_23"))
      fChain->SetBranchAddress("leading_l1jet_eta_tau_pt_23", &leading_l1jet_eta_map[23], &b_leading_l1jet_eta_tau_pt_23);
   if (fChain->GetListOfBranches()->FindObject("leading_l1jet_phi_tau_pt_23"))
      fChain->SetBranchAddress("leading_l1jet_phi_tau_pt_23", &leading_l1jet_phi_map[23], &b_leading_l1jet_phi_tau_pt_23);
   if (fChain->GetListOfBranches()->FindObject("subleading_l1jet_pt_tau_pt_23"))
      fChain->SetBranchAddress("subleading_l1jet_pt_tau_pt_23", &subleading_l1jet_pt_map[23], &b_subleading_l1jet_pt_tau_pt_23);
   if (fChain->GetListOfBranches()->FindObject("subleading_l1jet_eta_tau_pt_23"))
      fChain->SetBranchAddress("subleading_l1jet_eta_tau_pt_23", &subleading_l1jet_eta_map[23], &b_subleading_l1jet_eta_tau_pt_23);
   if (fChain->GetListOfBranches()->FindObject("subleading_l1jet_phi_tau_pt_23"))
      fChain->SetBranchAddress("subleading_l1jet_phi_tau_pt_23", &subleading_l1jet_phi_map[23], &b_subleading_l1jet_phi_tau_pt_23);
   if (fChain->GetListOfBranches()->FindObject("leading_l1jet_pt_tau_pt_24"))
      fChain->SetBranchAddress("leading_l1jet_pt_tau_pt_24", &leading_l1jet_pt_map[24], &b_leading_l1jet_pt_tau_pt_24);
   if (fChain->GetListOfBranches()->FindObject("leading_l1jet_eta_tau_pt_24"))
      fChain->SetBranchAddress("leading_l1jet_eta_tau_pt_24", &leading_l1jet_eta_map[24], &b_leading_l1jet_eta_tau_pt_24);
   if (fChain->GetListOfBranches()->FindObject("leading_l1jet_phi_tau_pt_24"))
      fChain->SetBranchAddress("leading_l1jet_phi_tau_pt_24", &leading_l1jet_phi_map[24], &b_leading_l1jet_phi_tau_pt_24);
   if (fChain->GetListOfBranches()->FindObject("subleading_l1jet_pt_tau_pt_24"))
      fChain->SetBranchAddress("subleading_l1jet_pt_tau_pt_24", &subleading_l1jet_pt_map[24], &b_subleading_l1jet_pt_tau_pt_24);
   if (fChain->GetListOfBranches()->FindObject("subleading_l1jet_eta_tau_pt_24"))
      fChain->SetBranchAddress("subleading_l1jet_eta_tau_pt_24", &subleading_l1jet_eta_map[24], &b_subleading_l1jet_eta_tau_pt_24);
   if (fChain->GetListOfBranches()->FindObject("subleading_l1jet_phi_tau_pt_24"))
      fChain->SetBranchAddress("subleading_l1jet_phi_tau_pt_24", &subleading_l1jet_phi_map[24], &b_subleading_l1jet_phi_tau_pt_24);
   if (fChain->GetListOfBranches()->FindObject("leading_l1jet_pt_tau_pt_25"))
      fChain->SetBranchAddress("leading_l1jet_pt_tau_pt_25", &leading_l1jet_pt_map[25], &b_leading_l1jet_pt_tau_pt_25);
   if (fChain->GetListOfBranches()->FindObject("leading_l1jet_eta_tau_pt_25"))
      fChain->SetBranchAddress("leading_l1jet_eta_tau_pt_25", &leading_l1jet_eta_map[25], &b_leading_l1jet_eta_tau_pt_25);
   if (fChain->GetListOfBranches()->FindObject("leading_l1jet_phi_tau_pt_25"))
      fChain->SetBranchAddress("leading_l1jet_phi_tau_pt_25", &leading_l1jet_phi_map[25], &b_leading_l1jet_phi_tau_pt_25);
   if (fChain->GetListOfBranches()->FindObject("subleading_l1jet_pt_tau_pt_25"))
      fChain->SetBranchAddress("subleading_l1jet_pt_tau_pt_25", &subleading_l1jet_pt_map[25], &b_subleading_l1jet_pt_tau_pt_25);
   if (fChain->GetListOfBranches()->FindObject("subleading_l1jet_eta_tau_pt_25"))
      fChain->SetBranchAddress("subleading_l1jet_eta_tau_pt_25", &subleading_l1jet_eta_map[25], &b_subleading_l1jet_eta_tau_pt_25);
   if (fChain->GetListOfBranches()->FindObject("subleading_l1jet_phi_tau_pt_25"))
      fChain->SetBranchAddress("subleading_l1jet_phi_tau_pt_25", &subleading_l1jet_phi_map[25], &b_subleading_l1jet_phi_tau_pt_25);
   if (fChain->GetListOfBranches()->FindObject("leading_l1jet_pt_tau_pt_26"))
      fChain->SetBranchAddress("leading_l1jet_pt_tau_pt_26", &leading_l1jet_pt_map[26], &b_leading_l1jet_pt_tau_pt_26);
   if (fChain->GetListOfBranches()->FindObject("leading_l1jet_eta_tau_pt_26"))
      fChain->SetBranchAddress("leading_l1jet_eta_tau_pt_26", &leading_l1jet_eta_map[26], &b_leading_l1jet_eta_tau_pt_26);
   if (fChain->GetListOfBranches()->FindObject("leading_l1jet_phi_tau_pt_26"))
      fChain->SetBranchAddress("leading_l1jet_phi_tau_pt_26", &leading_l1jet_phi_map[26], &b_leading_l1jet_phi_tau_pt_26);
   if (fChain->GetListOfBranches()->FindObject("subleading_l1jet_pt_tau_pt_26"))
      fChain->SetBranchAddress("subleading_l1jet_pt_tau_pt_26", &subleading_l1jet_pt_map[26], &b_subleading_l1jet_pt_tau_pt_26);
   if (fChain->GetListOfBranches()->FindObject("subleading_l1jet_eta_tau_pt_26"))
      fChain->SetBranchAddress("subleading_l1jet_eta_tau_pt_26", &subleading_l1jet_eta_map[26], &b_subleading_l1jet_eta_tau_pt_26);
   if (fChain->GetListOfBranches()->FindObject("subleading_l1jet_phi_tau_pt_26"))
      fChain->SetBranchAddress("subleading_l1jet_phi_tau_pt_26", &subleading_l1jet_phi_map[26], &b_subleading_l1jet_phi_tau_pt_26);
   if (fChain->GetListOfBranches()->FindObject("leading_l1jet_pt_tau_pt_27"))
      fChain->SetBranchAddress("leading_l1jet_pt_tau_pt_27", &leading_l1jet_pt_map[27], &b_leading_l1jet_pt_tau_pt_27);
   if (fChain->GetListOfBranches()->FindObject("leading_l1jet_eta_tau_pt_27"))
      fChain->SetBranchAddress("leading_l1jet_eta_tau_pt_27", &leading_l1jet_eta_map[27], &b_leading_l1jet_eta_tau_pt_27);
   if (fChain->GetListOfBranches()->FindObject("leading_l1jet_phi_tau_pt_27"))
      fChain->SetBranchAddress("leading_l1jet_phi_tau_pt_27", &leading_l1jet_phi_map[27], &b_leading_l1jet_phi_tau_pt_27);
   if (fChain->GetListOfBranches()->FindObject("subleading_l1jet_pt_tau_pt_27"))
      fChain->SetBranchAddress("subleading_l1jet_pt_tau_pt_27", &subleading_l1jet_pt_map[27], &b_subleading_l1jet_pt_tau_pt_27);
   if (fChain->GetListOfBranches()->FindObject("subleading_l1jet_eta_tau_pt_27"))
      fChain->SetBranchAddress("subleading_l1jet_eta_tau_pt_27", &subleading_l1jet_eta_map[27], &b_subleading_l1jet_eta_tau_pt_27);
   if (fChain->GetListOfBranches()->FindObject("subleading_l1jet_phi_tau_pt_27"))
      fChain->SetBranchAddress("subleading_l1jet_phi_tau_pt_27", &subleading_l1jet_phi_map[27], &b_subleading_l1jet_phi_tau_pt_27);
   if (fChain->GetListOfBranches()->FindObject("leading_l1jet_pt_tau_pt_28"))
      fChain->SetBranchAddress("leading_l1jet_pt_tau_pt_28", &leading_l1jet_pt_map[28], &b_leading_l1jet_pt_tau_pt_28);
   if (fChain->GetListOfBranches()->FindObject("leading_l1jet_eta_tau_pt_28"))
      fChain->SetBranchAddress("leading_l1jet_eta_tau_pt_28", &leading_l1jet_eta_map[28], &b_leading_l1jet_eta_tau_pt_28);
   if (fChain->GetListOfBranches()->FindObject("leading_l1jet_phi_tau_pt_28"))
      fChain->SetBranchAddress("leading_l1jet_phi_tau_pt_28", &leading_l1jet_phi_map[28], &b_leading_l1jet_phi_tau_pt_28);
   if (fChain->GetListOfBranches()->FindObject("subleading_l1jet_pt_tau_pt_28"))
      fChain->SetBranchAddress("subleading_l1jet_pt_tau_pt_28", &subleading_l1jet_pt_map[28], &b_subleading_l1jet_pt_tau_pt_28);
   if (fChain->GetListOfBranches()->FindObject("subleading_l1jet_eta_tau_pt_28"))
      fChain->SetBranchAddress("subleading_l1jet_eta_tau_pt_28", &subleading_l1jet_eta_map[28], &b_subleading_l1jet_eta_tau_pt_28);
   if (fChain->GetListOfBranches()->FindObject("subleading_l1jet_phi_tau_pt_28"))
      fChain->SetBranchAddress("subleading_l1jet_phi_tau_pt_28", &subleading_l1jet_phi_map[28], &b_subleading_l1jet_phi_tau_pt_28);
   if (fChain->GetListOfBranches()->FindObject("leading_l1jet_pt_tau_pt_29"))
      fChain->SetBranchAddress("leading_l1jet_pt_tau_pt_29", &leading_l1jet_pt_map[29], &b_leading_l1jet_pt_tau_pt_29);
   if (fChain->GetListOfBranches()->FindObject("leading_l1jet_eta_tau_pt_29"))
      fChain->SetBranchAddress("leading_l1jet_eta_tau_pt_29", &leading_l1jet_eta_map[29], &b_leading_l1jet_eta_tau_pt_29);
   if (fChain->GetListOfBranches()->FindObject("leading_l1jet_phi_tau_pt_29"))
      fChain->SetBranchAddress("leading_l1jet_phi_tau_pt_29", &leading_l1jet_phi_map[29], &b_leading_l1jet_phi_tau_pt_29);
   if (fChain->GetListOfBranches()->FindObject("subleading_l1jet_pt_tau_pt_29"))
      fChain->SetBranchAddress("subleading_l1jet_pt_tau_pt_29", &subleading_l1jet_pt_map[29], &b_subleading_l1jet_pt_tau_pt_29);
   if (fChain->GetListOfBranches()->FindObject("subleading_l1jet_eta_tau_pt_29"))
      fChain->SetBranchAddress("subleading_l1jet_eta_tau_pt_29", &subleading_l1jet_eta_map[29], &b_subleading_l1jet_eta_tau_pt_29);
   if (fChain->GetListOfBranches()->FindObject("subleading_l1jet_phi_tau_pt_29"))
      fChain->SetBranchAddress("subleading_l1jet_phi_tau_pt_29", &subleading_l1jet_phi_map[29], &b_subleading_l1jet_phi_tau_pt_29);
   if (fChain->GetListOfBranches()->FindObject("leading_l1jet_pt_tau_pt_30"))
      fChain->SetBranchAddress("leading_l1jet_pt_tau_pt_30", &leading_l1jet_pt_map[30], &b_leading_l1jet_pt_tau_pt_30);
   if (fChain->GetListOfBranches()->FindObject("leading_l1jet_eta_tau_pt_30"))
      fChain->SetBranchAddress("leading_l1jet_eta_tau_pt_30", &leading_l1jet_eta_map[30], &b_leading_l1jet_eta_tau_pt_30);
   if (fChain->GetListOfBranches()->FindObject("leading_l1jet_phi_tau_pt_30"))
      fChain->SetBranchAddress("leading_l1jet_phi_tau_pt_30", &leading_l1jet_phi_map[30], &b_leading_l1jet_phi_tau_pt_30);
   if (fChain->GetListOfBranches()->FindObject("subleading_l1jet_pt_tau_pt_30"))
      fChain->SetBranchAddress("subleading_l1jet_pt_tau_pt_30", &subleading_l1jet_pt_map[30], &b_subleading_l1jet_pt_tau_pt_30);
   if (fChain->GetListOfBranches()->FindObject("subleading_l1jet_eta_tau_pt_30"))
      fChain->SetBranchAddress("subleading_l1jet_eta_tau_pt_30", &subleading_l1jet_eta_map[30], &b_subleading_l1jet_eta_tau_pt_30);
   if (fChain->GetListOfBranches()->FindObject("subleading_l1jet_phi_tau_pt_30"))
      fChain->SetBranchAddress("subleading_l1jet_phi_tau_pt_30", &subleading_l1jet_phi_map[30], &b_subleading_l1jet_phi_tau_pt_30);
   if (fChain->GetListOfBranches()->FindObject("leading_l1jet_pt_tau_pt_31"))
      fChain->SetBranchAddress("leading_l1jet_pt_tau_pt_31", &leading_l1jet_pt_map[31], &b_leading_l1jet_pt_tau_pt_31);
   if (fChain->GetListOfBranches()->FindObject("leading_l1jet_eta_tau_pt_31"))
      fChain->SetBranchAddress("leading_l1jet_eta_tau_pt_31", &leading_l1jet_eta_map[31], &b_leading_l1jet_eta_tau_pt_31);
   if (fChain->GetListOfBranches()->FindObject("leading_l1jet_phi_tau_pt_31"))
      fChain->SetBranchAddress("leading_l1jet_phi_tau_pt_31", &leading_l1jet_phi_map[31], &b_leading_l1jet_phi_tau_pt_31);
   if (fChain->GetListOfBranches()->FindObject("subleading_l1jet_pt_tau_pt_31"))
      fChain->SetBranchAddress("subleading_l1jet_pt_tau_pt_31", &subleading_l1jet_pt_map[31], &b_subleading_l1jet_pt_tau_pt_31);
   if (fChain->GetListOfBranches()->FindObject("subleading_l1jet_eta_tau_pt_31"))
      fChain->SetBranchAddress("subleading_l1jet_eta_tau_pt_31", &subleading_l1jet_eta_map[31], &b_subleading_l1jet_eta_tau_pt_31);
   if (fChain->GetListOfBranches()->FindObject("subleading_l1jet_phi_tau_pt_31"))
      fChain->SetBranchAddress("subleading_l1jet_phi_tau_pt_31", &subleading_l1jet_phi_map[31], &b_subleading_l1jet_phi_tau_pt_31);
   if (fChain->GetListOfBranches()->FindObject("leading_l1jet_pt_tau_pt_32"))
      fChain->SetBranchAddress("leading_l1jet_pt_tau_pt_32", &leading_l1jet_pt_map[32], &b_leading_l1jet_pt_tau_pt_32);
   if (fChain->GetListOfBranches()->FindObject("leading_l1jet_eta_tau_pt_32"))
      fChain->SetBranchAddress("leading_l1jet_eta_tau_pt_32", &leading_l1jet_eta_map[32], &b_leading_l1jet_eta_tau_pt_32);
   if (fChain->GetListOfBranches()->FindObject("leading_l1jet_phi_tau_pt_32"))
      fChain->SetBranchAddress("leading_l1jet_phi_tau_pt_32", &leading_l1jet_phi_map[32], &b_leading_l1jet_phi_tau_pt_32);
   if (fChain->GetListOfBranches()->FindObject("subleading_l1jet_pt_tau_pt_32"))
      fChain->SetBranchAddress("subleading_l1jet_pt_tau_pt_32", &subleading_l1jet_pt_map[32], &b_subleading_l1jet_pt_tau_pt_32);
   if (fChain->GetListOfBranches()->FindObject("subleading_l1jet_eta_tau_pt_32"))
      fChain->SetBranchAddress("subleading_l1jet_eta_tau_pt_32", &subleading_l1jet_eta_map[32], &b_subleading_l1jet_eta_tau_pt_32);
   if (fChain->GetListOfBranches()->FindObject("subleading_l1jet_phi_tau_pt_32"))
      fChain->SetBranchAddress("subleading_l1jet_phi_tau_pt_32", &subleading_l1jet_phi_map[32], &b_subleading_l1jet_phi_tau_pt_32);



   // offline variables, only present in Skim (not L1Skim)
   if (fChain->GetListOfBranches()->FindObject("leading_tau_pt"))
     fChain->SetBranchAddress("leading_tau_pt", &leading_tau_pt, &b_leading_tau_pt);
   // if (fChain->GetListOfBranches()->FindObject("leading_tau_eta"))
     // fChain->SetBranchAddress("leading_tau_eta", &leading_tau_eta, &b_leading_tau_eta);
   // if (fChain->GetListOfBranches()->FindObject("leading_tau_phi"))
     // fChain->SetBranchAddress("leading_tau_phi", &leading_tau_phi, &b_leading_tau_phi);
   if (fChain->GetListOfBranches()->FindObject("subleading_tau_pt"))
     fChain->SetBranchAddress("subleading_tau_pt", &subleading_tau_pt, &b_subleading_tau_pt);
   // if (fChain->GetListOfBranches()->FindObject("subleading_tau_eta"))
     // fChain->SetBranchAddress("subleading_tau_eta", &subleading_tau_eta, &b_subleading_tau_eta);
   // if (fChain->GetListOfBranches()->FindObject("subleading_tau_phi"))
     // fChain->SetBranchAddress("subleading_tau_phi", &subleading_tau_phi, &b_subleading_tau_phi);
   if (fChain->GetListOfBranches()->FindObject("leading_jet_pt"))
     fChain->SetBranchAddress("leading_jet_pt", &leading_jet_pt, &b_leading_jet_pt);
   if (fChain->GetListOfBranches()->FindObject("leading_jet_eta"))
     fChain->SetBranchAddress("leading_jet_eta", &leading_jet_eta, &b_leading_jet_eta);
   if (fChain->GetListOfBranches()->FindObject("leading_jet_phi"))
     fChain->SetBranchAddress("leading_jet_phi", &leading_jet_phi, &b_leading_jet_phi);
   if (fChain->GetListOfBranches()->FindObject("subleading_jet_pt"))
     fChain->SetBranchAddress("subleading_jet_pt", &subleading_jet_pt, &b_subleading_jet_pt);
   if (fChain->GetListOfBranches()->FindObject("subleading_jet_eta"))
     fChain->SetBranchAddress("subleading_jet_eta", &subleading_jet_eta, &b_subleading_jet_eta);
   if (fChain->GetListOfBranches()->FindObject("subleading_jet_phi"))
     fChain->SetBranchAddress("subleading_jet_phi", &subleading_jet_phi, &b_subleading_jet_phi);
   if (fChain->GetListOfBranches()->FindObject("leading_jet_pt_tau_pt_20"))
      fChain->SetBranchAddress("leading_jet_pt_tau_pt_20", &leading_jet_pt_map[20], &b_leading_jet_pt_tau_pt_20);
   if (fChain->GetListOfBranches()->FindObject("leading_jet_eta_tau_pt_20"))
      fChain->SetBranchAddress("leading_jet_eta_tau_pt_20", &leading_jet_eta_map[20], &b_leading_jet_eta_tau_pt_20);
   if (fChain->GetListOfBranches()->FindObject("leading_jet_phi_tau_pt_20"))
      fChain->SetBranchAddress("leading_jet_phi_tau_pt_20", &leading_jet_phi_map[20], &b_leading_jet_phi_tau_pt_20);
   if (fChain->GetListOfBranches()->FindObject("subleading_jet_pt_tau_pt_20"))
      fChain->SetBranchAddress("subleading_jet_pt_tau_pt_20", &subleading_jet_pt_map[20], &b_subleading_jet_pt_tau_pt_20);
   if (fChain->GetListOfBranches()->FindObject("subleading_jet_eta_tau_pt_20"))
      fChain->SetBranchAddress("subleading_jet_eta_tau_pt_20", &subleading_jet_eta_map[20], &b_subleading_jet_eta_tau_pt_20);
   if (fChain->GetListOfBranches()->FindObject("subleading_jet_phi_tau_pt_20"))
      fChain->SetBranchAddress("subleading_jet_phi_tau_pt_20", &subleading_jet_phi_map[20], &b_subleading_jet_phi_tau_pt_20);
   if (fChain->GetListOfBranches()->FindObject("leading_jet_pt_tau_pt_21"))
      fChain->SetBranchAddress("leading_jet_pt_tau_pt_21", &leading_jet_pt_map[21], &b_leading_jet_pt_tau_pt_21);
   if (fChain->GetListOfBranches()->FindObject("leading_jet_eta_tau_pt_21"))
      fChain->SetBranchAddress("leading_jet_eta_tau_pt_21", &leading_jet_eta_map[21], &b_leading_jet_eta_tau_pt_21);
   if (fChain->GetListOfBranches()->FindObject("leading_jet_phi_tau_pt_21"))
      fChain->SetBranchAddress("leading_jet_phi_tau_pt_21", &leading_jet_phi_map[21], &b_leading_jet_phi_tau_pt_21);
   if (fChain->GetListOfBranches()->FindObject("subleading_jet_pt_tau_pt_21"))
      fChain->SetBranchAddress("subleading_jet_pt_tau_pt_21", &subleading_jet_pt_map[21], &b_subleading_jet_pt_tau_pt_21);
   if (fChain->GetListOfBranches()->FindObject("subleading_jet_eta_tau_pt_21"))
      fChain->SetBranchAddress("subleading_jet_eta_tau_pt_21", &subleading_jet_eta_map[21], &b_subleading_jet_eta_tau_pt_21);
   if (fChain->GetListOfBranches()->FindObject("subleading_jet_phi_tau_pt_21"))
      fChain->SetBranchAddress("subleading_jet_phi_tau_pt_21", &subleading_jet_phi_map[21], &b_subleading_jet_phi_tau_pt_21);
   if (fChain->GetListOfBranches()->FindObject("leading_jet_pt_tau_pt_22"))
      fChain->SetBranchAddress("leading_jet_pt_tau_pt_22", &leading_jet_pt_map[22], &b_leading_jet_pt_tau_pt_22);
   if (fChain->GetListOfBranches()->FindObject("leading_jet_eta_tau_pt_22"))
      fChain->SetBranchAddress("leading_jet_eta_tau_pt_22", &leading_jet_eta_map[22], &b_leading_jet_eta_tau_pt_22);
   if (fChain->GetListOfBranches()->FindObject("leading_jet_phi_tau_pt_22"))
      fChain->SetBranchAddress("leading_jet_phi_tau_pt_22", &leading_jet_phi_map[22], &b_leading_jet_phi_tau_pt_22);
   if (fChain->GetListOfBranches()->FindObject("subleading_jet_pt_tau_pt_22"))
      fChain->SetBranchAddress("subleading_jet_pt_tau_pt_22", &subleading_jet_pt_map[22], &b_subleading_jet_pt_tau_pt_22);
   if (fChain->GetListOfBranches()->FindObject("subleading_jet_eta_tau_pt_22"))
      fChain->SetBranchAddress("subleading_jet_eta_tau_pt_22", &subleading_jet_eta_map[22], &b_subleading_jet_eta_tau_pt_22);
   if (fChain->GetListOfBranches()->FindObject("subleading_jet_phi_tau_pt_22"))
      fChain->SetBranchAddress("subleading_jet_phi_tau_pt_22", &subleading_jet_phi_map[22], &b_subleading_jet_phi_tau_pt_22);
   if (fChain->GetListOfBranches()->FindObject("leading_jet_pt_tau_pt_23"))
      fChain->SetBranchAddress("leading_jet_pt_tau_pt_23", &leading_jet_pt_map[23], &b_leading_jet_pt_tau_pt_23);
   if (fChain->GetListOfBranches()->FindObject("leading_jet_eta_tau_pt_23"))
      fChain->SetBranchAddress("leading_jet_eta_tau_pt_23", &leading_jet_eta_map[23], &b_leading_jet_eta_tau_pt_23);
   if (fChain->GetListOfBranches()->FindObject("leading_jet_phi_tau_pt_23"))
      fChain->SetBranchAddress("leading_jet_phi_tau_pt_23", &leading_jet_phi_map[23], &b_leading_jet_phi_tau_pt_23);
   if (fChain->GetListOfBranches()->FindObject("subleading_jet_pt_tau_pt_23"))
      fChain->SetBranchAddress("subleading_jet_pt_tau_pt_23", &subleading_jet_pt_map[23], &b_subleading_jet_pt_tau_pt_23);
   if (fChain->GetListOfBranches()->FindObject("subleading_jet_eta_tau_pt_23"))
      fChain->SetBranchAddress("subleading_jet_eta_tau_pt_23", &subleading_jet_eta_map[23], &b_subleading_jet_eta_tau_pt_23);
   if (fChain->GetListOfBranches()->FindObject("subleading_jet_phi_tau_pt_23"))
      fChain->SetBranchAddress("subleading_jet_phi_tau_pt_23", &subleading_jet_phi_map[23], &b_subleading_jet_phi_tau_pt_23);
   if (fChain->GetListOfBranches()->FindObject("leading_jet_pt_tau_pt_24"))
      fChain->SetBranchAddress("leading_jet_pt_tau_pt_24", &leading_jet_pt_map[24], &b_leading_jet_pt_tau_pt_24);
   if (fChain->GetListOfBranches()->FindObject("leading_jet_eta_tau_pt_24"))
      fChain->SetBranchAddress("leading_jet_eta_tau_pt_24", &leading_jet_eta_map[24], &b_leading_jet_eta_tau_pt_24);
   if (fChain->GetListOfBranches()->FindObject("leading_jet_phi_tau_pt_24"))
      fChain->SetBranchAddress("leading_jet_phi_tau_pt_24", &leading_jet_phi_map[24], &b_leading_jet_phi_tau_pt_24);
   if (fChain->GetListOfBranches()->FindObject("subleading_jet_pt_tau_pt_24"))
      fChain->SetBranchAddress("subleading_jet_pt_tau_pt_24", &subleading_jet_pt_map[24], &b_subleading_jet_pt_tau_pt_24);
   if (fChain->GetListOfBranches()->FindObject("subleading_jet_eta_tau_pt_24"))
      fChain->SetBranchAddress("subleading_jet_eta_tau_pt_24", &subleading_jet_eta_map[24], &b_subleading_jet_eta_tau_pt_24);
   if (fChain->GetListOfBranches()->FindObject("subleading_jet_phi_tau_pt_24"))
      fChain->SetBranchAddress("subleading_jet_phi_tau_pt_24", &subleading_jet_phi_map[24], &b_subleading_jet_phi_tau_pt_24);
   if (fChain->GetListOfBranches()->FindObject("leading_jet_pt_tau_pt_25"))
      fChain->SetBranchAddress("leading_jet_pt_tau_pt_25", &leading_jet_pt_map[25], &b_leading_jet_pt_tau_pt_25);
   if (fChain->GetListOfBranches()->FindObject("leading_jet_eta_tau_pt_25"))
      fChain->SetBranchAddress("leading_jet_eta_tau_pt_25", &leading_jet_eta_map[25], &b_leading_jet_eta_tau_pt_25);
   if (fChain->GetListOfBranches()->FindObject("leading_jet_phi_tau_pt_25"))
      fChain->SetBranchAddress("leading_jet_phi_tau_pt_25", &leading_jet_phi_map[25], &b_leading_jet_phi_tau_pt_25);
   if (fChain->GetListOfBranches()->FindObject("subleading_jet_pt_tau_pt_25"))
      fChain->SetBranchAddress("subleading_jet_pt_tau_pt_25", &subleading_jet_pt_map[25], &b_subleading_jet_pt_tau_pt_25);
   if (fChain->GetListOfBranches()->FindObject("subleading_jet_eta_tau_pt_25"))
      fChain->SetBranchAddress("subleading_jet_eta_tau_pt_25", &subleading_jet_eta_map[25], &b_subleading_jet_eta_tau_pt_25);
   if (fChain->GetListOfBranches()->FindObject("subleading_jet_phi_tau_pt_25"))
      fChain->SetBranchAddress("subleading_jet_phi_tau_pt_25", &subleading_jet_phi_map[25], &b_subleading_jet_phi_tau_pt_25);
   if (fChain->GetListOfBranches()->FindObject("leading_jet_pt_tau_pt_26"))
      fChain->SetBranchAddress("leading_jet_pt_tau_pt_26", &leading_jet_pt_map[26], &b_leading_jet_pt_tau_pt_26);
   if (fChain->GetListOfBranches()->FindObject("leading_jet_eta_tau_pt_26"))
      fChain->SetBranchAddress("leading_jet_eta_tau_pt_26", &leading_jet_eta_map[26], &b_leading_jet_eta_tau_pt_26);
   if (fChain->GetListOfBranches()->FindObject("leading_jet_phi_tau_pt_26"))
      fChain->SetBranchAddress("leading_jet_phi_tau_pt_26", &leading_jet_phi_map[26], &b_leading_jet_phi_tau_pt_26);
   if (fChain->GetListOfBranches()->FindObject("subleading_jet_pt_tau_pt_26"))
      fChain->SetBranchAddress("subleading_jet_pt_tau_pt_26", &subleading_jet_pt_map[26], &b_subleading_jet_pt_tau_pt_26);
   if (fChain->GetListOfBranches()->FindObject("subleading_jet_eta_tau_pt_26"))
      fChain->SetBranchAddress("subleading_jet_eta_tau_pt_26", &subleading_jet_eta_map[26], &b_subleading_jet_eta_tau_pt_26);
   if (fChain->GetListOfBranches()->FindObject("subleading_jet_phi_tau_pt_26"))
      fChain->SetBranchAddress("subleading_jet_phi_tau_pt_26", &subleading_jet_phi_map[26], &b_subleading_jet_phi_tau_pt_26);
   if (fChain->GetListOfBranches()->FindObject("leading_jet_pt_tau_pt_27"))
      fChain->SetBranchAddress("leading_jet_pt_tau_pt_27", &leading_jet_pt_map[27], &b_leading_jet_pt_tau_pt_27);
   if (fChain->GetListOfBranches()->FindObject("leading_jet_eta_tau_pt_27"))
      fChain->SetBranchAddress("leading_jet_eta_tau_pt_27", &leading_jet_eta_map[27], &b_leading_jet_eta_tau_pt_27);
   if (fChain->GetListOfBranches()->FindObject("leading_jet_phi_tau_pt_27"))
      fChain->SetBranchAddress("leading_jet_phi_tau_pt_27", &leading_jet_phi_map[27], &b_leading_jet_phi_tau_pt_27);
   if (fChain->GetListOfBranches()->FindObject("subleading_jet_pt_tau_pt_27"))
      fChain->SetBranchAddress("subleading_jet_pt_tau_pt_27", &subleading_jet_pt_map[27], &b_subleading_jet_pt_tau_pt_27);
   if (fChain->GetListOfBranches()->FindObject("subleading_jet_eta_tau_pt_27"))
      fChain->SetBranchAddress("subleading_jet_eta_tau_pt_27", &subleading_jet_eta_map[27], &b_subleading_jet_eta_tau_pt_27);
   if (fChain->GetListOfBranches()->FindObject("subleading_jet_phi_tau_pt_27"))
      fChain->SetBranchAddress("subleading_jet_phi_tau_pt_27", &subleading_jet_phi_map[27], &b_subleading_jet_phi_tau_pt_27);
   if (fChain->GetListOfBranches()->FindObject("leading_jet_pt_tau_pt_28"))
      fChain->SetBranchAddress("leading_jet_pt_tau_pt_28", &leading_jet_pt_map[28], &b_leading_jet_pt_tau_pt_28);
   if (fChain->GetListOfBranches()->FindObject("leading_jet_eta_tau_pt_28"))
      fChain->SetBranchAddress("leading_jet_eta_tau_pt_28", &leading_jet_eta_map[28], &b_leading_jet_eta_tau_pt_28);
   if (fChain->GetListOfBranches()->FindObject("leading_jet_phi_tau_pt_28"))
      fChain->SetBranchAddress("leading_jet_phi_tau_pt_28", &leading_jet_phi_map[28], &b_leading_jet_phi_tau_pt_28);
   if (fChain->GetListOfBranches()->FindObject("subleading_jet_pt_tau_pt_28"))
      fChain->SetBranchAddress("subleading_jet_pt_tau_pt_28", &subleading_jet_pt_map[28], &b_subleading_jet_pt_tau_pt_28);
   if (fChain->GetListOfBranches()->FindObject("subleading_jet_eta_tau_pt_28"))
      fChain->SetBranchAddress("subleading_jet_eta_tau_pt_28", &subleading_jet_eta_map[28], &b_subleading_jet_eta_tau_pt_28);
   if (fChain->GetListOfBranches()->FindObject("subleading_jet_phi_tau_pt_28"))
      fChain->SetBranchAddress("subleading_jet_phi_tau_pt_28", &subleading_jet_phi_map[28], &b_subleading_jet_phi_tau_pt_28);
   if (fChain->GetListOfBranches()->FindObject("leading_jet_pt_tau_pt_29"))
      fChain->SetBranchAddress("leading_jet_pt_tau_pt_29", &leading_jet_pt_map[29], &b_leading_jet_pt_tau_pt_29);
   if (fChain->GetListOfBranches()->FindObject("leading_jet_eta_tau_pt_29"))
      fChain->SetBranchAddress("leading_jet_eta_tau_pt_29", &leading_jet_eta_map[29], &b_leading_jet_eta_tau_pt_29);
   if (fChain->GetListOfBranches()->FindObject("leading_jet_phi_tau_pt_29"))
      fChain->SetBranchAddress("leading_jet_phi_tau_pt_29", &leading_jet_phi_map[29], &b_leading_jet_phi_tau_pt_29);
   if (fChain->GetListOfBranches()->FindObject("subleading_jet_pt_tau_pt_29"))
      fChain->SetBranchAddress("subleading_jet_pt_tau_pt_29", &subleading_jet_pt_map[29], &b_subleading_jet_pt_tau_pt_29);
   if (fChain->GetListOfBranches()->FindObject("subleading_jet_eta_tau_pt_29"))
      fChain->SetBranchAddress("subleading_jet_eta_tau_pt_29", &subleading_jet_eta_map[29], &b_subleading_jet_eta_tau_pt_29);
   if (fChain->GetListOfBranches()->FindObject("subleading_jet_phi_tau_pt_29"))
      fChain->SetBranchAddress("subleading_jet_phi_tau_pt_29", &subleading_jet_phi_map[29], &b_subleading_jet_phi_tau_pt_29);
   if (fChain->GetListOfBranches()->FindObject("leading_jet_pt_tau_pt_30"))
      fChain->SetBranchAddress("leading_jet_pt_tau_pt_30", &leading_jet_pt_map[30], &b_leading_jet_pt_tau_pt_30);
   if (fChain->GetListOfBranches()->FindObject("leading_jet_eta_tau_pt_30"))
      fChain->SetBranchAddress("leading_jet_eta_tau_pt_30", &leading_jet_eta_map[30], &b_leading_jet_eta_tau_pt_30);
   if (fChain->GetListOfBranches()->FindObject("leading_jet_phi_tau_pt_30"))
      fChain->SetBranchAddress("leading_jet_phi_tau_pt_30", &leading_jet_phi_map[30], &b_leading_jet_phi_tau_pt_30);
   if (fChain->GetListOfBranches()->FindObject("subleading_jet_pt_tau_pt_30"))
      fChain->SetBranchAddress("subleading_jet_pt_tau_pt_30", &subleading_jet_pt_map[30], &b_subleading_jet_pt_tau_pt_30);
   if (fChain->GetListOfBranches()->FindObject("subleading_jet_eta_tau_pt_30"))
      fChain->SetBranchAddress("subleading_jet_eta_tau_pt_30", &subleading_jet_eta_map[30], &b_subleading_jet_eta_tau_pt_30);
   if (fChain->GetListOfBranches()->FindObject("subleading_jet_phi_tau_pt_30"))
      fChain->SetBranchAddress("subleading_jet_phi_tau_pt_30", &subleading_jet_phi_map[30], &b_subleading_jet_phi_tau_pt_30);
   if (fChain->GetListOfBranches()->FindObject("leading_jet_pt_tau_pt_31"))
      fChain->SetBranchAddress("leading_jet_pt_tau_pt_31", &leading_jet_pt_map[31], &b_leading_jet_pt_tau_pt_31);
   if (fChain->GetListOfBranches()->FindObject("leading_jet_eta_tau_pt_31"))
      fChain->SetBranchAddress("leading_jet_eta_tau_pt_31", &leading_jet_eta_map[31], &b_leading_jet_eta_tau_pt_31);
   if (fChain->GetListOfBranches()->FindObject("leading_jet_phi_tau_pt_31"))
      fChain->SetBranchAddress("leading_jet_phi_tau_pt_31", &leading_jet_phi_map[31], &b_leading_jet_phi_tau_pt_31);
   if (fChain->GetListOfBranches()->FindObject("subleading_jet_pt_tau_pt_31"))
      fChain->SetBranchAddress("subleading_jet_pt_tau_pt_31", &subleading_jet_pt_map[31], &b_subleading_jet_pt_tau_pt_31);
   if (fChain->GetListOfBranches()->FindObject("subleading_jet_eta_tau_pt_31"))
      fChain->SetBranchAddress("subleading_jet_eta_tau_pt_31", &subleading_jet_eta_map[31], &b_subleading_jet_eta_tau_pt_31);
   if (fChain->GetListOfBranches()->FindObject("subleading_jet_phi_tau_pt_31"))
      fChain->SetBranchAddress("subleading_jet_phi_tau_pt_31", &subleading_jet_phi_map[31], &b_subleading_jet_phi_tau_pt_31);
   if (fChain->GetListOfBranches()->FindObject("leading_jet_pt_tau_pt_32"))
      fChain->SetBranchAddress("leading_jet_pt_tau_pt_32", &leading_jet_pt_map[32], &b_leading_jet_pt_tau_pt_32);
   if (fChain->GetListOfBranches()->FindObject("leading_jet_eta_tau_pt_32"))
      fChain->SetBranchAddress("leading_jet_eta_tau_pt_32", &leading_jet_eta_map[32], &b_leading_jet_eta_tau_pt_32);
   if (fChain->GetListOfBranches()->FindObject("leading_jet_phi_tau_pt_32"))
      fChain->SetBranchAddress("leading_jet_phi_tau_pt_32", &leading_jet_phi_map[32], &b_leading_jet_phi_tau_pt_32);
   if (fChain->GetListOfBranches()->FindObject("subleading_jet_pt_tau_pt_32"))
      fChain->SetBranchAddress("subleading_jet_pt_tau_pt_32", &subleading_jet_pt_map[32], &b_subleading_jet_pt_tau_pt_32);
   if (fChain->GetListOfBranches()->FindObject("subleading_jet_eta_tau_pt_32"))
      fChain->SetBranchAddress("subleading_jet_eta_tau_pt_32", &subleading_jet_eta_map[32], &b_subleading_jet_eta_tau_pt_32);
   if (fChain->GetListOfBranches()->FindObject("subleading_jet_phi_tau_pt_32"))
      fChain->SetBranchAddress("subleading_jet_phi_tau_pt_32", &subleading_jet_phi_map[32], &b_subleading_jet_phi_tau_pt_32);

   if (fChain->GetListOfBranches()->FindObject("weight"))
     fChain->SetBranchAddress("weight", &weight, &b_weight);
   if (fChain->GetListOfBranches()->FindObject("eventnumber"))
     fChain->SetBranchAddress("eventnumber", &eventnumber, &b_eventnumber);
   
   Notify();
}

Bool_t TotalTrigger::Notify()
{
   // The Notify() function is called when a new file is opened. This
   // can be either for a new TTree in a TChain or when when a new TTree
   // is started when using PROOF. It is normally not necessary to make changes
   // to the generated code, but the routine can be extended by the
   // user if needed. The return value is currently not used.

   return kTRUE;
}

void TotalTrigger::Show(Long64_t entry)
{
// Print contents of entry.
// If entry is not specified, print current entry
   if (!fChain) return;
   fChain->Show(entry);
}
Int_t TotalTrigger::Cut(Long64_t entry)
{
// This function may be called from Loop.
// returns  1 if entry is accepted.
// returns -1 otherwise.
   return 1;
}
#endif // #ifdef TotalTrigger_cxx
