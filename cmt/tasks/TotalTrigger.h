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

   int minx, maxx, miny, maxy, minz, maxz, mindz, maxdz;
   int add_to_leading_tau = -1, add_to_subleading_tau = -1, add_to_jet = -1;

// Fixed size dimensions of array or collections stored in the TTree if any.

   // Declaration of leaf types
   UInt_t          nL1Obj;
   Float_t         L1Obj_pt[24];   //[nL1Obj]
   Float_t         L1Obj_eta[24];   //[nL1Obj]
   Float_t         L1Obj_phi[24];   //[nL1Obj]
   Int_t           L1Obj_type[24];   //[nL1Obj]
   Float_t         leading_l1tau_pt;
   Float_t         subleading_l1tau_pt;
   Float_t         leading_l1jet_pt;
   Float_t         subleading_l1jet_pt;
   Float_t         leading_tau_pt;
   Float_t         subleading_tau_pt;
   Float_t         leading_jet_pt;
   Float_t         subleading_jet_pt;

   // List of branches
   TBranch        *b_nL1Obj;   //!
   TBranch        *b_L1Obj_pt;   //!
   TBranch        *b_L1Obj_eta;   //!
   TBranch        *b_L1Obj_phi;   //!
   TBranch        *b_L1Obj_type;   //!
   TBranch        *b_leading_l1tau_pt;   //!
   TBranch        *b_subleading_l1tau_pt;   //!
   TBranch        *b_leading_l1jet_pt;   //!
   TBranch        *b_subleading_l1jet_pt;   //!
   TBranch        *b_leading_tau_pt;   //!
   TBranch        *b_subleading_tau_pt;   //!
   TBranch        *b_leading_jet_pt;   //!
   TBranch        *b_subleading_jet_pt;   //!

   TotalTrigger(
     const TString & inSample, const TString & outName, const TString & treename,
     int minx, int maxx,
     int miny, int maxy,
     int minz, int maxz,
     int mindz, int maxdz,
     int add_to_leading_tau, int add_to_subleading_tau, int add_to_jet);
   TotalTrigger(
     const TString & inSample, const TString & outName, const TString & treename,
     int minx, int maxx,
     int miny, int maxy,
     int minz, int maxz,
     int mindz, int maxdz);
   virtual ~TotalTrigger();
   virtual Int_t    Cut(Long64_t entry);
   virtual Int_t    GetEntry(Long64_t entry);
   virtual Long64_t LoadTree(Long64_t entry);
   virtual void     Init(TTree *tree);
   virtual void     Loop();
   virtual void     RateLoop();
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

TotalTrigger::TotalTrigger(
  const TString & inSample, const TString & outName, const TString & treename,
  int minx, int maxx,
  int miny, int maxy,
  int minz, int maxz,
  int mindz, int maxdz) : fChain(0) 
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

   fChain->SetBranchAddress("nL1Obj", &nL1Obj, &b_nL1Obj);
   fChain->SetBranchAddress("L1Obj_pt", L1Obj_pt, &b_L1Obj_pt);
   fChain->SetBranchAddress("L1Obj_eta", L1Obj_eta, &b_L1Obj_eta);
   fChain->SetBranchAddress("L1Obj_phi", L1Obj_phi, &b_L1Obj_phi);
   fChain->SetBranchAddress("L1Obj_type", L1Obj_type, &b_L1Obj_type);
   fChain->SetBranchAddress("leading_l1tau_pt", &leading_l1tau_pt, &b_leading_l1tau_pt);
   fChain->SetBranchAddress("subleading_l1tau_pt", &subleading_l1tau_pt, &b_subleading_l1tau_pt);
   fChain->SetBranchAddress("leading_l1jet_pt", &leading_l1jet_pt, &b_leading_l1jet_pt);
   fChain->SetBranchAddress("subleading_l1jet_pt", &subleading_l1jet_pt, &b_subleading_l1jet_pt);
   if (fChain->GetListOfBranches()->FindObject("leading_tau_pt"))
     fChain->SetBranchAddress("leading_tau_pt", &leading_tau_pt, &b_leading_tau_pt);
   if (fChain->GetListOfBranches()->FindObject("subleading_tau_pt"))
     fChain->SetBranchAddress("subleading_tau_pt", &subleading_tau_pt, &b_subleading_tau_pt);
   if (fChain->GetListOfBranches()->FindObject("leading_jet_pt"))
     fChain->SetBranchAddress("leading_jet_pt", &leading_jet_pt, &b_leading_jet_pt);
   if (fChain->GetListOfBranches()->FindObject("subleading_jet_pt"))
     fChain->SetBranchAddress("subleading_jet_pt", &subleading_jet_pt, &b_subleading_jet_pt);
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
