# Installation (on lxplus)

```
git clone https://gitlab.cern.ch/jaimeleonh/triggertau.git -b tautrigger
cd triggertau
source setup.sh
law index --verbose
```

Compile cpp code needed:
```
root -b
gROOT->ProcessLine(".L cmt/tasks/TotalTrigger.C++");
.q
```


After starting a new session, you always need to do ``` source setup.sh ```
