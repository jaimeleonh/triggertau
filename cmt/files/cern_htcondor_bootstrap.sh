#!/usr/bin/env bash

action() {
    # on the CERN HTCondor batch, the PATH variable is changed even though "getenv" is set
    # in the job file, so set the PATH manually to the desired same value
    export PATH="{{cmt_env_path}}"

    # set the CMT_ON_HTCONDOR which is recognized by the setup script below
    export CMT_ON_HTCONDOR="1"

    # source the main setup
    source "{{cmt_base}}/setup.sh" ""
}
action "$@"
