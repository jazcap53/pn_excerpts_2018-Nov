#!/usr/bin/env bash

# file: control_script_bash.sh
# andrew jarcho
# 2018-07-22

# at intervals, pulls data from the Marketplace and Crunchbase APIs

next_run_dat=${1}

echo -n 'Next run will be at: ';echo ${next_run_dat}

if [[ $# -eq 2 ]]
then
    modified_since=${2}
else
    modified_since=$(date -d "2000-01-01" +'%Y-%m-%d')
fi

echo -n 'Next run modified date will be: ';echo ${modified_since}

chmod 0755 ./mktplc_export_lics/src/load_licenses.py ./crunchbase_orgs/src/load_organizations.py

now_dat=`date +'%Y-%m-%dT%H:%M:%S'`
while [[ ${now_dat} < ${next_run_dat} ]]
do
    echo -n 'The time is now: ';echo ${now_dat}
    sleep 60  # call home every minute
    now_dat=`date +'%Y-%m-%dT%H:%M:%S'`
done

source ./mktplc_export_lics/admin/set_envs.sh
source ./crunchbase_orgs/admin/set_envs.sh

while true
do
    rm -f ./json_files/crunchbase_orgs_input_3.json

    python3.6 ./mktplc_export_lics/src/load_licenses.py -o ./json_files/crunchbase_orgs_input_3.json -m ${modified_since}

    echo
    echo "==============================================================================="
    echo

    sleep 10

    python3.6 ./crunchbase_orgs/src/load_organizations.py -i ./json_files/crunchbase_orgs_input_3.json

    echo
    echo "==============================================================================="
    echo

    modified_since=$(date -d "${next_run_dat}" +'%Y-%m-%d')
    next_run_dat=$(date -d "${next_run_dat} 30 minutes" +'%Y-%m-%dT%H:%M:%S')

    echo -n 'Next run will be at: ';echo ${next_run_dat}
    echo -n 'Next run modified date will be: ';echo ${modified_since}

    now_dat=`date +'%Y-%m-%dT%H:%M:%S'`
    while [[ ${now_dat} < ${next_run_dat} ]]
    do
        echo -n 'The time is now: ';echo ${now_dat}
        sleep 120  # call home every 2 minutes
        now_dat=`date +'%Y-%m-%dT%H:%M:%S'`
    done

done
