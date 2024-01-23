#!/usr/bin/env bash

# activate Nextflow conda env
conda init bash
eval "$(conda shell.bash hook)"
conda activate nextflow

# clean up previous run files
rm -f *.log
rm -f nextflow.nohup.PID.txt 

# start Nextflow in background:
export NXF_OPTS="-Xms5G -Xmx5G"
nohup nextflow run ./nf_irods_to_lustre/pipelines/main.nf \
      -c ./nf_irods_to_lustre/nextflow.config -c ./nf_irods_to_lustre/scripts/inputs.nf -profile lsf \
      -with-dag flowchart.png -with-report report.html -resume --nf_ci_loc $PWD > nextflow.nohup.log 2>&1 & 

# get process PID 
sleep 1 && export PID=$(pgrep -f "\\-\\-nf_ci_loc $RUN_DIR")
echo $PID > nextflow.nohup.PID.txt
echo "Nextflow PID is $PID (saved in ./nextflow.nohup.PID.txt)" 
echo kill with \"kill $PID\"
echo "check logs files nextflow.nohup.log and .nextflow.log"
