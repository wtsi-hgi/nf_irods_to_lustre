#!/usr/bin/env bash

# activate Nextflow conda env
source /software/hgi/installs/anaconda3/etc/profile.d/conda.sh
conda init bash
eval "$(conda shell.bash hook)"
conda activate nextflow
# clean up previous run files
rm -f *.log
rm -f nextflow.nohup.PID.txt 

# start Nextflow in background:
export NXF_OPTS="-Xms5G -Xmx5G"
nohup nextflow run /software/hgi/pipelines/yascp_nf_irods_to_lustre_versions/yascp_nf_irods_to_lustre_1.1/pipelines/main.nf \
      -c /software/hgi/pipelines/yascp_nf_irods_to_lustre_versions/yascp_nf_irods_to_lustre_1.1/nextflow.config -c /software/hgi/pipelines/yascp_nf_irods_to_lustre_versions/yascp_nf_irods_to_lustre_1.1/scripts/inputs.nf -profile lsf \
      -with-dag flowchart.png -with-report report.html -resume --nf_ci_loc $PWD > nextflow.nohup.log 2>&1 & 

# get process PID 
sleep 1 && export PID=$(pgrep -f "\\-\\-nf_ci_loc $RUN_DIR")
echo $PID > nextflow.nohup.PID.txt
echo "Nextflow PID is $PID (saved in ./nextflow.nohup.PID.txt)" 
echo kill with \"kill $PID\"
echo "check logs files nextflow.nohup.log and .nextflow.log"
