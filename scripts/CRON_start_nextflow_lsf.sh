#!/usr/bin/env bash

# activate Nextflow conda env
/software/hgi/installs/anaconda3/bin/conda init bash
eval "$(conda shell.bash hook)"
/software/hgi/installs/anaconda3/bin/conda activate nextflow

# clean up previous run files
rm -f *.log
rm -f nextflow.nohup.PID.txt 

# start Nextflow in background:
export NXF_OPTS="-Xms5G -Xmx5G"
nextflow run ./nf_irods_to_lustre/pipelines/main.nf \
      -c ./nf_irods_to_lustre/nextflow.config --resume -c ./nf_irods_to_lustre/scripts/inputs.nf -profile lsf \
      -with-dag flowchart.png -with-report report.html --resume --nf_ci_loc $PWD > nextflow.nohup.log

echo DONE
