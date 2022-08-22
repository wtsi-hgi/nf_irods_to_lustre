#!/usr/bin/env bash
SHELL=/bin/bash
BASH_ENV=/lustre/scratch123/hgi/projects/ukbb_scrna/pipelines/Pilot_UKB/fetch/SETUP_fech_input_prep/.bashrc_conda
# activate Nextflow conda env
eval "$(cat /lustre/scratch123/hgi/projects/ukbb_scrna/pipelines/Pilot_UKB/fetch/SETUP_fech_input_prep/.bashrc_conda | tail -n +10)"
conda init bash
eval "$(conda shell.bash hook)"

conda activate nextflow

# clean up previous run files
rm -f *.log
rm -f nextflow.nohup.PID.txt 
which bsub
# start Nextflow in background:
export NXF_OPTS="-Xms5G -Xmx5G"
nextflow run $PWD/nf_irods_to_lustre/pipelines/main.nf \
      -c $PWD/nf_irods_to_lustre/nextflow.config --resume -c $PWD/nf_irods_to_lustre/scripts/inputs.nf -profile lsf \
      -with-dag flowchart.png -with-report report.html --resume --nf_ci_loc $PWD > nextflow.nohup.log

echo DONE
