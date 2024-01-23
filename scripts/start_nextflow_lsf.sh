#!/usr/bin/env bash

# activate Nextflow conda env
conda init bash
eval "$(conda shell.bash hook)"
conda activate nextflow

# run nextflow main.nf with inputs and lsf config:
export NXF_OPTS="-Xms5G -Xmx5G"
nextflow run ./nf_irods_to_lustre/pipelines/main.nf \
      -c ./nf_irods_to_lustre/nextflow.config -c inputs.nf -profile lsf \
      -with-dag flowchart.png -with-report report.html --nf_ci_loc $PWD -resume
