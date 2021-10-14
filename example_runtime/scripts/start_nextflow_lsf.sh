#!/usr/bin/env bash

# activate Nextflow conda env
conda init bash
eval "$(conda shell.bash hook)"
conda activate nextflow

# run nextflow main.nf with inputs and lsf config:
export NXF_OPTS="-Xms5G -Xmx5G"
nextflow run ./pipelines/main.nf \
	 -c ./nextflow.config \
	 -c inputs.nf \
	 -c sql_credentials.conf \
	 -profile lsf \
	 --nf_ci_loc $PWD -resume
