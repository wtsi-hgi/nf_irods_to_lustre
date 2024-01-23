#!/usr/bin/env bash
sample='Fetch'
bsub -R'select[mem>4000] rusage[mem=4000]' -J $sample -n 1 -M 4000 -o $sample.o -e $sample.e -q normal bash nf_irods_to_lustre/scripts/nohup_start_nextflow_lsf.sh