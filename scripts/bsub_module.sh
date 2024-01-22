#!/usr/bin/env bash
sample='Fetch'
bsub -R'select[mem>4000] rusage[mem=4000]' -J $sample -n 1 -M 4000 -o $sample.o -e $sample.e -q basement bash /software/hgi/pipelines/yascp_nf_irods_to_lustre_versions/yascp_nf_irods_to_lustre_1.1/scripts/module_nohup_start_nextflow_lsf.sh