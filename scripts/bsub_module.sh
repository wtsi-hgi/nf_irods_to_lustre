#!/usr/bin/env bash
INPUT_FILE="$@"
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
sample='Fetch'
bsub -R'select[mem>4000] rusage[mem=4000]' -J $sample -n 1 -M 4000 -o $sample.o -e $sample.e -q normal bash $SCRIPT_DIR/module_nohup_start_nextflow_lsf.sh $INPUT_FILE