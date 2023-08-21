process 'iget_study_cellranger' {
    tag "$sample"
    publishDir "${params.outdir}/iget_study_cellranger/${study_id}/${run_id}/${sample}/", mode: "${params.copy_mode}"
    
    when: 
    params.run_iget_study_cellranger

    input:
    tuple val(study_id), val(sample), val(cellranger_irods_object),val(run_id)
    
  output:
    tuple val(study_id), val(sample), path("cellranger_${sample}/*"), emit: study_sample_cellranger
    tuple val(sample), env(FILT_DIR), env(FILT_BARCODES), env(FILT_H5), env(BAM_FILE), emit: cellranger_filtered_outputs
    tuple val(sample), env(RAW_DIR), env(RAW_BARCODES), env(RAW_H5), env(BAM_FILE), emit: cellranger_raw_outputs
    path("${sample}.metadata.tsv"), emit: cellranger_metadata_tsv

  script:
    """
echo pwd is \${PWD}

iget -r -K -f -v ${cellranger_irods_object} cellranger_${sample}
echo first iget done
ls -ltra
! test -d cellranger_${sample} && sleep 10 && echo retry cellranger 1 && iget -r -K -f -v ${cellranger_irods_object} cellranger_${sample} || true
! test -d cellranger_${sample} && sleep 10 && echo retry cellranger 2 && iget -r -K -f -v ${cellranger_irods_object} cellranger_${sample} || true
! test -d cellranger_${sample} && echo get cellranger directory failed && exit 1 || true 
echo all iget done
ls -ltra

echo \"${cellranger_irods_object}\" > cellranger_${sample}/irods_cellranger_path.txt

# parse cellranger output file hierarchy (it depends on cellranger version) for filtered data:
RESULTS_DIR=${params.outdir}/iget_study_cellranger/${study_id}/${sample}
FILT_BARCODES=\$RESULTS_DIR/\$(find -L . | grep 'barcodes.tsv' | grep 'filtered_.*_bc_matr' | cut -c 3-)
FILT_DIR=\$(dirname \$FILT_BARCODES)
FILT_H5=\$RESULTS_DIR/\$(find . | grep 'filtered_.*_bc_matr.*.h5\$' | cut -c 3-)
BAM_FILE=\$RESULTS_DIR/\$(find . | grep 'possorted_genome_bam.bam\$'  | cut -c 3-)

echo RESULTS_DIR is \$RESULTS_DIR
echo FILT_BARCODES is \$FILT_BARCODES
echo FILT_DIR is \$FILT_DIR
echo FILT_H5 is \$FILT_H5
echo BAM_FILE is \$BAM_FILE

# parse cellranger output file hierarchy (it depends on cellranger version) for raw data:
RAW_BARCODES=\$RESULTS_DIR/\$(find -L . | grep 'barcodes.tsv' | grep 'raw_.*_bc_matr' | cut -c 3-)
RAW_DIR=\$(dirname \$RAW_BARCODES)
RAW_H5=\$RESULTS_DIR/\$(find . | grep 'raw_.*_bc_matr.*.h5\$' | cut -c 3-)

echo RAW_BARCODES is \$RAW_BARCODES
echo RAW_DIR is \$RAW_DIR
echo RAW_H5 is \$RAW_H5

# prepare metadata tsv row for that sample:
echo sanger_sample_id,experiment_id,irods_cellranger_path > metadata1.csv 
echo ${sample},${sample},${cellranger_irods_object} >> metadata1.csv 
cat cellranger_${sample}/metrics_summary.csv | $workflow.projectDir/../bin/remove_comma_inside_quotes.sh  > metadata2.csv
paste -d, metadata1.csv metadata2.csv | tr ',' '\t' > ${sample}.metadata.tsv
rm metadata1.csv 
rm metadata2.csv 

echo script done
   """
}

// paste -d, metadata1.csv cellranger_${sample}/metrics_summary.csv | tr ',' '\\t' 
