process imeta_study_cellranger {
    tag "${sample} ${run_id} ${study_id}"
    
    publishDir "${params.outdir}/multiple_cellranger/${clean_study_id}/", pattern: "${clean_sample}.mult.cellranger.irods.txt", mode: "copy"
    
    when: 
    params.run_imeta_study_cellranger

    input: 
      tuple val(study_id), val(sample), val(run_id)

    output: 
      tuple val(study_id), val(clean_sample), val(clean_run_id), path(".command__*.env"), env(WORK_DIR), emit: study_id_sample_cellranger_object
      env(WORK_DIR), emit: work_dir_to_remove
      tuple val(study_id), val(clean_sample), val(clean_run_id), path("${clean_sample}.mult.cellranger.irods.txt"), emit: study_id_sample_mutiple_cellranger optional true

    script:
     clean_study_id = study_id.contains('/') ? study_id.tokenize('/')[-1] : study_id
     clean_run_id = run_id.contains('/') ? run_id.tokenize('/')[-1] : run_id
     clean_sample = sample.contains('/') ? sample.tokenize('/')[-1] : sample

    """
    bash $workflow.projectDir/../bin/imeta_study_cellranger.sh ${sample} ${clean_run_id}
    export sample='${sample}'
    export run_id='${clean_run_id}'
    export sample_name=\$(basename "${sample}")
    export run_id_2digits=\$(echo \${run_id} | head -c2)
    echo run_id is \$run_id
    echo run_id_2digits is \$run_id_2digits

    echo looking for cellranger data
    ils /seq/\${run_id}/cellranger/ | grep \${sample_name} > \${sample_name}.cellranger.found_in_irods.txt || true
    ils /seq/illumina/\${run_id_2digits}/\${run_id}/cellranger/ | grep \${sample_name} >> \${sample_name}.cellranger.found_in_irods.txt || true
    ils /seq/illumina/runs/\${run_id_2digits}/\${run_id}/cellranger/ | grep \${sample_name} >> \${sample_name}.cellranger.found_in_irods.txt || true
    ils /seq/illumina/cellranger/ | grep \${sample_name} >> \${sample_name}.cellranger.found_in_irods.txt || true
    ils \${sample}/.. | grep \${run_id} >> \${sample_name}.cellranger.found_in_irods.txt || true
    imeta qu -z /seq -C sample = \${sample_name} | grep cellranger | grep \${run_id} >> \${sample_name}.cellranger.found_in_irods.txt || true

    if [ -s \${sample_name}.cellranger.found_in_irods.txt ] 
    then 
            echo cellranger data found \${sample_name}
            cat \${sample_name}.cellranger.found_in_irods.txt  | awk '{print \$2}' >> cellranger.object_dub.txt
    else
            echo cellranger data not found
    fi
    echo end cellranger fetch
    sort -u cellranger.object_dub.txt > cellranger.object.txt
    rm -f \${sample_name}.cellranger.found_in_irods.txt cellranger.object_dub.txt

    if [ -f cellranger.object.txt ] 
    then 
        echo file cellranger.object.txt found
        if [[ \$(wc -l <cellranger.object.txt) -ge 2 ]]
        then
          echo \"warning: more than one cellranger output found in Irods for sample ${clean_sample}:\"
          cat cellranger.object.txt | sort | tail -n 1 > ${clean_sample}.mult.cellranger.irods.txt
        else
          echo \"One and only one cellranger output found in Irods for sample ${clean_sample}\"
        fi

        WORK_DIR=dont_remove
        COUNTER=0
        while IFS= read -r line; do
          echo "\$line" > .command__\${COUNTER}.env
          echo WORK_DIR=\${WORK_DIR[@]} >> .command__\${COUNTER}.env
          echo WORK_DIR=\${WORK_DIR[@]} >> .command__\${COUNTER}.env
          COUNTER=\$[\$COUNTER +1]
        done < \"cellranger.object.txt\"
        rm cellranger.object.txt
    else
        echo not found file cellranger.object.txt
        CELLRANGER_IRODS_OBJECT=cellranger_irods_not_found
        WORK_DIR=\$PWD
    fi
    """
}
