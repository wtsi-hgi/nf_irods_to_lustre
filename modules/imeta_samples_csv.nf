process imeta_samples_csv {
    cache false
    tag "${input_csv}"
    publishDir "${params.outdir}/imeta_study/study_id_${study_id}/", mode: 'copy', pattern: "samples.tsv", overwrite: true
    publishDir "${params.outdir}/", mode: 'copy', pattern: "samples.tsv", overwrite: true
    publishDir "${params.outdir}/imeta_study/study_id_${study_id}/", mode: 'copy', pattern: "samples_noduplicates.tsv", overwrite: true
    publishDir "${params.outdir}/", mode: 'copy', pattern: "samples_noduplicates.tsv", overwrite: true
    
    when: 
    params.run_imeta_samples

    input: 
    path(input_csv)
    val(irods_sample_column)

    output: 
    tuple env(study_id), path('samples.tsv'), emit: irods_samples_tsv
    env(WORK_DIR), emit: work_dir_to_remove
    tuple env(study_id), path('samples_noduplicates.tsv'), emit: samples_noduplicates_tsv
    env(study_id), emit: study_id

    script:
    """

        export input_csv=$input_csv
        export column_samples="sanger_sample_id"
        echo input csv table is \$input_csv
        echo column samples is \$column_samples

        export samples_json_array=\$(csvcut -c "\$column_samples" \$input_csv | \\
                        sed 1d | uniq | jq -R -s -c 'split("\\n")[:-1]')
        echo samples_json_array is \$samples_json_array

        rm -f samples.tmp.tsv
        rm -f samples.tsv
        printf 'sample\\tobject\\tid_run\\tis_paired_read\\tstudy_id\\tstudy\\n' > samples_noduplicates.tsv

        jq --argjson samples_json_array \$samples_json_array -n '{avus: [
            {attribute: "sample", value: \$samples_json_array, o: "in"}
            ]}' | \\
            baton-metaquery --zone seq --obj --avu | \\
        jq '.[] as \$a| 
        "\\(\$a.avus | .[] | select(.attribute == "target") | .value)____\\(\$a.avus | .[] | select(.attribute == "manual_qc") | .value)____\\(\$a.avus | .[] | select(.attribute == "sample") | .value)____\\(\$a.collection)/\\(\$a.data_object)____\\(\$a.avus | .[] | select(.attribute == "id_run") | .value)____\\(\$a.avus | .[] | select(.attribute == "is_paired_read") | .value)____\\(\$a.avus | .[] | select(.attribute == "study_id") | .value)____\\(\$a.avus | .[] | select(.attribute == "study") | .value)"' |\\
            sed s"/\$(printf '\\t')//"g |\\
            sed s"/\\"//"g |\\
            sed s"/____/\$(printf '\\t')/"g |\\
        sort | uniq | grep -P "^1\\t1" >> samples.tmp.tsv && echo 'Done1'

        cat samples.tmp.tsv | awk '{print substr(\$0, index(\$0, \$3))}' > samples.tsv 
        rm samples.tmp.tsv


        echo  'Process any path-based samples (check for paths in input and add them)'
        while IFS= read -r line; do
            if [[ "\$line" =~ "/" ]]; then
                # Treat as a path and check if it exists in iRODS using ils
                if ils "\$line" >/dev/null 2>&1; then
                    echo "Found valid iRODS path: \$line"
                    # Extract sample name and object path
                    sample_name=\$(basename "\$line")
                    object_path=\$(dirname "\$line")
                    printf "\$line\\t\$line\\t\$line\\t\$line\\t\$line\\t\$line\\n" >> samples_noduplicates.tsv
                else
                    echo "Path does not exist in iRODS: \$line"
                fi
            fi
        done < <(csvcut -c "\$column_samples" "\$input_csv" | sed 1d | uniq)

        echo jq search study id done
        echo see samples.tsv

        awk '!a[\$1]++' samples.tsv >> samples_noduplicates.tsv 
        rm samples.tsv
        cp samples_noduplicates.tsv samples.tsv
        # Save work dir so that it can be removed onComplete of workflow, 
        # to ensure that this task Irods search is re-run on each run NF run, 
        # in case new sequencing samples are ready: 
        WORK_DIR=\$PWD
        study_id=tsv

        # capture process environment
        set +u
        echo study_id=\${study_id[@]} > .command.env
        echo WORK_DIR=\${WORK_DIR[@]} >> .command.env
        echo study_id=\${study_id[@]} >> .command.env
        echo study_id=\${study_id[@]} >> .command.env

    """
}
// awk removes duplicates as one sanger sample can have several run_id
