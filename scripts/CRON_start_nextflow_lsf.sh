#!/usr/bin/env bash
export SHELL=/bin/bash
export BASH_ENV=/lustre/scratch123/hgi/projects/ukbb_scrna/pipelines/Pilot_UKB/fetch/SETUP_fech_input_prep/.bashrc_conda
export PATH="/nfs/users/nfs_m/mercury/mercuryrc/host/hgi-farm5/bin:/software/hgi/installs/genestack/bin:/software/hgi/installs/symlink-fixer/wrapper:/software/hgi/installs/racket/bin:/software/hgi/installs/baton:/software/sciops/pkgg/samtools/1.10+42_g3c4b380+irods_4.2.7/bin:/software/sciops/pkgg/bcftools/1.10.2+irods_4.2.7/bin:/software/sciops/pkgg/baton/2.0.1+1da6bc5bd75b49a2f27d449afeb659cf6ec1b513/bin:.:/nfs/users/nfs_m/mercury/mercuryrc/farm/farm5/bin:/lustre/scratch123/hgi/mdt2/teams/hgi/mo11/.vscode-server/bin/6d9b74a70ca9c7733b29f0456fd8195364076dda/bin/remote-cli:/nfs/users/nfs_m/mercury/mercuryrc/host/hgi-farm5/bin:/software/hgi/installs/genestack/bin:/software/hgi/installs/symlink-fixer/wrapper:/software/hgi/installs/racket/bin:/software/hgi/installs/baton:/software/sciops/pkgg/samtools/1.10+42_g3c4b380+irods_4.2.7/bin:/software/sciops/pkgg/bcftools/1.10.2+irods_4.2.7/bin:/software/sciops/pkgg/baton/2.0.1+1da6bc5bd75b49a2f27d449afeb659cf6ec1b513/bin:.:/software/singularity-v3.6.4/bin:/nfs/users/nfs_m/mercury/mercuryrc/farm/farm5/bin:/usr/lib/oracle/12.1/client64/bin:/software/lsf-farm5/10.1/linux3.10-glibc2.17-x86_64/etc:/software/lsf-farm5/10.1/linux3.10-glibc2.17-x86_64/bin:/software/hgi/installs/anaconda3/condabin:/nfs/users/nfs_m/mercury/mercuryrc/host/hgi-farm5/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games:/snap/bin:/bin:/software/bin:/software/hgi/installs/vault/users"

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
