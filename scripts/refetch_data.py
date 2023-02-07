import pandas as pd
import os
cwd1 = os.getcwd()
To_Fetch = pd.read_csv(f"results/cellranger_irods_objects.csv")
os.mkdir('results/iget_study_cellranger')

for i,row1 in To_Fetch.iterrows():
    try:
        os.mkdir(f'results/iget_study_cellranger/{row1.study_id}')
    except:
        _='exists'
    try:
        os.mkdir(f'results/iget_study_cellranger/{row1.study_id}/{row1.sanger_sample_id}/')
    except:
        _='exists'
    os.system(f"cd {cwd1}/results/iget_study_cellranger/{row1.study_id}/{row1.sanger_sample_id} && bsub -R'select[mem>20000] rusage[mem=20000]' -J refetch_{row1.sanger_sample_id}  -n 1 -M 20000 -o fetch.o -e fetch.e -q normal iget -r -K -f -v {row1.cellranger_irods_object} cellranger_{row1.sanger_sample_id}")
print('Done')