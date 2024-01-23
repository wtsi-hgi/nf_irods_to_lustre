#!/usr/bin/env python


__date__ = '2022-07-20'
__version__ = '0.0.1'
import os
import pandas as pd
from datetime import date,datetime
import argparse
import mysql.connector

try:
    import imp
    mod = imp.load_source("module_name", '/path/to/credentials/file/mlwpw.py')
    PASSWORD = mod.PASSWORD
    USERNAME = mod.USERNAME
    PORT = mod.PORT
    HOST = mod.HOST
    DATABASE = mod.DATABASE
except:
    PASSWORD = ''
    USERNAME = ''   
    PORT = ''
    HOST = ''
    DATABASE = ''


"""Run CLI."""
parser = argparse.ArgumentParser(
    description="""
        Place all the files in the correct places.
        """
)

parser.add_argument(
    '-i', '--input',
    dest='input',
    required=False,
    default='run',
    help='Input mode'
)

options = parser.parse_args()
type_of_run = options.input
# type_of_run='run'
# This code runs the imeta querry and determines which files have already been run. 
# It further creates required folders in each of the analysis folders - 
# i.e:
# 1) Fech folder
# 2) QC folder
# 3) Cardinal Analysis folder
prefix='/lustre/scratch123/hgi/projects/ukbb_scrna/pipelines/Pilot_UKB/fetch/SETUP_fech_input_prep'

# We check for the already processed samples so we do not refech the same data.
Already_Analysed_Files = pd.read_csv(f'{prefix}/All_Processed2.tsv')
all_samples_previous = Already_Analysed_Files['sanger_sample_id']+'___'+Already_Analysed_Files['Run_ID'].astype(str)
# New_Samples=Already_Analysed_Files
# Querry irods for all the available runs and preform the analysis on which one is which
output_stream = os.popen('imeta qu -z /seq -C study_id = 6776')
data = output_stream.read()
data = pd.DataFrame(data.split('\n'))
d3 = pd.DataFrame(data)
df = data[data[0].str.startswith('collection')]
all_samples = df[0].str.split('/').str[-1].str.split('_').str[3:5].str.join("_")
all_run_ids = pd.DataFrame(df[0].str.split('/').str[-1].str.split('_').str[2])
all_run_ids['ide'] = all_samples
all_samples_irods = all_run_ids['ide']+'___'+all_run_ids[0]
# all_run_ids.to_csv(f"{prefix}/All_Processed2.tsv")
# all_run_ids['idx'] = all_samples
# new2  =all_run_ids.drop_duplicates('idx')

# all_run_ids[all_run_ids.duplicated('idx')]
# all_run_ids[all_run_ids.idx == 'CRD_CMB13041064']
# Check all the new samples
All_Processed = Already_Analysed_Files.iloc[:,0]
New_Samples = set(all_samples_irods) - set(all_samples_previous)
len(set(all_samples))
if type_of_run=='test':
    # For TEST purposes we are just picking the first available 2 samples
    New_Samples = all_samples_previous[1:2]
    
today = date.today()
now = datetime.now() 
d4 = today.strftime("%b_%d_%Y")
d5 = now.strftime("%b_%d_%Y, %H:%M:%S")
f = open("/path/to/your/crons/log/file/CRON_LOG.log", "a")
f.write(f"CRON LOGS:\n")
f.write(f"Jobs exacuted on :{d5}\n")
f.close()

if (len(New_Samples)==0):
    print("No new samples")
else:
    print(f"{len(New_Samples)} new samples")
    New_Samples = pd.DataFrame(New_Samples)
    New_Samples.columns = ['sanger_sample_id']
    New_Samples.index = New_Samples['sanger_sample_id'].str.split('___').str[0]
    New_Samples['Run_ID']= New_Samples['sanger_sample_id'].str.split('___').str[1]
    New_Samples = New_Samples.drop('sanger_sample_id',axis=1)
    # New_Samples= New_Samples.reset_index()
    # for loop here


    # New_Samples.to_csv('/lustre/scratch123/hgi/projects/ukbb_scrna/pipelines/Pilot_UKB/fetch/SETUP_fech_input_prep/analysed.bin')
    if type_of_run=='test':
        # for TEST run we just make the run as a random digit 
        New_Samples['Run_ID']= 'TEST_12345'
    else:
        print('real run')
        
    
    for Run_ID1 in set(New_Samples.Run_ID):
        mydb = mysql.connector.connect(
            host=HOST,port=PORT,
            user=USERNAME,
            passwd=PASSWORD,
            database=DATABASE,
            auth_plugin='mysql_native_password'
        )
        mycursor2 = mydb.cursor()
        sql = f"SELECT DISTINCT sample.name FROM mlwarehouse.iseq_flowcell \
        JOIN mlwarehouse.sample ON iseq_flowcell.id_sample_tmp = sample.id_sample_tmp \
        JOIN mlwarehouse.study ON iseq_flowcell.id_study_tmp = study.id_study_tmp \
        JOIN mlwarehouse.iseq_product_metrics ON iseq_flowcell.id_iseq_flowcell_tmp = iseq_product_metrics.id_iseq_flowcell_tmp \
        JOIN mlwarehouse.iseq_run on iseq_run.id_run = iseq_product_metrics.id_run \
        JOIN mlwarehouse.iseq_run_lane_metrics on iseq_run_lane_metrics.id_run = iseq_run.id_run \
        JOIN mlwarehouse.psd_sample_compounds_components pscc ON iseq_flowcell.id_sample_tmp = pscc.compound_id_sample_tmp \
        JOIN mlwarehouse.sample as donors ON donors.id_sample_tmp = pscc.component_id_sample_tmp \
        JOIN mlwarehouse.stock_resource ON donors.id_sample_tmp = stock_resource.id_sample_tmp \
        JOIN mlwarehouse.study as original_study ON original_study.id_study_tmp = stock_resource.id_study_tmp \
        WHERE iseq_product_metrics.id_run LIKE ('{Run_ID1}')"
        mycursor2.execute(sql)
        Reviewed_metadata = pd.DataFrame(mycursor2.fetchall())
        if not Reviewed_metadata.empty:
            field_names = [i[0] for i in mycursor2.description]
            Reviewed_metadata.columns = field_names
        
        New_Samples_Run = New_Samples[New_Samples.Run_ID == Run_ID1]
        New_Samples_Run2 = pd.DataFrame(New_Samples_Run.index)
        # Here we add the samples that are successfuly retried to exist in irods and the number matches expected number of samples.
        if (len(set(Reviewed_metadata.name) - set(New_Samples_Run.index))==0):


            print("##########################")
            # if this is not a test case and multiple new runs have been deposited to IRODS we want to split them out in a different Tranches
            print(f"Have set up a directory structure for Cardinal_{Run_ID1}_{d4} and trigerred the Fech run at: \n 1) /lustre/scratch123/hgi/projects/ukbb_scrna/pipelines/Pilot_UKB/fetch/Cardinal_{Run_ID1}_{d4} \n When this is completed please triger qc step in: \n 2) /lustre/scratch123/hgi/projects/ukbb_scrna/pipelines/Pilot_UKB/qc/Cardinal_{Run_ID1}_{d4}")
            print(f"To do this please run - \n 3) cd /lustre/scratch123/hgi/projects/ukbb_scrna/pipelines/Pilot_UKB/qc/Cardinal_{Run_ID1}_{d4} && bash ./scripts/nohup_start_nextflow_lsf.sh")
            print("##########################\n")
            # Run_ID1 = f'_m_{Run_ID1}'
            os.mkdir(f"/lustre/scratch123/hgi/projects/ukbb_scrna/pipelines/Pilot_UKB/fetch/Cardinal_{Run_ID1}_{d4}")
            os.mkdir(f"/lustre/scratch123/hgi/projects/cardinal_analysis/qc/Cardinal_{Run_ID1}_{d4}")
            os.mkdir(f"/lustre/scratch123/hgi/projects/ukbb_scrna/pipelines/Pilot_UKB/qc/Cardinal_{Run_ID1}_{d4}")
            New_Samples_Run2.to_csv(f"/lustre/scratch123/hgi/projects/ukbb_scrna/pipelines/Pilot_UKB/fetch/Cardinal_{Run_ID1}_{d4}/input.tsv",index=False)
            os.system(f"cd /lustre/scratch123/hgi/projects/ukbb_scrna/pipelines/Pilot_UKB/fetch/Cardinal_{Run_ID1}_{d4} && git clone -b add_sql_metadata https://github.com/wtsi-hgi/nf_irods_to_lustre.git")
            os.system(f"cd /lustre/scratch123/hgi/projects/ukbb_scrna/pipelines/Pilot_UKB/qc/Cardinal_{Run_ID1}_{d4} && git clone https://github.com/wtsi-hgi/yascp.git")
            os.system(f"cp -r ./scripts /lustre/scratch123/hgi/projects/ukbb_scrna/pipelines/Pilot_UKB/qc/Cardinal_{Run_ID1}_{d4}")
            os.system(f"cp ./inputs.nf /lustre/scratch123/hgi/projects/ukbb_scrna/pipelines/Pilot_UKB/qc/Cardinal_{Run_ID1}_{d4}")
            os.system(f"cp ./sample_qc.yml /lustre/scratch123/hgi/projects/ukbb_scrna/pipelines/Pilot_UKB/qc/Cardinal_{Run_ID1}_{d4}")
            # for backup purposes we keep the samples already analysed in a different directory.
            os.system(f'cp All_Processed.tsv ./processed_runs/{d4}_Backup_All_Processed.tsv')
            New_Samples_Run_adding = New_Samples_Run.reset_index()
            Already_Analysed_Files = pd.concat([Already_Analysed_Files,New_Samples_Run_adding])
            # Now Triger the tasks and send users an email
            os.system(f'nohup bash {prefix}/Cardinal_Exacute.sh {Run_ID1} {d4}  > /lustre/scratch123/hgi/projects/ukbb_scrna/pipelines/Pilot_UKB/fetch/Cardinal_{Run_ID1}_{d4}_nextflow.nohup.log 2>&1 &')
            f = open("/lustre/scratch123/hgi/projects/ukbb_scrna/pipelines/Pilot_UKB/fetch/SETUP_fech_input_prep/CRON_LOG.log", "a")
            f.write(f"Subbmitted: {Run_ID1} for analysis with {len(set(New_Samples_Run.index))} samples\n")
            f.close()
            Already_Analysed_Files.to_csv(f'{prefix}/All_Processed2.tsv',index=False)
        else:
            print(f'For run_ID {Run_ID1} only {len(set(New_Samples_Run.index))}/{len(set(Reviewed_metadata.name))} deposited in irods, please wait till everything is deposited and come back later.')
            f = open("/lustre/scratch123/hgi/projects/ukbb_scrna/pipelines/Pilot_UKB/fetch/SETUP_fech_input_prep/CRON_LOG.log", "a")
            f.write(f"For run_ID {Run_ID1} only {len(set(New_Samples_Run.index))}/{len(set(Reviewed_metadata.name))} deposited in irods, please wait till everything is deposited and come back later.\n")
            f.close()
# Add in samples that are processed.
