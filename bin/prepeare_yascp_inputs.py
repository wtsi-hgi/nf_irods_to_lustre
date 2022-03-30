#!/usr/bin/env python


__date__ = '2022-04-01'
__version__ = '0.0.1'

import argparse
import pandas as pd
import mysql.connector

try:
    import imp
    mod = imp.load_source("module_name", '/lustre/scratch123/hgi/projects/ukbb_scrna/pipelines/Pilot_UKB/secret/mlwpw.py')
    PASSWORD = mod.PASSWORD
    USERNAME = mod.USERNAME
    PORT = mod.PORT
    HOST = mod.HOST
    DATABASE = mod.DATABASE
except:
    print('failed')
    PASSWORD = ''
    USERNAME = ''   
    PORT = ''
    HOST = ''
    DATABASE = ''
print(PASSWORD)
def main():
    """Run CLI."""
    parser = argparse.ArgumentParser(
        description="""
            Produces an input file for the yascp pipeline, by taking the sanger_sample_ids from the raw.file_paths_10x.tsv
            """
    )
    parser.add_argument(
        '-v', '--version',
        action='version',
        version='%(prog)s {version}'.format(version=__version__)
    )
    parser.add_argument(
         '-f10x', '--file_paths_10x',
        action='store',
        dest='file_paths_10x',
        required=True,
        help='Path to 10x data.'
    )

    options = parser.parse_args()
    # Get clean names
    file_paths_10x = options.file_paths_10x
    Data = pd.read_csv(file_paths_10x,sep='\t')
    Search_IDs = '\''+'\',\''.join(set(Data.experiment_id))+'\''
    Data['n_pooled']='1' #add these if availabe in mlwh
    Data['donor_vcf_ids']='none1' #add these if availabe in mlwh
    # do the mysql fetch for extra metadata.
    # if available add the number of samples pooled and produce the correct input format.
    if (USERNAME!=''):
        # if we have successfuly sourced the credentials for the mlwh then get all the extra metadata.
        mydb = mysql.connector.connect(
            host=HOST,port=PORT,
            user=USERNAME,
            passwd=PASSWORD,
            database=DATABASE,
            auth_plugin='mysql_native_password'
        )
        mycursor = mydb.cursor()
        sql = f"SELECT DISTINCT sample.sanger_sample_id,sample.cohort, iseq_flowcell.id_study_tmp, study.id_study_lims, study.last_updated, sample.public_name, sample.donor_id, iseq_run_lane_metrics.instrument_model, iseq_run_lane_metrics.instrument_external_name, iseq_run_lane_metrics.instrument_name \
            FROM mlwarehouse.iseq_flowcell \
            JOIN mlwarehouse.sample ON iseq_flowcell.id_sample_tmp = sample.id_sample_tmp \
            JOIN mlwarehouse.study ON iseq_flowcell.id_study_tmp = study.id_study_tmp \
            JOIN mlwarehouse.iseq_product_metrics ON iseq_flowcell.id_iseq_flowcell_tmp = iseq_product_metrics.id_iseq_flowcell_tmp \
            JOIN mlwarehouse.iseq_run on iseq_run.id_run = iseq_product_metrics.id_run \
            JOIN mlwarehouse.iseq_run_lane_metrics on iseq_run_lane_metrics.id_run = iseq_run.id_run \
            WHERE sample.sanger_sample_id IN ({Search_IDs});"
        mycursor.execute(sql)
        Reviewed_metadata = pd.DataFrame(mycursor.fetchall())
        if not Reviewed_metadata.empty:
            field_names = [i[0] for i in mycursor.description]
            Reviewed_metadata.columns = field_names
        Reviewed_metadata.to_csv('Extra_Metadata.tsv',sep='\t', index=False)
    # Data2 = Data.loc['experiment_id','n_pooled','donor_vcf_ids','data_path_10x_format']
    Data2 = Data.loc[:,['experiment_id','n_pooled','donor_vcf_ids','data_path_10x_format']]
    Data2.to_csv('input.tsv',sep='\t', index=False)
    

if __name__ == '__main__':
    main()