process yascp_input{

  publishDir "${params.outdir}/yascp_inputs", mode: 'copy', overwrite: true

  input:
    path(in10x_paths)
  output:
    path('*.tsv')
  script: 
  """  

    echo '2'
    python $workflow.projectDir/../bin/prepeare_yascp_inputs.py -f10x ${in10x_paths}
  """

}