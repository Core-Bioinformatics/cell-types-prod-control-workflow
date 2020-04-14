#!/usr/bin/env nextflow 

process run_garnett_workflow {
    conda "${baseDir}/envs/nextflow.yaml"
    errorStrategy { task.exitStatus == 130 || task.exitStatus == 137  ? 'retry' : 'finish' }   
    maxRetries 5
    memory { 16.GB * task.attempt }

    output:
        file("garnett_output.tsv") into GARNETT_OUTPUT 

    """
    RESULTS_DIR=\$PWD

    nextflow run $PROD_WORKFLOWS/garnett-prod-workflow/main.nf\
                        -profile cluster\
                        --query_10x_dir ${params.query_expr_data}\
                        --results_dir \$RESULTS_DIR\
                        --classifiers ${params.garnett.classifiers}\
                        --cluster_extend ${params.garnett.cluster_extend}\
                        --cds_gene_id_type ${params.garnett.cds_gene_id_type}\
                        --database ${params.garnett.database}\
                        --rank_prob_ratio ${params.garnett.rank_prob_ratio}\
                        --predicted_cell_type_field ${params.garnett.predicted_cell_type_field}\
                        --label_dir ${params.garnett.label_dir}
    """
}

process run_scmap_cluster_workflow {
    conda "${baseDir}/envs/nextflow.yaml"
    errorStrategy { task.exitStatus == 130 || task.exitStatus == 137  ? 'retry' : 'finish' }   
    maxRetries 5
    memory { 16.GB * task.attempt }

    output:
        file("scmap-clust_output.tsv") into SCMAP_CLUST_OUTPUT

    """
    RESULTS_DIR=\$PWD

    nextflow run $PROD_WORKFLOWS/scmap-prod-workflow/main.nf\
                        -profile cluster\
                        --results_dir \$RESULTS_DIR\
                        --clust_idx ${params.scmap_cluster.clust_idx}\
                        --query_10x_dir ${params.query_expr_data}\
                        --projection_method ${params.scmap_cluster.projection_method}\
                        --col_names ${params.scmap_cluster.col_names}\
                        --pred_threshold ${params.scmap_cluster.pred_threshold}\
                        --cluster_col ${params.scmap_cluster.cluster_col}\
                        --label_dir ${params.scmap_cluster.label_dir}
    """ 
}

process run_scmap_cell_workflow {
    conda "${baseDir}/envs/nextflow.yaml"
    errorStrategy { task.exitStatus == 130 || task.exitStatus == 137  ? 'retry' : 'finish' }   
    maxRetries 5
    memory { 16.GB * task.attempt }

    output:
        file("scmap-cell_output.tsv") into SCMAP_CELL_OUTPUT

    """
    RESULTS_DIR=\$PWD

    nextflow run $PROD_WORKFLOWS/scmap-prod-workflow/main.nf\
                        -profile cluster\
                        --results_dir \$RESULTS_DIR\
                        --cell_idx ${params.scmap_cell.cell_idx}\
                        --query_10x_dir ${params.query_expr_data}\
                        --projection_method ${params.scmap_cell.projection_method}\
                        --col_names ${params.scmap_cell.col_names}\
                        --pred_threshold ${params.scmap_cell.pred_threshold}\
                        --cluster_col ${params.scmap_cell.cluster_col}\
                        --label_dir ${params.scmap_cell.label_dir}
    """ 
}

process run_scpred_workflow {
    conda "${baseDir}/envs/nextflow.yaml"
    errorStrategy { task.exitStatus == 130 || task.exitStatus == 137  ? 'retry' : 'finish' }   
    maxRetries 5
    memory { 16.GB * task.attempt }

    output:
        file("scpred_output.tsv") into SCPRED_OUTPUT

    """
    RESULTS_DIR=\$PWD

    nextflow run $PROD_WORKFLOWS/scpred-prod-workflow/main.nf\
                        -profile cluster'
                        --scpred_models ${params.scpred.scpred_models}\
                        --results_dir ${params.scpred.results_dir}\
                        --query_10x_dir ${params.query_expr_data}\
                        --norm_counts_slot ${params.scpred.norm_counts_slot}\
                        --col_names ${params.scpred.col_names}\
                        --pred_threshold ${params.scpred.pred_threshold}

    """
}

// collate output tables into single channel 
ALL_RESULTS = 
    GARNETT_OUTPUT
    .concat(SCMAP_CLUST_OUTPUT)
    .concat(SCMAP_CELL_OUTPUT)
    .concat(SCPRED_OUTPUT)


// combine outputs into single directory 
process combine_outputs {
    input:
        file(method_outputs) from ALL_RESULTS.collect()

    output:
        file('results_dir') into COMBINED_RESULTS_DIR

    """
    mkdir -p results_dir/
    for file in ${method_outputs}
    do
        mv \$file results_dir
    done
    """
}

SDRFs = Channel.fromPath(params.label_analysis.sdrfs) 
process generate_cl_dict{
    conda "${baseDir}/envs/cell-types-analysis.yaml"
    
    errorStrategy { task.exitStatus == 130 || task.exitStatus == 137  ? 'retry' : 'finish' }   
    maxRetries 5
    memory { 16.GB * task.attempt }

    input:
        file(sdrfs) from SDRFs

    output:
        file("cl_dictionary.rds") into CL_DICT 

    """
    build_cell_ontology_dict.R\
          --input-dir ${sdrfs}\
          --condensed-sdrf ${params.label_analysis.condensed}\
          --output-dict-path cl_dictionary.rds
    """
}

// generate consensus labels
process run_label_analysis {
    publishDir "${baseDir}/data", mode: 'copy'
    conda "${baseDir}/envs/cell-types-analysis.yaml"
    errorStrategy { task.exitStatus == 130 || task.exitStatus == 137  ? 'retry' : 'finish' }   
    maxRetries 5
    memory { 16.GB * task.attempt }

    input:
        file(results_dir) from COMBINED_RESULTS_DIR
        file(cl_dict) from CL_DICT

    output: 
        file("summary_table.tsv") into SUMMARY_TABLE
        file("raw_labels.tsv") into RAW_LABELS

    """
    get_consensus_output.R\
          --input-dir ${results_dir}\
          --tool-table ${params.label_analysis.tool_perf_table}\
          --num-cores ${params.label_analysis.num_cores}\
          --cl-dictionary ${cl_dict}\
          --ontology-graph ${params.label_analysis.ontology_graph}\
          --semantic-sim-metric ${params.label_analysis.sem_sim_metric}\
          --summary-table-output-path summary_table.tsv
          --raw-table-output-path raw_labels.tsv
    """
}

