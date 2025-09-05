using DataFrames

abstract type AbstractGendUnivDataConfiguration end;
struct UM <: AbstractGendUnivDataConfiguration end;

# Configurations for auditing and checking data preprocessing.
abstract type AbstractDataChecks end;
struct DataAudit <: AbstractDataChecks end;
struct NoAudit <: AbstractDataChecks end;

abstract type GendUnivData end;


initial_params_dde = ComponentArray(
rattr_f1 = 0.01,
rattr_f2 = 0.001,
rattr_f3 = 0.01,
rattr_m1 = 0.01,
rattr_m2 = 0.01,
rattr_m3 = 0.01,
rhire_f1 = 0.01,
rhire_f2 = 0.01,
rhire_f3 = 0.001,
rhire_m1 = 0.01,
rhire_m2 = 0.01,
rhire_m3 = 0.01,
rprom_f1 = 0.01,
rprom_f2 = 0.01,
rprom_f3 = 0.00,
rprom_m1 = 0.01,
rprom_m2 = 0.01,
rprom_m3 = 0.00,
growth_rate_linear = 0.01);


mutable struct DeptClusterData
    cluster_vector_agg_norm::Vector{Float64}
    cluster_vector_agg_ynorm::Vector{Float64}
    cluster_vector_detail_norm::Vector{Float64}
    cluster_vector_detail_ynorm::Vector{Float64}
    cluster_vector_spline_agg_norm::Vector{Float64}
    cluster_vector_spline_agg_ynorm::Vector{Float64}
    cluster_vector_spline_detail_norm::Vector{Float64}
    cluster_vector_spline_detail_ynorm::Vector{Float64}
    cluster_vector_act_norm_deptn::Vector{Float64}
    cluster_vector_spline_norm_deptn::Vector{Float64}
end;

function DeptClusterData()
    return DeptClusterData(Float64[], Float64[], Float64[], Float64[], 
                            Float64[], Float64[], Float64[], Float64[],
                            Float64[], Float64[]) 
end


mutable struct UMDeptData
    raw_df::DataFrame
    processed_data::DataFrame
    first_year::Integer
    num_years::Integer
    dept_rates::Dict{Symbol, Float64}
    dept_name::String
    cluster_vector::Vector{Float64}
    cluster_data::DeptClusterData
    _column_names::Vector{Symbol}
    _prof_entry_exit::DataFrame
    _prof_ids::Vector{Float32}
    _unique_years::Vector{Integer}
    _first_year::Integer
    _valid_assistants::Vector{String}
    _valid_associates::Vector{String}
    _valid_professors::Vector{String}
    final_year::Integer
    sindy_matrix::Matrix{Float64}
    hankel_matrix::Matrix{Float64}
    _period::Integer
    _bootiter::Integer
    _blocksize::Integer
    bootstrap_df::DataFrame
    optimization_df::DataFrame
    smoothing_spline_nknots::Integer
    smoothing_spline_df::DataFrame
    smoothing_spline_spar::Float64
    _data_cols::Vector{Symbol}
    _bootstrap_cols::Vector{Symbol}
    _bootstrap_derivative_cols::Vector{Symbol}
    _bootstrap_norm_cols::Vector{Symbol}
    _spline_cols::Vector{Symbol}
    _spline_derivative_cols::Vector{Symbol}
    _spline_norm_cols::Vector{Symbol}
    _optimization_cols::Vector{Symbol}
    _tspan::Tuple{Float64, Float64}
    _u0::DataFrame
    _initial_params_dde::typeof(initial_params_dde)
    _final_params_dde::typeof(initial_params_dde)
    _u0_columns::Vector{Symbol}
end;

function UMDeptData(df::DataFrame, first_year::Integer, num_years::Integer) 

    column_names = [:year,:act_f1, :act_f2, :act_f3, :act_m1, :act_m2, :act_m3 ,
                :act_fhire1, :act_fhire2, :act_fhire3, :act_mhire1, :act_mhire2, 
                :act_mhire3, :act_fattr1, :act_fattr2, :act_fattr3, :act_mattr1, 
                :act_mattr2, :act_mattr3, :act_fprom1, :act_fprom2, :act_mprom1, 
                :act_mprom2, :act_deptn, :act_hire, :act_f, :act_m, :act_fpct,
                :act_mpct, :act_deptname, :act_normf1, :act_normf2, :act_normf3,
                :act_normm1, :act_normm2, :act_normm3, :act_norm_f, :act_norm_m, 
                :act_norm_deptn, :act_ynorm_f1, :act_ynorm_f2, :act_ynorm_f3,
                :act_ynorm_m1, :act_ynorm_m2, :act_ynorm_m3, :act_ynorm_f, 
                :act_ynorm_m]

    valid_assistants = ["ASST PROFESSOR", "CLINICAL ASST PROF II", 
                        "CLINICAL ASST PROF", 
                        "ASST PROF/POSTDOC SCH-MSF", "ASST PROF EMERITUS/A", 
                        "ASST PROF/POST DOC", 
                        "RESEARCH ASST PROFESSOR", 
                        "CLINICAL ASST PROF EMERITUS/A"]


    valid_associates = ["ASSOC PROFESSOR", "CLINICAL ASSOC PROF II", "CLINICAL ASSOC PROF",  
                        "ASSOC PROF EMERITUS/A", "RESEARCH ASSOC PROF EMERITUS/A"]

    valid_full_profs = ["PROFESSOR", "CLINICAL PROFESSOR II", 
                        "CLINICAL PROFESSOR", "PROFESSOR EMERITUS/A", "RESEARCH PROFESSOR", 
                        "RESEARCH PROFESSOR EMERITUS/A", "CLIN PROF EMERITUS/A"]


    data_cols = [:act_f1, :act_f2, :act_f3, :act_m1, :act_m2, :act_m3, :act_f, :act_m, :act_deptn,
                :act_fattr1, :act_fattr2, :act_fattr3, :act_mattr1, :act_mattr2, :act_mattr3,
                :act_fhire1, :act_fhire2, :act_fhire3, :act_mhire1, :act_mhire2, :act_mhire3,
                :act_fprom1, :act_fprom2, :act_mprom1, :act_mprom1]

    spline_cols = [:spline_f1, :spline_f2, :spline_f3, :spline_m1, :spline_m2, 
                :spline_m3, :spline_f, :spline_m, :spline_deptn,
                :spline_fattr1, :spline_fattr2, :spline_fattr3, 
                :spline_mattr1, :spline_mattr2, :spline_mattr3,
                :spline_fhire1, :spline_fhire2, :spline_fhire3,
                :spline_mhire1, :spline_mhire2, :spline_mhire3,
                :spline_fprom1, :spline_fprom2, 
                :spline_mprom1, :spline_mprom2]
    
    boot_cols = [:boot_f1, :boot_f2, :boot_f3, :boot_m1, :boot_m2, :boot_m3, 
                :boot_f, :boot_m, :boot_deptn,
                :boot_fattr1, :boot_fattr2, :boot_fattr3,
                :boot_mattr1, :boot_mattr2, :boot_mattr3,
                :boot_fhire1, :boot_fhire2, :boot_fhire3,
                :boot_mhire1, :boot_mhire2, :boot_mhire3,
                :boot_fprom1, :boot_fprom2,
                :boot_mprom1, :boot_mprom2]
    
    optimization_cols = [:opt_f1, :opt_f2, :opt_f3, :opt_m1, :opt_m2, :opt_m3, 
                        :opt_f, :opt_m, :opt_deptn]

    spline_deriv_cols = [:spline_deriv_f1, :spline_deriv_f2, :spline_deriv_f3, :spline_deriv_m1,
                    :spline_deriv_m2, :spline_deriv_m3, :spline_deriv_f, :spline_deriv_m,
                    :spline_deriv_deptn,
                    :spline_deriv_fattr1, :spline_deriv_fattr2, :spline_deriv_fattr3,
                    :spline_deriv_mattr1, :spline_deriv_mattr2, :spline_deriv_mattr3,
                    :spline_deriv_fhire1, :spline_deriv_fhire2, :spline_deriv_fhire3,
                    :spline_deriv_mhire1, :spline_deriv_mhire2, :spline_deriv_mhire3,
                    :spline_deriv_fprom1, :spline_deriv_fprom2,
                    :spline_deriv_mprom1, :spline_deriv_mprom2]

    boot_deriv_cols = [:boot_deriv_f1, :boot_deriv_f2, :boot_deriv_f3, :boot_deriv_m1,
                    :boot_deriv_m2, :boot_deriv_m3, :boot_deriv_f, :boot_deriv_m,
                    :boot_deriv_deptn,
                    :boot_deriv_fattr1, :boot_deriv_fattr2, :boot_deriv_fattr3,
                    :boot_deriv_mattr1, :boot_deriv_mattr2, :boot_deriv_mattr3,
                    :boot_deriv_fhire1, :boot_deriv_fhire2, :boot_deriv_fhire3,
                    :boot_deriv_mhire1, :boot_deriv_mhire2, :boot_deriv_mhire3,
                    :boot_deriv_fprom1, :boot_deriv_fprom2, 
                    :boot_deriv_mprom1, :boot_deriv_mprom2]

    boot_norm_cols = [:boot_norm_f1, :boot_norm_f2, :boot_norm_f3, :boot_norm_m1, 
                    :boot_norm_m2, :boot_norm_m3, :boot_norm_f, :boot_norm_m, 
                    :boot_norm_deptn, :boot_ynorm_f1, :boot_ynorm_f2, :boot_ynorm_f3, 
                    :boot_ynorm_m1, :boot_ynorm_m2, :boot_ynorm_m3, :boot_ynorm_f, :boot_ynorm_m
                    ]
    spline_norm_cols = [:spline_norm_f1, :spline_norm_f2, :spline_norm_f3, 
                    :spline_norm_m1, :spline_norm_m2, :spline_norm_m3, 
                    :spline_norm_f, :spline_norm_m, :spline_norm_deptn,
                    :spline_ynorm_f1, :spline_ynorm_f2, :spline_ynorm_f3, 
                    :spline_ynorm_m1, :spline_ynorm_m2, :spline_ynorm_m3, 
                    :spline_ynorm_f, :spline_ynorm_m]
    u0_cols = [:u0_act_unnormalized, :u0_act_bootnorm, :u0_act_splinenorm, :u0_boot_bootnorm, :u0_spline_splinenorm]
    u0_df = DataFrame(zeros(6, length(u0_cols)), u0_cols)    

    last_year = first_year + num_years

    tspan = (1.0, float(num_years))

    dfcopy = copy(df) # just to make sure we don't change the original dataframe. 
    return UMDeptData(dfcopy,
            DataFrame(),
            first_year,
            num_years,
            Dict{Symbol, Float64}(),
            "",
            Float64[],
            DeptClusterData(),
            column_names, 
            DataFrame(),
            Int16[], 
            Int16[],
            0, 
            valid_assistants,
            valid_associates, 
            valid_full_profs,
            last_year,
            Matrix(rand(2,2)),
            Matrix(rand(2,2)), 
            1,
            100,
            2,
            DataFrame(), 
            DataFrame(),
            0,
            DataFrame(),
            0.0,
            data_cols,
            boot_cols,
            boot_deriv_cols,
            boot_norm_cols,
            spline_cols,
            spline_deriv_cols,
            spline_norm_cols,
            optimization_cols,
            tspan,
            u0_df,
            initial_params_dde,
            initial_params_dde,
            u0_cols)
end;


mutable struct ClusterResult
    num_clusters::Int
    cluster_sizes::Vector{Int}
    weighted_cluster_sizes::Vector{Float64}
    centers::Matrix{Float64}
    assignments::Vector{Int}
    dict::Dict
    _graph::Dict
end;


function ClusterResult()
    return ClusterResult(   -1, 
                            Int64[], 
                            Float64[], 
                            Matrix(rand(2,2)), 
                            Int64[],
                            Dict(), 
                            Dict())
end;


mutable struct ClusteringMethod
    raw_matrix::Matrix{Float64}
    pca_matrix::Matrix{Float64}
    distance_matrix::Matrix{Float64}
    optimal_clustering::Int
    kmeans::ClusterResult
    kmedoids::ClusterResult
    hierarchical_clustering::ClusterResult
    dbscan_clustering::ClusterResult
    spectral_clustering::ClusterResult
    affinity_propagation::ClusterResult
end;


function ClusteringMethod()
    return ClusteringMethod(    Matrix(rand(2,2)),
                                Matrix(rand(2,2)),
                                Matrix(rand(2,2)),
                                -1,
                                ClusterResult(), 
                                ClusterResult(), 
                                ClusterResult(), 
                                ClusterResult(), 
                                ClusterResult(), 
                                ClusterResult())
end;


mutable struct ClusteringGroup
    aggregated_norm::ClusteringMethod
    aggregated_ynorm::ClusteringMethod
    detail_norm::ClusteringMethod
    detail_ynorm::ClusteringMethod
    spline_aggregated_norm::ClusteringMethod
    spline_aggregated_ynorm::ClusteringMethod
    spline_detail_norm::ClusteringMethod
    spline_detail_ynorm::ClusteringMethod
    act_norm_deptn::ClusteringMethod
    spline_norm_deptn::ClusteringMethod
end;


function ClusteringGroup()
    return ClusteringGroup(ClusteringMethod(), 
                        ClusteringMethod(),
                        ClusteringMethod(), 
                        ClusteringMethod(),
                        ClusteringMethod(), 
                        ClusteringMethod(),
                        ClusteringMethod(), 
                        ClusteringMethod(),
                        ClusteringMethod(), 
                        ClusteringMethod())
end;

mutable struct UMData <: GendUnivData
    _file_path::String
    _raw_df::DataFrame
    _raw_df_backup::DataFrame
    univ_name::String
    first_year::Integer
    final_year::Integer
    num_years::Integer
    department_names::Vector{String}
    dept_rate_dict::Vector{Dict{Symbol, Float64}}
    univ_cluster_matrix::Matrix{Float64}
    _valid_dept_summary::DataFrame
    processed_df::DataFrame
    dept_data_vector::Vector{UMDeptData}
    univ_sindy_matrix::Matrix{Float64}
    univ_bootstrap_df::DataFrame
    cluster_number::Int
    clustering::ClusteringGroup
end;

function UMData(file_path::String, df::DataFrame) 

    return UMData(file_path,
                    df,
                    df,
                    "UNIVERSITY OF MICHIGAN",
                    0,
                    0,
                    0, 
                    [string()],
                    [Dict{Symbol, Float64}() for i in 1:2],
                    Matrix(rand(3, 4)),
                    DataFrame(),
                    DataFrame(),
                    UMDeptData[],
                    Matrix(rand(2,2)),
                    DataFrame(),
                    -1,
                    ClusteringGroup()
    )
end;

#=

List of Assistant Professor codes
["ASST PROFESSOR", "CLINICAL ASST PROF II", "ADJUNCT ASST PROFESSOR", 
"CLINICAL ASST PROF", "VISITING ASST PROFESSOR I", "VISITING ASST PROF II", 
"ADJUNCT CLIN ASST PROFESSOR", "ASST PROF/POSTDOC SCH-MSF", 
"ASST PROF EMERITUS/A", "VISITING CLIN ASST PROF", "ASST PROF/POST DOC", 
"RESEARCH ASST PROFESSOR", "LEO ADJUNCT ASST PROFESSOR", 
"ADJUNCT RES ASST PROF", "CLINICAL ASST PROF EMERITUS/A", 
"VISITING RES ASST PROF"]

List of Associate Professor codes
["ASSOC PROFESSOR", "CLINICAL ASSOC PROF II", "CLINICAL ASSOC PROF", 
"VISITING ASSOC PROF II", "ADJUNCT ASSOC PROFESSOR", "VISITING ASSOC PROF I", 
"ASSOC PROF EMERITUS/A", "ADJUNCT CLIN ASSOC PROF", "ADJUNCT RES ASSOC PROF", 
"LEO ADJUNCT ASSOC PROFESSOR", "VISITING RES ASSOC PROF", 
"RESEARCH ASSOC PROF EMERITUS/A"]

List of Full Professor codes
["PROFESSOR", "ASST PROFESSOR", "ASSOC PROFESSOR", "DIR ALLIED HLTH PROF EDUC", 
"MGR MED-PROF CLAIMS", "COORD PROF SVCS UNITS", "COORD PROF ED PROGRAMS", 
"CLINICAL ASST PROF II", "CLINICAL ASSOC PROF II", "CLINICAL PROFESSOR II", 
"ADJUNCT ASST PROFESSOR", "ADJUNCT PROFESSOR", "CLINICAL ASSOC PROF", 
"CLINICAL ASST PROF", "VISITING ASSOC PROF II", "VISITING PROFESSOR I", 
"VISITING ASST PROFESSOR I", "VISITING ASST PROF II", "CLINICAL PROFESSOR", 
"ADJUNCT ASSOC PROFESSOR", "PROFESSOR EMERITUS/A", 
"ADJUNCT CLIN ASST PROFESSOR", "VISITING PROFESSOR II", 
"ASST PROF/POSTDOC SCH-MSF", "VISITING ASSOC PROF I", "ASST PROF EMERITUS/A", 
"ASSOC PROF EMERITUS/A", "ADJUNCT CLINICAL PROFESSOR", "VISITING CLIN ASST PROF", 
"ADJUNCT CLIN ASSOC PROF", "ASST PROF/POST DOC", "RESEARCH PROFESSOR", 
"RESEARCH ASSOCIATE PROFESSOR", "RESEARCH ASST PROFESSOR", 
"DIR CTR FOR PROFESSIONAL DEVEL", "ADJUNCT RES PROF", "ADJUNCT RES ASSOC PROF", 
"LEO ADJUNCT ASSOC PROFESSOR", "MEETING/EVENT COORD NON PROFIT", 
"PROFESSIONAL SERVICES DIR", "RESEARCH PROFESSOR EMERITUS/A", 
"LEO ADJUNCT ASST PROFESSOR", "EXEC DIR NON-PROFIT ARTS ORG", 
"LEO ADJUNCT PROFESSOR", "GRAD PATIENT CARE SVCS PROF", "ADJUNCT RES ASST PROF", 
"VISITING RES PROF", "VISITING RES ASSOC PROF", "RESEARCH ASSOC PROF EMERITUS/A", 
"CLINICAL ASST PROF EMERITUS/A", "CLIN PROF EMERITUS/A", "VISITING RES ASST PROF"]


=#
