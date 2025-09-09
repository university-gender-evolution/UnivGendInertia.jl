module UnivGendInertia

# Write your package code here.
using DataFrames
using CSV
using FreqTables
using Plots
using StatFiles
using StatsBase
using DataFramesMeta
using Clustering
using MultivariateStats
using RCall
using Distances
using Interpolations
using Zygote
using KernelDensity
using PDFmerger
using Dates
using ComponentArrays
using Combinatorics
using Logging, LoggingExtras
using Mustache


include("Types.jl")
include("API.jl")
include("ClusterProcessing.jl");
include("DepartmentProcessing.jl");
include("ValidateProfJobCodesByDept.jl")


 export preprocess_data, preprocess_dept_train_test_split, get_department_data, 
        UM, DataAudit, NoAudit, UMData, UMDeptData











end
