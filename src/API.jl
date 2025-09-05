function _load_univ_data(file_path::String, config::AbstractGendUnivDataConfiguration)
    dstructure = _setup_data(file_path, config)
    return dstructure
end;


function _setup_data(file_path::String, config::AbstractGendUnivDataConfiguration)

    df = DataFrame(StatFiles.load(file_path))
    disallowmissing!(df, error=false)
    d = UMData(file_path, df)
    _set_department_summaries!(d, config)
    return d
end;


function _set_department_summaries!(univ_data::GendUnivData, ::UM)

    _validation_adjust_depts(univ_data)
    depts_prof = subset(univ_data._raw_df, :jobdes => ByRow(contains("PROF")))
    
    dept_prof_unique = unique(depts_prof.orgname)
    newdf = @rsubset(univ_data._raw_df, :orgname ∈ dept_prof_unique)
    df2 = combine(groupby(newdf, [:orgname]), :year => minimum => :first_year,
                                    :year => maximum => :last_year, 
                                    groupindices)

    df2[!, "nyears"] = df2.last_year .- df2.first_year .+ 1

    df2[!, :first_year] = convert.(Int64, df2[!, :first_year])
    df2[!, :last_year] = convert.(Int64, df2[!, :last_year])

    df2 = filter(:nyears => x-> x > 3, df2)
    univ_data._valid_dept_summary = df2
end;



function _get_departments!(univdata::GendUnivData, ::UM)

    # First filter by departments that start in the target year and have 
    # sufficient subsequent years
    df = subset(univdata._valid_dept_summary, 
                    :first_year => ByRow(==(univdata.first_year)), 
                    :nyears => ByRow(>=(univdata.num_years)))

    univdata.processed_df = subset(univdata._raw_df, :orgname => x -> x .∈ [df.orgname])
    univdata.department_names = df.orgname
end;


function _get_departments!(univdata::GendUnivData, dept_index::Integer, ::UM)
    dept_name =  univdata._valid_dept_summary[(univdata._valid_dept_summary.groupindices .== dept_index), :].orgname
    univdata.processed_df = subset(univdata._raw_df, :orgname => ByRow(==(dept_name[1])))
    #@debug("get processed_df size: $(size(univdata.processed_df))")
    univdata.department_names = dept_name
end;

function get_department_data(file_path::String, dept_name::String,
                            config::AbstractGendUnivDataConfiguration)

    univ_data = _load_univ_data(file_path, config)

    if any(occursin.(strip(dept_name), univ_data._valid_dept_summary.orgname))
        dept_data = univ_data._valid_dept_summary[(univ_data._valid_dept_summary.orgname .== dept_name), :]
        dept_index = dept_data.groupindices[1] 
    else
        throw(DomainError(dept_name, "The provided department name does not match any existing record. 
        Please make sure the name is specified exactly."))
    end
    return (dept_data, dept_index)
end;



function _process_each_dept!(univdata::GendUnivData, ::UM, audit_config)   
    new_dept_names = []
    #@debug("process_each_dept - Dept Names: $(univdata.department_names)")
    for (index, value) in enumerate(univdata.department_names)
        input = filter(:orgname => contains(value), univdata.processed_df)
        #@debug("process_each_dept - input size: $(size(input))")
        #@debug("process_each_dept - dept name: $(input.orgname[1])")
        res = preprocess_um_data(input, univdata.first_year, univdata.num_years, audit_config)
        if describe(res.processed_data[:, [:act_deptn]], :mean)[1,2] == 0.0
            @info("dept data empty:  $value")
            continue
        else            
            push!(univdata.dept_data_vector, res)
            @info("department added: $value")
            #@debug("process_each_dept - size of dept_data_vector: $(length(univdata.dept_data_vector))")
            @debug("process_each_dept - size of results df: $(size(res.processed_data))")
            push!(new_dept_names, value)
            @debug("rows processed: $(nrow(res.processed_data))")
        end
    end
    univdata.department_names = new_dept_names
end;

function _postprocess_data_arrays!(univdata::GendUnivData, ::UM)

    t1 = [univdata.dept_data_vector[i].processed_data for i in 1:length(univdata.dept_data_vector)]
    t2 = [univdata.dept_data_vector[i].cluster_vector[1:univdata.num_years*6] for i in 1:length(univdata.dept_data_vector)]
    t3 = [univdata.dept_data_vector[i].sindy_matrix[1:univdata.num_years, :] for i in 1:length(univdata.dept_data_vector)]
    t4 = [univdata.dept_data_vector[i].bootstrap_df[1:univdata.num_years, :] for i in 1:length(univdata.dept_data_vector)]
    univdata.processed_df = reduce(vcat, t1)
    univdata.univ_cluster_matrix = reduce(hcat, t2)
    univdata.univ_sindy_matrix = reduce(hcat, t3)
    univdata.univ_bootstrap_df = reduce(vcat, t4)
    aggregate_cluster_vectors_to_matrix(univdata)
    _process_clustering_analysis!(univdata)    
end;

function _postprocess_data_arrays_all_depts!(univdata::GendUnivData, ::UM)
    @debug("size of dept data vector: $(size(univdata.dept_data_vector))")
    t1 = univdata.dept_data_vector[end].processed_data
    #t2 = [univdata.dept_data_vector[end].cluster_vector[1:univdata.num_years*6] for i in 1:length(univdata.dept_data_vector)]
    @debug("length of sindy matrix: $(size(univdata.dept_data_vector[end].sindy_matrix))")
    @debug("size I am trying to access: $(1:univdata.num_years)")
    t3 = univdata.dept_data_vector[end].sindy_matrix
    t4 = univdata.dept_data_vector[end].bootstrap_df
    univdata.processed_df = t1
    #univdata.univ_cluster_matrix = reduce(hcat, t2)
    univdata.univ_sindy_matrix = t3
    univdata.univ_bootstrap_df = t4
    #aggregate_cluster_vectors_to_matrix(univdata)
    #_process_clustering_analysis!(univdata)    
end;



function preprocess_data(file_path::String, 
                        first_year::Integer, 
                        num_years::Integer, 
                        config::AbstractGendUnivDataConfiguration; 
                        audit_config::AbstractDataChecks=NoAudit())

    logger = TeeLogger(
            ConsoleLogger(stderr),
            FormatLogger(open("logfile.txt", "w")) do io, args
            # Write the module, level and message only
                println(io, args._module, " | ", "[", args.level, "] ", args.message)
            end )
    
    global_logger(logger)

    @debug "logging initiated."

    univ_data = _load_univ_data(file_path, config)
    univ_data.first_year = first_year
    univ_data.num_years = num_years
    _get_departments!(univ_data, config)
    _process_each_dept!(univ_data, config, audit_config)
    _postprocess_data_arrays!(univ_data, config)

    @debug "Closing the log"
    
    return univ_data
end;


function preprocess_data(file_path::String, 
                        dept_name::String, 
                        config::AbstractGendUnivDataConfiguration; 
                        audit_config::AbstractDataChecks=NoAudit())

    univ_data = _load_univ_data(file_path, config)

    if any(occursin.(strip(dept_name), univ_data._valid_dept_summary.orgname))
        dept_index = univ_data._valid_dept_summary[(univ_data._valid_dept_summary.orgname .== dept_name), :].groupindices[1]
    else
        throw(DomainError(dept_name, "The provided department name does not match any existing record. 
        Please make sure the name is specified exactly."))
    end
    
    univ_data.first_year = univ_data._valid_dept_summary[(univ_data._valid_dept_summary.groupindices .== dept_index), :].first_year[1]
    univ_data.num_years = univ_data._valid_dept_summary[(univ_data._valid_dept_summary.groupindices .== dept_index), :].nyears[1]
    _get_departments!(univ_data, dept_index, config)
    _process_each_dept!(univ_data, config, audit_config)
    _postprocess_data_arrays!(univ_data, config)
    return univ_data
end;


function preprocess_data(file_path::String, 
                        dept_index::Integer, 
                        config::AbstractGendUnivDataConfiguration; 
                        audit_config::AbstractDataChecks=NoAudit())

    univ_data = _load_univ_data(file_path, config)

    if dept_index ∈ univ_data._valid_dept_summary.groupindices

    else
        throw(DomainError(dept_name, "The provided department index does not match any existing record. 
        Please make sure the index is specified correctly."))
    end

    univ_data.first_year = univ_data._valid_dept_summary[(univ_data._valid_dept_summary.groupindices .== dept_index), :].first_year[1]
    univ_data.num_years = univ_data._valid_dept_summary[(univ_data._valid_dept_summary.groupindices .== dept_index), :].nyears[1]
    _get_departments!(univ_data, dept_index, config)
    _process_each_dept!(univ_data, config, audit_config)
    _postprocess_data_arrays!(univ_data, config)
    return univ_data
end;


function preprocess_data(file_path::String, 
                        dept_name::String,
                        start_year::Integer,
                        num_years::Integer, 
                        config::AbstractGendUnivDataConfiguration; 
                        audit_config::AbstractDataChecks=NoAudit())


    univ_data = _load_univ_data(file_path, config)

    if any(occursin.(strip(dept_name), univ_data._valid_dept_summary.orgname))
        dept_data = univ_data._valid_dept_summary[(univ_data._valid_dept_summary.orgname .== dept_name), :]
        dept_index = dept_data.groupindices[1] 
    else
        throw(DomainError(dept_name, "The provided department name does not match any existing record. 
        Please make sure the name is specified exactly."))
    end

    if (start_year + num_years) < dept_data.last_year[1] && start_year >= dept_data.first_year[1]

    else
        throw(DomainError(dept_name, "The provided start_year and number of years falls outside of the 
        range of the data. Either the start year falls before the first year of data, or the 
        start year + number of years falls beyond the last year of data."))
    end

    univ_data.first_year = start_year
    univ_data.num_years = num_years
    _get_departments!(univ_data, dept_index, config)
    _process_each_dept!(univ_data, config, audit_config)
    _postprocess_data_arrays!(univ_data, config)
    return univ_data
end;


function preprocess_data(file_path::String, 
                        dept_index::Integer,
                        start_year::Integer,
                        num_years::Integer, 
                        config::AbstractGendUnivDataConfiguration; 
                        audit_config::AbstractDataChecks=NoAudit())

    univ_data = _load_univ_data(file_path, config)

    if dept_index ∈ univ_data._valid_dept_summary.groupindices
        dept_data = univ_data._valid_dept_summary[(univ_data._valid_dept_summary.groupindices .== dept_index), :]
    else
        throw(DomainError(dept_data.orgname, "The provided department index does not match any existing record. 
        Please make sure the index is specified correctly."))
    end

    if (start_year + num_years) < dept_data.last_year[1] && start_year >= dept_data.first_year[1]

    else
        throw(DomainError(dept_data.orgname, "The provided start_year and number of years falls outside of the 
        range of the data. Either the start year falls before the first year of data, or the 
        start year + number of years falls beyond the last year of data."))
    end

    univ_data.first_year = start_year
    univ_data.num_years = num_years
    _get_departments!(univ_data, dept_index, config)
    _process_each_dept!(univ_data, config, audit_config)
    _postprocess_data_arrays!(univ_data, config)
    return univ_data
end;


function preprocess_data(univ_data::GendUnivData, 
                        config::AbstractGendUnivDataConfiguration; 
                        audit_config::AbstractDataChecks=NoAudit())



    logger = TeeLogger(
            ConsoleLogger(stderr),
            FormatLogger(open("logfile_generate_all_depts.txt", "w")) do io, args
            # Write the module, level and message only
                println(io, args._module, " | ", "[", args.level, "] ", args.message)
            end )
    
    global_logger(logger)

    @debug "logging initiated."

    combined_df = DataFrame()

    for (i, dept_name) in enumerate(univ_data._valid_dept_summary.orgname)

        dept_index = univ_data._valid_dept_summary[(univ_data._valid_dept_summary.orgname .== dept_name), :].groupindices[1]
        @debug("loading department: $(dept_name).")
        univ_data.first_year = univ_data._valid_dept_summary[(univ_data._valid_dept_summary.groupindices .== dept_index), :].first_year[1]
        univ_data.num_years = univ_data._valid_dept_summary[(univ_data._valid_dept_summary.groupindices .== dept_index), :].nyears[1]
        _get_departments!(univ_data, dept_index, config)
        _process_each_dept!(univ_data, config, audit_config)
        _postprocess_data_arrays_all_depts!(univ_data, config)
        if isempty(combined_df)
            combined_df = hcat(univ_data.dept_data_vector[end].processed_data, 
                            univ_data.dept_data_vector[end].bootstrap_df,
                            makeunique=true)
        else
            @debug("size of processed data: $(size(univ_data.dept_data_vector[end].processed_data))")
            @debug("size of bootstrap data: $(size(univ_data.dept_data_vector[end].bootstrap_df))")

            tdf = hcat(univ_data.dept_data_vector[end].processed_data, 
                        univ_data.dept_data_vector[end].bootstrap_df,
                        makeunique=true)
            append!(combined_df, tdf, promote=true)
        end
    end
    CSV.write("test_all_depts.csv", combined_df)    
    @debug("Writing data to csv.")
    
    return univ_data





end



function preprocess_data(file_path::String, 
                        config::AbstractGendUnivDataConfiguration; 
                        audit_config::AbstractDataChecks=NoAudit())


    univ_data = _load_univ_data(file_path, config)
    preprocess_data(univ_data, config; audit_config=audit_config)


end




function _validation_checks_train_test_split(dept_data::DataFrame,
                                            train_start_year::Integer,
                                            train_nyears::Integer,
                                            test_start_year::Integer,
                                            test_nyears::Integer)
    if train_start_year < test_start_year
        true
    else
        throw(DomainError(dept_data.orgname[1]. "The test start year must 
        come before the train start year."))
    end

    if (train_start_year + train_nyears) < dept_data.last_year[1] && train_start_year >= dept_data.first_year[1]
        true
    else
        throw(DomainError(dept_data.orgname[1], "The provided training start_year and number of years falls outside of the 
        range of the data. Either the start year falls before the first year of data, or the 
        start year + number of training years falls beyond the last year of data."))
    end

    if (test_start_year + test_nyears) < dept_data.last_year[1] && test_start_year >= dept_data.first_year[1]
        true
    else
        throw(DomainError(dept_data.orgname[1], "The provided testing start_year
        and number of years falls outside of the 
        range of the data. Either the start year falls before the first year of 
        data, or the start year + number of testing years falls beyond the last 
        year of data."))
    end
end;

function preprocess_dept_train_test_split(file_path::String, 
                                            dept_name::String,
                                            start_year::Integer,
                                            train_nyears::Integer,
                                            test_nyears::Integer, 
                                            config::AbstractGendUnivDataConfiguration; 
                                            audit_config::AbstractDataChecks=NoAudit())


    univ_data = _load_univ_data(file_path, config)

    if any(occursin.(strip(dept_name), univ_data._valid_dept_summary.orgname))
        dept_data = univ_data._valid_dept_summary[(univ_data._valid_dept_summary.orgname .== dept_name), :]
        dept_index = dept_data.groupindices[1] 
    else
        throw(DomainError(dept_name, "The provided department name does not match any existing record. 
        Please make sure the name is specified exactly."))
    end


    train_start_year = start_year
    test_start_year = train_start_year + train_nyears
    test_end_year = test_start_year + test_nyears      

    _validation_checks_train_test_split(dept_data, train_start_year, 
                                        train_nyears, test_start_year, 
                                        test_nyears)

    univ_data_train = preprocess_data(file_path, dept_index, train_start_year,
                        train_nyears, config; audit_config)
    
    univ_data_test = preprocess_data(file_path, dept_index, test_start_year,
                        test_nyears, config; audit_config)

    return (univ_data_train, univ_data_test)
end;