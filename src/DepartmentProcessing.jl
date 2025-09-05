
using DataFrames;
using DataFramesMeta;
using StatsBase
using DifferentialEquations

function _setup_um_preprocessing(df::DataFrame, 
                                first_year::Integer, 
                                num_years::Integer, 
                                ::NoAudit)

    umdata = UMDeptData(df, first_year, num_years);

    umdata._prof_entry_exit = combine(groupby(df, 
                                [:id, :FEMALE, :jobdes]), 
                                :year => minimum => :hire,
                                :year => maximum => :attr)

    years_range = collect(range(start=first_year, step=1, stop=first_year+num_years))


    umdata.processed_data = DataFrame(zeros(length(years_range), 
                                        length(umdata._column_names)), 
                                        umdata._column_names)

    umdata.processed_data.year = years_range     
    umdata._unique_years = unique(years_range)
    umdata._prof_ids = unique(df.id)
    umdata._first_year = minimum(years_range)
    umdata.dept_name = first(df.orgname)
    umdata.smoothing_spline_nknots = length(years_range)
    umdata.smoothing_spline_spar = 0.7
    return umdata

end;


function _setup_um_preprocessing(df::DataFrame, 
                                first_year::Integer, 
                                num_years::Integer, 
                                ::DataAudit)

    umdata = UMDeptData(df, first_year, num_years);

    umdata._column_names = [:year,:act_f1, :act_f2, :act_f3, :act_m1, :act_m2, :act_m3 ,
                :act_fhire1, :act_fhire2, :act_fhire3, :act_mhire1, :act_mhire2, 
                :act_mhire3, :act_fattr1, :act_fattr2, :act_fattr3, :act_mattr1, 
                :act_mattr2, :act_mattr3, :act_fprom1, :act_fprom2, :act_mprom1, 
                :act_mprom2, :act_deptn, :act_hire, :act_f, :act_m, :act_fpct,
                :act_mpct, :act_deptname, :act_normf1, :act_normf2, :act_normf3,
                :act_normm1, :act_normm2, :act_normm3, :act_norm_f, :act_norm_m, 
                :act_norm_deptn, :act_ynorm_f1, :act_ynorm_f2, :act_ynorm_f3,
                :act_ynorm_m1, :act_ynorm_m2, :act_ynorm_m3, :act_ynorm_f, 
                :act_ynorm_m]

    audit_columns = [:aud_f1, :aud_f2, :aud_f3,  :aud_m1, :aud_m2, :aud_m3, 
                    :aud_fhire1, :aud_fhire2, :aud_fhire3, :aud_mhire1, 
                    :aud_mhire2, :aud_mhire3, :aud_fattr1, :aud_fattr2, 
                    :aud_fattr3, :aud_mattr1, :aud_mattr2, :aud_mattr3, 
                    :aud_fprom1, :aud_fprom2, :aud_mprom1, :aud_mprom2]

    years_range = collect(range(start=first_year, step=1, stop=first_year+num_years))
    
    processed_data = DataFrame(zeros(length(years_range), 
                                        length(umdata._column_names)), 
                                        umdata._column_names)

    audit_df = DataFrame(string.(zeros(length(years_range), 
                                        length(audit_columns))), 
                                        audit_columns)
    audit_df .= ""

    umdata.processed_data = hcat(processed_data, audit_df)

    umdata._column_names = vcat(umdata._column_names, audit_columns)

    umdata._prof_entry_exit = combine(groupby(df, 
                                [:id, :FEMALE, :jobdes]), 
                                :year => minimum => :hire,
                                :year => maximum => :attr,
                                :name => identity => :name)


    umdata.processed_data.year = years_range
    umdata._unique_years = unique(years_range)
    umdata._prof_ids = unique(df.id)
    umdata._first_year = minimum(years_range)
    umdata.dept_name = first(df.orgname)
    umdata.smoothing_spline_nknots = length(years_range)
    umdata.smoothing_spline_spar = 0.5

    return umdata


end;


function _process_department_numbers!(umdata::UMDeptData, ::NoAudit)

    tdf = sort(umdata.raw_df, [:year, :id])

    for row in eachrow(tdf)
        if row.jobdes == "ASST PROFESSOR" && row.FEMALE == 1
            umdata.processed_data[umdata.processed_data.year .== row.year, [:act_f1]] .+= 1.0
        elseif row.jobdes == "ASST PROFESSOR" && row.FEMALE == 0        
            umdata.processed_data[umdata.processed_data.year .== row.year, [:act_m1]] .+= 1.0
        elseif row.jobdes == "ASSOC PROFESSOR" && row.FEMALE == 1
            umdata.processed_data[umdata.processed_data.year .== row.year, [:act_f2]] .+= 1.0
        elseif row.jobdes == "ASSOC PROFESSOR" && row.FEMALE == 0        
            umdata.processed_data[umdata.processed_data.year .== row.year, [:act_m2]] .+= 1.0
        elseif row.jobdes == "PROFESSOR" && row.FEMALE == 1
            umdata.processed_data[umdata.processed_data.year .== row.year, [:act_f3]] .+= 1.0
        elseif row.jobdes == "PROFESSOR" && row.FEMALE == 0        
            umdata.processed_data[umdata.processed_data.year .== row.year, [:act_m3]] .+= 1.0
        end
    end

    # set u0 to the first entry in each professor count
    umdata._u0[:, :u0_act_unnormalized] = vec(Array(umdata.processed_data[1, [:act_f1, :act_f2, :act_f3, :act_m1, :act_m2, :act_m3]]))
end;


function _process_department_numbers!(umdata::UMDeptData, ::DataAudit)

    tdf = sort(umdata.raw_df, [:year, :id])
    for row in eachrow(tdf)
        if row.jobdes == "ASST PROFESSOR" && row.FEMALE == 1
            umdata.processed_data[umdata.processed_data.year .== row.year, [:act_f1]] .+= 1.0
            umdata.processed_data[umdata.processed_data.year .== row.year, [:aud_f1]] .*= row.name * "\n "
        elseif row.jobdes == "ASST PROFESSOR" && row.FEMALE == 0        
            umdata.processed_data[umdata.processed_data.year .== row.year, [:act_m1]] .+= 1.0
            umdata.processed_data[umdata.processed_data.year .== row.year, [:aud_m1]] .*= row.name * "\n "
        elseif row.jobdes == "ASSOC PROFESSOR" && row.FEMALE == 1
            umdata.processed_data[umdata.processed_data.year .== row.year, [:act_f2]] .+= 1.0
            umdata.processed_data[umdata.processed_data.year .== row.year, [:aud_f2]] .*= row.name * "\n "
        elseif row.jobdes == "ASSOC PROFESSOR" && row.FEMALE == 0        
            umdata.processed_data[umdata.processed_data.year .== row.year, [:act_m2]] .+= 1.0
            umdata.processed_data[umdata.processed_data.year .== row.year, [:aud_m2]] .*= row.name * "\n "
        elseif row.jobdes == "PROFESSOR" && row.FEMALE == 1
            umdata.processed_data[umdata.processed_data.year .== row.year, [:act_f3]] .+= 1.0
            umdata.processed_data[umdata.processed_data.year .== row.year, [:aud_f3]] .*= row.name * "\n "
        elseif row.jobdes == "PROFESSOR" && row.FEMALE == 0        
            umdata.processed_data[umdata.processed_data.year .== row.year, [:act_m3]] .+= 1.0
            umdata.processed_data[umdata.processed_data.year .== row.year, [:aud_m3]] .*= row.name * "\n "
        end
    end
    # set u0 to the first entry in each professor count
    umdata._u0[:, :u0_act_unnormalized] = vec(Array(umdata.processed_data[1, [:act_f1, :act_f2, :act_f3, :act_m1, :act_m2, :act_m3]]))
end;


function _process_department_hires_attr_promotion!(umdata::UMDeptData, 
                                audit_config::JuliaGendUniv_Types.AbstractDataChecks)

    ## setup the hires
    for idx in umdata._prof_ids
        s = sort(subset(umdata._prof_entry_exit, :id => ByRow(==(idx))), :hire)
        _process_hire!(s[1, :], umdata, audit_config)
    end

    ## setup the attritions
    for idx in umdata._prof_ids
        s = sort(subset(umdata._prof_entry_exit, :id => ByRow(==(idx))), :attr)
        s = filter(:attr => !=(umdata.final_year), s)
        if nrow(s) > 0
            _process_attrition!(s[end, :], umdata, audit_config)
        end
        end
    
    ## setup the promotions
    for idx in umdata._prof_ids
        s = sort(subset(umdata._prof_entry_exit, :id => ByRow(==(idx))), :attr)
        s = unique(s, :jobdes)
        if nrow(s) > 1
            _process_promotions!(s[2:end, :], umdata, audit_config)
        end
    end
end;        


function _process_hire!(dfrow::DataFrameRow, umdata::UMDeptData, ::NoAudit)

    if dfrow.jobdes == "ASST PROFESSOR" && dfrow.FEMALE == 1.0
        umdata.processed_data[umdata.processed_data.year .== dfrow.hire - 1, [:act_fhire1]] .+= 1.0
    elseif dfrow.jobdes == "ASST PROFESSOR" && dfrow.FEMALE == 0.0
        umdata.processed_data[umdata.processed_data.year .== dfrow.hire - 1, [:act_mhire1]] .+= 1.0
    elseif dfrow.jobdes == "ASSOC PROFESSOR" && dfrow.FEMALE == 1.0
        umdata.processed_data[umdata.processed_data.year .== dfrow.hire - 1, [:act_fhire2]] .+= 1.0
    elseif dfrow.jobdes == "ASSOC PROFESSOR" && dfrow.FEMALE == 0.0
        umdata.processed_data[umdata.processed_data.year .== dfrow.hire - 1, [:act_mhire2]] .+= 1.0
    elseif dfrow.jobdes == "PROFESSOR" && dfrow.FEMALE == 1.0
        umdata.processed_data[umdata.processed_data.year .== dfrow.hire - 1, [:act_fhire3]] .+= 1.0
    elseif dfrow.jobdes == "PROFESSOR" && dfrow.FEMALE == 0.0
        umdata.processed_data[umdata.processed_data.year .== dfrow.hire - 1, [:act_mhire3]] .+= 1.0
    end
end;


function _process_hire!(dfrow::DataFrameRow, umdata::UMDeptData, ::DataAudit)
    "any(occursin.(dfrow.jobdes, umdata._valid_assistants))"
    if  dfrow.jobdes == "ASST PROFESSOR" && dfrow.FEMALE == 1.0
        umdata.processed_data[umdata.processed_data.year .== dfrow.hire - 1, [:act_fhire1]] .+= 1.0
        umdata.processed_data[umdata.processed_data.year .== dfrow.hire - 1, [:aud_fhire1]] .*= dfrow.name * "\n "
    elseif dfrow.jobdes == "ASST PROFESSOR" && dfrow.FEMALE == 0.0
        umdata.processed_data[umdata.processed_data.year .== dfrow.hire - 1, [:act_mhire1]] .+= 1.0
        umdata.processed_data[umdata.processed_data.year .== dfrow.hire - 1, [:aud_mhire1]] .*= dfrow.name * "\n "
    elseif dfrow.jobdes == "ASSOC PROFESSOR" && dfrow.FEMALE == 1.0
        umdata.processed_data[umdata.processed_data.year .== dfrow.hire - 1, [:act_fhire2]] .+= 1.0
        umdata.processed_data[umdata.processed_data.year .== dfrow.hire - 1, [:aud_fhire2]] .*= dfrow.name * "\n "
    elseif dfrow.jobdes == "ASSOC PROFESSOR" && dfrow.FEMALE == 0.0
        umdata.processed_data[umdata.processed_data.year .== dfrow.hire - 1, [:act_mhire2]] .+= 1.0
        umdata.processed_data[umdata.processed_data.year .== dfrow.hire - 1, [:aud_mhire2]] .*= dfrow.name * "\n "
    elseif dfrow.jobdes == "PROFESSOR" && dfrow.FEMALE == 1.0
        umdata.processed_data[umdata.processed_data.year .== dfrow.hire - 1, [:act_fhire3]] .+= 1.0
        umdata.processed_data[umdata.processed_data.year .== dfrow.hire - 1, [:aud_fhire3]] .*= dfrow.name * "\n "
    elseif dfrow.jobdes == "PROFESSOR" && dfrow.FEMALE == 0.0
        umdata.processed_data[umdata.processed_data.year .== dfrow.hire - 1, [:act_mhire3]] .+= 1.0
        umdata.processed_data[umdata.processed_data.year .== dfrow.hire - 1, [:aud_mhire3]] .*= dfrow.name * "\n "
    end
end;


function _process_attrition!(dfrow::DataFrameRow, umdata::UMDeptData, ::NoAudit)

    if dfrow.jobdes == "ASST PROFESSOR" && dfrow.FEMALE == 1.0
        umdata.processed_data[umdata.processed_data.year.== dfrow.attr, [:act_fattr1]] .+= 1.0
    elseif dfrow.jobdes == "ASST PROFESSOR" && dfrow.FEMALE == 0.0
        umdata.processed_data[umdata.processed_data.year.== dfrow.attr, [:act_mattr1]] .+= 1.0
    elseif dfrow.jobdes == "ASSOC PROFESSOR" && dfrow.FEMALE == 1.0
        umdata.processed_data[umdata.processed_data.year.== dfrow.attr, [:act_fattr2]] .+= 1.0
    elseif dfrow.jobdes == "ASSOC PROFESSOR" && dfrow.FEMALE == 0.0
        umdata.processed_data[umdata.processed_data.year.== dfrow.attr, [:act_mattr2]] .+= 1.0
    elseif dfrow.jobdes == "PROFESSOR" && dfrow.FEMALE == 1.0
        umdata.processed_data[umdata.processed_data.year.== dfrow.attr, [:act_fattr3]] .+= 1.0
    elseif dfrow.jobdes == "PROFESSOR" && dfrow.FEMALE == 0.0
        umdata.processed_data[umdata.processed_data.year.== dfrow.attr, [:act_mattr3]] .+= 1.0
    end
end;


function _process_attrition!(dfrow::DataFrameRow, umdata::UMDeptData, ::DataAudit)

    if dfrow.jobdes == "ASST PROFESSOR" && dfrow.FEMALE == 1.0
        umdata.processed_data[umdata.processed_data.year .== dfrow.attr, [:act_fattr1]] .+= 1.0
        umdata.processed_data[umdata.processed_data.year .== dfrow.attr, [:aud_fattr1]] .*= dfrow.name * "\n "
    elseif dfrow.jobdes == "ASST PROFESSOR" && dfrow.FEMALE == 0.0
        umdata.processed_data[umdata.processed_data.year .== dfrow.attr, [:act_mattr1]] .+= 1.0
        umdata.processed_data[umdata.processed_data.year .== dfrow.attr, [:aud_mattr1]] .*= dfrow.name * "\n "
    elseif dfrow.jobdes == "ASSOC PROFESSOR" && dfrow.FEMALE == 1.0
        umdata.processed_data[umdata.processed_data.year .== dfrow.attr, [:act_fattr2]] .+= 1.0
        umdata.processed_data[umdata.processed_data.year .== dfrow.attr, [:aud_fattr2]] .*= dfrow.name * "\n "
    elseif dfrow.jobdes == "ASSOC PROFESSOR" && dfrow.FEMALE == 0.0
        umdata.processed_data[umdata.processed_data.year .== dfrow.attr, [:act_mattr2]] .+= 1.0
        umdata.processed_data[umdata.processed_data.year .== dfrow.attr, [:aud_mattr2]] .*= dfrow.name * "\n "
    elseif dfrow.jobdes == "PROFESSOR" && dfrow.FEMALE == 1.0
        umdata.processed_data[umdata.processed_data.year .== dfrow.attr, [:act_fattr3]] .+= 1.0
        umdata.processed_data[umdata.processed_data.year .== dfrow.attr, [:aud_fattr3]] .*= dfrow.name * "\n "
    elseif dfrow.jobdes == "PROFESSOR" && dfrow.FEMALE == 0.0
        umdata.processed_data[umdata.processed_data.year .== dfrow.attr, [:act_mattr3]] .+= 1.0
        umdata.processed_data[umdata.processed_data.year .== dfrow.attr, [:aud_mattr3]] .*= dfrow.name * "\n "
    end
end;

function _process_promotions!(dfprofset::DataFrame, umdata::UMDeptData, ::NoAudit)

    for (i, row) in enumerate(eachrow(dfprofset))
        if row.jobdes == "ASSOC PROFESSOR" && row.FEMALE == 1
            umdata.processed_data[umdata.processed_data.year.==row.hire-1, [:act_fprom1]] .+= 1.0
        elseif row.jobdes == "ASSOC PROFESSOR" && row.FEMALE == 0
            umdata.processed_data[umdata.processed_data.year.==row.hire-1, [:act_mprom1]] .+= 1.0
        elseif row.jobdes == "PROFESSOR" && row.FEMALE == 1
            umdata.processed_data[umdata.processed_data.year.==row.hire-1, [:act_fprom2]] .+= 1.0
        elseif row.jobdes == "PROFESSOR" && row.FEMALE == 0
            umdata.processed_data[umdata.processed_data.year.==row.hire-1, [:act_mprom2]] .+= 1.0
        end
    end
end;


function _process_promotions!(dfprofset::DataFrame, umdata::UMDeptData, ::DataAudit)

    for (i, row) in enumerate(eachrow(dfprofset))
        if row.jobdes == "ASSOC PROFESSOR" && row.FEMALE == 1.0
            umdata.processed_data[umdata.processed_data.year .== row.hire - 1, [:act_fprom1]] .+= 1.0
            umdata.processed_data[umdata.processed_data.year .== row.hire - 1, [:aud_fprom1]] .*= row.name * "\n "
        elseif row.jobdes == "ASSOC PROFESSOR" && row.FEMALE == 0.0
            umdata.processed_data[umdata.processed_data.year .== row.hire - 1, [:act_mprom1]] .+= 1.0
            umdata.processed_data[umdata.processed_data.year .== row.hire - 1, [:aud_mprom1]] .*= row.name * "\n "
        elseif row.jobdes == "PROFESSOR" && row.FEMALE == 1.0
            umdata.processed_data[umdata.processed_data.year .== row.hire - 1, [:act_fprom2]] .+= 1.0
            umdata.processed_data[umdata.processed_data.year .== row.hire - 1, [:aud_fprom2]] .*= row.name * "\n "
        elseif row.jobdes == "PROFESSOR" && row.FEMALE == 0.0
            umdata.processed_data[umdata.processed_data.year .== row.hire - 1, [:act_mprom2]] .+= 1.0
            umdata.processed_data[umdata.processed_data.year .== row.hire - 1, [:aud_mprom2]] .*= row.name * "\n "
        end
    end
end;

function _process_summary_data!(umdata::UMDeptData)

    ## setup department numbers

    umdata.processed_data[!, [:act_deptn]] = @select(umdata.processed_data, :act_deptn = :act_f1 + :act_f2 + :act_f3 + :act_m1 + :act_m2 + :act_m3)
    umdata.processed_data[!, [:act_hire]] = @select(umdata.processed_data, :act_hire = :act_fhire1 + :act_fhire2 + :act_fhire3 + :act_mhire1 + :act_mhire2 + :act_mhire3)
    umdata.processed_data[!, [:act_f]] = @select(umdata.processed_data, :act_f = :act_f1 + :act_f2 + :act_f3)
    umdata.processed_data[!, [:act_m]] = @select(umdata.processed_data, :act_m = :act_m1 + :act_m2 + :act_m3)
    umdata.processed_data[!, [:act_fpct]] = @rselect(umdata.processed_data, :act_fpct = :act_f / :act_deptn)
    umdata.processed_data[!, [:act_mpct]] = @rselect(umdata.processed_data, :act_mpct = 1 - :act_fpct)
    umdata.processed_data[!, [:act_deptname]] .= umdata.dept_name

    max_deptn = maximum(umdata.processed_data.act_deptn)
    umdata.processed_data[!, [:act_normf1]] .= umdata.processed_data.act_f1/max_deptn
    umdata.processed_data[!, [:act_normf2]] .= umdata.processed_data.act_f2/max_deptn
    umdata.processed_data[!, [:act_normf3]] .= umdata.processed_data.act_f3/max_deptn
    umdata.processed_data[!, [:act_normm1]] .= umdata.processed_data.act_m1/max_deptn
    umdata.processed_data[!, [:act_normm2]] .= umdata.processed_data.act_m2/max_deptn
    umdata.processed_data[!, [:act_normm3]] .= umdata.processed_data.act_m3/max_deptn
    umdata.processed_data[!, [:act_norm_deptn]] .= umdata.processed_data.act_deptn/max_deptn
    umdata.processed_data[!, [:act_norm_f]] .= umdata.processed_data.act_f/max_deptn
    umdata.processed_data[!, [:act_norm_m]] .= umdata.processed_data.act_m/max_deptn
    umdata.processed_data[!, [:act_ynorm_f1]] .= umdata.processed_data.act_f1 ./ umdata.processed_data.act_deptn
    umdata.processed_data[!, [:act_ynorm_f2]] .= umdata.processed_data.act_f2 ./ umdata.processed_data.act_deptn
    umdata.processed_data[!, [:act_ynorm_f3]] .= umdata.processed_data.act_f3 ./ umdata.processed_data.act_deptn
    umdata.processed_data[!, [:act_ynorm_m1]] .= umdata.processed_data.act_m1 ./ umdata.processed_data.act_deptn
    umdata.processed_data[!, [:act_ynorm_m2]] .= umdata.processed_data.act_m2 ./ umdata.processed_data.act_deptn
    umdata.processed_data[!, [:act_ynorm_m3]] .= umdata.processed_data.act_m3 ./ umdata.processed_data.act_deptn
    umdata.processed_data[!, [:act_ynorm_f]] .= umdata.processed_data.act_f ./ umdata.processed_data.act_deptn
    umdata.processed_data[!, [:act_ynorm_m]] .= umdata.processed_data.act_m ./ umdata.processed_data.act_deptn
end;


function _process_parameter_data!(umdata::UMDeptData)
    rattrf1 = mean.(eachcol(@rselect(umdata.processed_data, :rattrf1 = :act_fattr1 / :act_f1)))
    rattrf2 = mean.(eachcol(@rselect(umdata.processed_data, :rattrf2 = :act_fattr2 / :act_f2)))
    rattrf3 = mean.(eachcol(@rselect(umdata.processed_data, :rattrf3 = :act_fattr3 / :act_f3)))
    rattrm1 = mean.(eachcol(@rselect(umdata.processed_data, :rattrm1 = :act_mattr1 / :act_m1)))
    rattrm2 = mean.(eachcol(@rselect(umdata.processed_data, :rattrm2 = :act_mattr2 / :act_m2)))
    rattrm3 = mean.(eachcol(@rselect(umdata.processed_data, :rattrm3 = :act_mattr3 / :act_m3)))

    rhiref1 = mean.(eachcol(@rselect(umdata.processed_data, :rhiref1 = :act_fhire1 / :act_hire)))
    rhiref2 = mean.(eachcol(@rselect(umdata.processed_data, :rhiref2 = :act_fhire2 / :act_hire)))
    rhiref3 = mean.(eachcol(@rselect(umdata.processed_data, :rhiref3 = :act_fhire3 / :act_hire)))
    rhirem1 = mean.(eachcol(@rselect(umdata.processed_data, :rhirem1 = :act_mhire1 / :act_hire)))
    rhirem2 = mean.(eachcol(@rselect(umdata.processed_data, :rhirem2 = :act_mhire2 / :act_hire)))
    rhirem3 = mean.(eachcol(@rselect(umdata.processed_data, :rhirem3 = :act_mhire3 / :act_hire)))


    rpromf1 = mean.(eachcol(@rselect(umdata.processed_data, :rpromf1 = :act_fprom1 / :act_f1)))
    rpromf2 = mean.(eachcol(@rselect(umdata.processed_data, :rpromf2 = :act_fprom2 / :act_f2)))
    rpromm1 = mean.(eachcol(@rselect(umdata.processed_data, :rpromm1 = :act_mprom1 / :act_m1)))
    rpromm2 = mean.(eachcol(@rselect(umdata.processed_data, :rpromm2 = :act_mprom2 / :act_m2)))

    umdata.dept_rates = Dict(:rattrf1 => ifelse(isnan(first(rattrf1)), 0.001, round(first(rattrf1), digits=4)),
                        :rattrf2 => ifelse(isnan(first(rattrf2)), 0.001, round(first(rattrf2), digits=4)),
                        :rattrf3 => ifelse(isnan(first(rattrf3)), 0.001, round(first(rattrf3), digits=4)),
                        :rattrm1 => ifelse(isnan(first(rattrm1)), 0.001, round(first(rattrm1), digits=4)),
                        :rattrm2 => ifelse(isnan(first(rattrm2)), 0.001, round(first(rattrm2), digits=4)),
                        :rattrm3 => ifelse(isnan(first(rattrm3)), 0.001, round(first(rattrm3), digits=4)),
                        :rhiref1 => ifelse(isnan(first(rhiref1)), 0.001, round(first(rhiref1), digits=4)),
                        :rhiref2 => ifelse(isnan(first(rhiref2)), 0.001, round(first(rhiref2), digits=4)),
                        :rhiref3 => ifelse(isnan(first(rhiref3)), 0.001, round(first(rhiref3), digits=4)),
                        :rhirem1 => ifelse(isnan(first(rhirem1)), 0.001, round(first(rhirem1), digits=4)),
                        :rhimem2 => ifelse(isnan(first(rhirem2)), 0.001, round(first(rhirem2), digits=4)),
                        :rhirem3 => ifelse(isnan(first(rhirem3)), 0.001, round(first(rhirem3), digits=4)),
                        :rpromf1 => ifelse(isnan(first(rpromf1)), 0.001, round(first(rpromf1), digits=4)),
                        :rpromf2 => ifelse(isnan(first(rpromf2)), 0.001, round(first(rpromf2), digits=4)),
                        :rpromm1 => ifelse(isnan(first(rpromm1)), 0.001, round(first(rpromm1), digits=4)),
                        :rpromm2 => ifelse(isnan(first(rpromm2)), 0.001, round(first(rpromm2), digits=4)))
end;



function _process_sindy_matrix!(umdata::UMDeptData)

    mat = hcat(umdata.processed_data.act_normf1, 
                        umdata.processed_data.act_normf2, 
                        umdata.processed_data.act_normf3, 
                        umdata.processed_data.act_normm1, 
                        umdata.processed_data.act_normm2, 
                        umdata.processed_data.act_normm3)
    mat .= ifelse.(isnan.(mat), 0, mat)
    umdata.sindy_matrix = mat
end;


function _compute_bootstrap_data(x::Vector{Float64}, 
                                period::Integer, 
                                bootiter::Integer, 
                                blocksize::Integer)

    res1 = R"""
    library(forecast) 
    library(data.table) # manipulating the data

    d = as.numeric($x)
    period <- $period # yearly period

    data_boot_mbb <- bld.mbb.bootstrap(ts(d, freq = period), $bootiter, $blocksize)
    # data_plot <- data.table(Value = unlist(data_boot_mbb),
    #                         ID = rep(1:length(data_boot_mbb), each = length(data_ts)),
    #                         Time = rep(1:length(data_ts), length(data_boot_mbb))
    #                         )
    """

    res2 = R"""
    library(forecast); 
    library(data.table); # manipulating the data

    d = as.numeric($x)
    period <- $period # yearly period

    data_boot_mbb <- bld.mbb.bootstrap(ts(d, freq = period), $bootiter, $blocksize)
    data_plot <- data.table(Value = unlist(data_boot_mbb),
                            ID = rep(1:length(data_boot_mbb), each = length(d)),
                            Time = rep(1:length(d), length(data_boot_mbb))
                            )
    """
    r = (rcopy(res1), rcopy(res2))
    return r
end;


function _process_bootstrap_data!(umdata::UMDeptData)
    cols = umdata._data_cols
    bootcols = umdata._bootstrap_cols
    deriv_cols = umdata._bootstrap_derivative_cols
    norm_cols = umdata._bootstrap_norm_cols

    total_cols = vcat(bootcols, deriv_cols, norm_cols)
    num_rows = length(umdata.processed_data.year)
    temp_df = DataFrame(zeros(num_rows, length(total_cols)), total_cols)
    for (i, c) in enumerate(cols)
        # compute the bootstrap
        t1, _ = _compute_bootstrap_data(convert(Vector{Float64}, umdata.processed_data[:, c]),
                                umdata._period, 
                                umdata._bootiter,
                                umdata._blocksize)
        temp_df[:, [bootcols[i]]] .= mean(t1)
        temp_df[:, [bootcols[i]]] .= ifelse.(temp_df[:, [bootcols[i]]] .< 0.0, 0.0, temp_df[:, [bootcols[i]]])


        # compute derivative at each point.
        xs = convert(Vector{Float64}, sort(umdata.processed_data.year))
        itp = interpolate(mean(t1), BSpline(Cubic(Natural(OnCell()))))
        itp = scale(itp, range(start=minimum(xs), step=1, stop=maximum(xs)))
        temp_df[:, [deriv_cols[i]]] .= [Interpolations.gradient(itp, x)[1] for x in xs]
    end

    temp_df[!, :boot_deptn] .= 0
    temp_df[!, [:boot_deptn]] = @select(temp_df, :boot_deptn = :boot_f1 + :boot_f2 + :boot_f3 + :boot_m1 + :boot_m2 + :boot_m3)

    max_deptn = maximum(temp_df.boot_deptn)
    temp_df[!, [:boot_norm_f1]] .= temp_df.boot_f1/max_deptn
    temp_df[!, [:boot_norm_f2]] .= temp_df.boot_f2/max_deptn
    temp_df[!, [:boot_norm_f3]] .= temp_df.boot_f3/max_deptn
    temp_df[!, [:boot_norm_m1]] .= temp_df.boot_m1/max_deptn
    temp_df[!, [:boot_norm_m2]] .= temp_df.boot_m2/max_deptn
    temp_df[!, [:boot_norm_m3]] .= temp_df.boot_m3/max_deptn
    temp_df[!, [:boot_norm_deptn]] .= temp_df.boot_deptn/max_deptn
    temp_df[!, [:boot_norm_m]] .= temp_df.boot_m/max_deptn
    temp_df[!, [:boot_norm_f]] .= temp_df.boot_f/max_deptn

    temp_df[!, [:boot_ynorm_f1]] .= temp_df.boot_f1 ./ temp_df.boot_deptn
    temp_df[!, [:boot_ynorm_f2]] .= temp_df.boot_f2 ./ temp_df.boot_deptn
    temp_df[!, [:boot_ynorm_f3]] .= temp_df.boot_f3 ./ temp_df.boot_deptn
    temp_df[!, [:boot_ynorm_m1]] .= temp_df.boot_m1 ./ temp_df.boot_deptn
    temp_df[!, [:boot_ynorm_m2]] .= temp_df.boot_m2 ./ temp_df.boot_deptn
    temp_df[!, [:boot_ynorm_m3]] .= temp_df.boot_m3 ./ temp_df.boot_deptn
    temp_df[!, [:boot_ynorm_f]] .= temp_df.boot_f ./ temp_df.boot_deptn
    temp_df[!, [:boot_ynorm_m]] .= temp_df.boot_m ./ temp_df.boot_deptn
    


    umdata._u0[!, [:u0_act_bootnorm]] .= umdata._u0.u0_act_unnormalized/max_deptn
    umdata._u0[:, :u0_boot_bootnorm] = vec(Array(temp_df[1, [:boot_norm_f1, 
                                                            :boot_norm_f2, 
                                                            :boot_norm_f3, 
                                                            :boot_norm_m1, 
                                                            :boot_norm_m2, 
                                                            :boot_norm_m3]]))
    temp_df.year .= sort(umdata.processed_data.year)
    temp_df.dept_name .= umdata.dept_name
    umdata.bootstrap_df = temp_df



end;


function _compute_smoothing_spline(x::Vector{Float64},
                                    y::Vector{Float64}; 
                                    spar::Float64)

    res = R"""
        sp <- ifelse($spar==0.0, NULL, $spar)
        mod <- smooth.spline($x, $y, spar=sp)
        pred_y <-predict(mod, $x)$y
        pred_y
    """
    r = rcopy(res)
    return r
end;

function _process_spline_data!(umdata::UMDeptData)
    cols = umdata._data_cols
    spcols = umdata._spline_cols
    deriv_cols = umdata._spline_derivative_cols
    norm_cols = umdata._spline_norm_cols
    total_cols = vcat(spcols, deriv_cols, norm_cols)
    num_rows = length(umdata.processed_data.year)
    temp_df = DataFrame(zeros(num_rows, length(total_cols)), total_cols)
    for (i, c) in enumerate(cols)
        # compute spline
        if umdata.dept_name == "FLINT SOM GRAD AD/DEV"
            @debug("data vector: $(umdata.processed_data[:, c])")
        end
        t1 = _compute_smoothing_spline(convert(Vector{Float64}, umdata.processed_data.year),
                                        convert(Vector{Float64}, umdata.processed_data[:, c]),
                                        spar=umdata.smoothing_spline_spar)
        
        temp_df[:, [spcols[i]]] .= t1
        temp_df[:, [spcols[i]]] .= ifelse.(temp_df[:, [spcols[i]]] .< 0.0, 0.0, temp_df[:, [spcols[i]]])
        #@show c
        # if c == :act_fhire1
        #     @show temp_df[:, [spcols[i]]]
        # end
        # compute derivative at each point.
        xs = convert(Vector{Float64}, sort(umdata.processed_data.year))
        itp = interpolate(t1, BSpline(Cubic(Line(OnCell()))))
        itp = scale(itp, range(start=minimum(xs), step=1, stop=maximum(xs)))
        temp_df[:, [deriv_cols[i]]] .= [Interpolations.gradient(itp, x)[1] for x in xs]
    end

    temp_df[!, :spline_deptn] .= 0
    temp_df[!, [:spline_deptn]] = @select(temp_df, :spline_deptn = :spline_f1 + :spline_f2 + :spline_f3 + :spline_m1 + :spline_m2 + :spline_m3)

    max_deptn = maximum(temp_df.spline_deptn)
    temp_df[!, [:spline_norm_f1]] .= temp_df.spline_f1/max_deptn
    temp_df[!, [:spline_norm_f2]] .= temp_df.spline_f2/max_deptn
    temp_df[!, [:spline_norm_f3]] .= temp_df.spline_f3/max_deptn
    temp_df[!, [:spline_norm_m1]] .= temp_df.spline_m1/max_deptn
    temp_df[!, [:spline_norm_m2]] .= temp_df.spline_m2/max_deptn
    temp_df[!, [:spline_norm_m3]] .= temp_df.spline_m3/max_deptn
    temp_df[!, [:spline_norm_deptn]] .= temp_df.spline_deptn/max_deptn
    temp_df[!, [:spline_norm_m]] .= temp_df.spline_m/max_deptn
    temp_df[!, [:spline_norm_f]] .= temp_df.spline_f/max_deptn

    temp_df[!, [:spline_ynorm_f1]] .= temp_df.spline_f1 ./ temp_df.spline_deptn
    temp_df[!, [:spline_ynorm_f2]] .= temp_df.spline_f2 ./ temp_df.spline_deptn
    temp_df[!, [:spline_ynorm_f3]] .= temp_df.spline_f3 ./ temp_df.spline_deptn
    temp_df[!, [:spline_ynorm_m1]] .= temp_df.spline_m1 ./ temp_df.spline_deptn
    temp_df[!, [:spline_ynorm_m2]] .= temp_df.spline_m2 ./ temp_df.spline_deptn
    temp_df[!, [:spline_ynorm_m3]] .= temp_df.spline_m3 ./ temp_df.spline_deptn
    temp_df[!, [:spline_ynorm_m]] .= temp_df.spline_m ./ temp_df.spline_deptn
    temp_df[!, [:spline_ynorm_f]] .= temp_df.spline_f ./ temp_df.spline_deptn

    umdata.bootstrap_df = hcat(umdata.bootstrap_df, temp_df)


    umdata._u0[!, [:u0_act_splinenorm]] .= umdata._u0.u0_act_unnormalized/max_deptn
    umdata._u0[:, :u0_spline_splinenorm] = vec(Array(temp_df[1, [:spline_norm_f1, 
                                                            :spline_norm_f2, 
                                                            :spline_norm_f3, 
                                                            :spline_norm_m1, 
                                                            :spline_norm_m2, 
                                                            :spline_norm_m3]]))

end;


function __genduniv_dde!(du, u, h, p, t)

    du[1] = p.rhire_f1*u[1] - p.rattr_f1*u[1] - p.rprom_f1*h(p, t-6.0)[1]
    du[2] = p.rhire_f2*u[2] + p.rprom_f1*u[1] - p.rattr_f2*u[2] - p.rprom_f2*h(p, t-6.0)[2] 
    du[3] = p.rhire_f3*u[3] + p.rprom_f2*u[2] - p.rattr_f3*u[3]
    du[4] = p.rhire_m1*u[4] - p.rattr_m1*u[4] - p.rprom_m1*h(p, t-6.0)[4]
    du[5] = p.rhire_m2*u[5] + p.rprom_m1*u[4] - p.rattr_m2*u[5] - p.rprom_m2*h(p, t-6.0)[5]
    du[6] = p.rhire_m3*u[6] + p.rprom_m2*u[5] - p.rattr_m3*u[6]
    du
end;





function __callback_plot_fulldata(p, l, pred; doplot = true)

    # TODO set the anim variable in the function.
    #display(l)
    # plot current prediction against data
    plt1 = scatter(tsteps, full_data[1, :], linewidth=5, title = "UMich F1, $(round(l, digits=3))")
    plt2 = scatter(tsteps, full_data[2, :], linewidth=5, title = "F2")
    plt3 = scatter(tsteps, full_data[3, :], linewidth=5, title = "F3")
    plt4 = scatter(tsteps, full_data[4, :], linewidth=5, title = "M1")
    plt5 = scatter(tsteps, full_data[5, :], linewidth=5, title = "M2")
    plt6 = scatter(tsteps, full_data[6, :], linewidth=5, title = "M3")
    plot!(plt1, tsteps, pred[1,:])
    plot!(plt2, tsteps, pred[2,:])
    plot!(plt3, tsteps, pred[3,:])
    plot!(plt4, tsteps, pred[4,:])
    plot!(plt5, tsteps, pred[5,:])
    plot!(plt6, tsteps, pred[6,:])

    if doplot

        plts = plot(plt1, plt2, plt3, plt4, plt5, plt6, layout=(2,3), legend=false)
        frame(anim)
        display(plot(plts))
    end
    return false
end;


function _optimization_dde(umdata::UMDeptData, ::NoAudit)

    tspan = umdata._tspan
    u0 = umdata._u0.u0_act_bootnorm
    initial_params = umdata._initial_params_dde
    full_data = umdata.bootstrap_df[:, [:boot_norm_f1, :boot_norm_f2, :boot_norm_f3, :boot_norm_m1, :boot_norm_m2, :boot_norm_m3]]
    full_data = transpose(Array(full_data))[:, Int(tspan[1]):Int(tspan[2])]
    
    #throw(ErrorException("just checking stuff"))

    h(p, t) = zeros(6)
    lags = [6.0]

    genduniv_dde_prob = DDEProblem(__genduniv_dde!,
    u0,
    h,
    tspan,
    umdata._initial_params_dde,
    constant_lags=lags)

    function __loss_dde(p)

        alg = MethodOfSteps(Rosenbrock23())
        genduniv_dde_prob = remake(genduniv_dde_prob, p=p)
        sol = Array(solve(genduniv_dde_prob, alg, saveat=1.0))
        loss = sum(abs2, full_data .- sol)
        return loss, sol
    end
    
    adtype = Optimization.AutoForwardDiff()
    optf = Optimization.OptimizationFunction((p,x) -> __loss_dde(p), adtype)
    optprob = Optimization.OptimizationProblem(optf, initial_params)

    res_opt_dde = Optimization.solve(optprob, 
                                    ADAM(0.01),
                                    maxiters=150)

    optprob2 = remake(optprob, u0 = res_opt_dde.u)

    res_opt_dde = Optimization.solve(optprob2, 
                                    BFGS(initial_stepnorm=0.01),
                                    allow_f_increases=true, 
                                    maxiters=200)

    umdata._final_params_dde = res_opt_dde.u

    genduniv_dde_prob = remake(genduniv_dde_prob, p = res_opt_dde.u)
    sol = transpose(Array(solve(genduniv_dde_prob, MethodOfSteps(Rosenbrock23()), saveat=1.0)))
    temp_df = DataFrame(sol, umdata._optimization_cols)
    temp_df[!, :year] .= 0
    temp_df.year .= sort(umdata.processed_data.year[Int(tspan[1]):Int(tspan[2])])

    umdata.optimization_df = temp_df
end;

function _optimization_dde(umdata::UMDeptData, ::DataAudit)

    tspan = umdata._tspan
    u0 = umdata._u0.u0_act_bootnorm
    initial_params = umdata._initial_params_dde
    full_data = umdata.bootstrap_df[:, [:boot_norm_f1, :boot_norm_f2, :boot_norm_f3, :boot_norm_m1, :boot_norm_m2, :boot_norm_m3]]
    full_data = transpose(Array(full_data))[:, Int(tspan[1]):Int(tspan[2])]

    h(p, t) = zeros(6)
    lags = [6.0]

    genduniv_dde_prob = DDEProblem(__genduniv_dde!,
    u0,
    h,
    tspan,
    umdata._initial_params_dde,
    constant_lags=lags)


    function __loss_dde(p)

        alg = MethodOfSteps(Rosenbrock23())
        genduniv_dde_prob = remake(genduniv_dde_prob, p=p)
        sol = Array(solve(genduniv_dde_prob, alg, saveat=1.0))
        loss = sum(abs2, full_data .- sol)
        return loss, sol
    end

    adtype = Optimization.AutoForwardDiff()
    optf = Optimization.OptimizationFunction((p,x) -> __loss_dde(p), adtype)
    optprob = Optimization.OptimizationProblem(optf, initial_params)


    res_opt_dde = Optimization.solve(optprob, 
                                    ADAM(0.01),
                                    #callback=__callback_plot_fulldata, 
                                    maxiters=200)

    optprob2 = remake(optprob, u0 = res_opt_dde.u)

    res_opt_dde = Optimization.solve(optprob2, 
                                    BFGS(initial_stepnorm=0.01),
                                    #callback = __callback_plot_fulldata,
                                    allow_f_increases=true, 
                                    maxiters=200)

    umdata._final_params_dde = res_opt_dde.u

    genduniv_dde_prob = remake(genduniv_dde_prob, p = res_opt_dde.u)
    sol = transpose(Array(solve(genduniv_dde_prob, MethodOfSteps(Rosenbrock23()), saveat=1.0)))
    temp_df = DataFrame(sol, umdata._optimization_cols)
    temp_df[!, :year] .= 0
    temp_df.year .= sort(umdata.processed_data.year[Int(tspan[1]):Int(tspan[2])])

    umdata.optimization_df = temp_df
end;


function _finalize_audit(umdata::UMDeptData, ::NoAudit)
    umdata.processed_data = hcat(umdata.processed_data, select(umdata.bootstrap_df, Not([:year, :dept_name])))
end;


function _finalize_audit(umdata::UMDeptData, ::DataAudit)
    dt = Dates.format(Dates.now(), "yyyy-mm-dd HH:MM:SS")
    umdata.processed_data = hcat(umdata.processed_data, select(umdata.bootstrap_df, Not([:year, :dept_name])))

    cols = [:act_f1, :act_f2, :act_f3, :act_m1, :act_m2, :act_m3]
    spcols = [:spline_f1, :spline_f2, :spline_f3, :spline_m1, :spline_m2, :spline_m3]
    bootcols = [:boot_f1, :boot_f2, :boot_f3, :boot_m1, :boot_m2, :boot_m3]
    boot_norm_cols = [:boot_norm_f1, :boot_norm_f2, :boot_norm_f3, :boot_norm_m1, :boot_norm_m2, :boot_norm_m3]
    deriv_cols = [:spline_deriv_f1, :spline_deriv_f2, :spline_deriv_f3, :spline_deriv_m1,
                    :spline_deriv_m2, :spline_deriv_m3]
    optcols = umdata._optimization_cols

    newfn = "audit/audit_output_" * umdata.dept_name * string(dt) * ".pdf"
    all_combos = combinations(spcols, 2)

    # Plot the data, including splines and bootstrap data. 
    for i in 1:6
        p = scatter(umdata.processed_data[:, :year], umdata.processed_data[:, cols[i]], 
                title="Dept: $(umdata.dept_name), Group: $(String(cols[i])) data",
                linewidth=2, label="data")
        plot!(p, umdata.processed_data[:, :year], umdata.processed_data[:, bootcols[i]],
                linewidth=2, label="bootstrap")
        plot!(p, umdata.processed_data[:, :year], umdata.processed_data[:, spcols[i]], 
                linewidth=2, label="spline")        
        savefig(p, "tmp.pdf")
        append_pdf!(newfn, "tmp.pdf", cleanup=true)
    end

    for i in 1:6
        p = scatter(umdata.bootstrap_df[:, :year], umdata.bootstrap_df[:, boot_norm_cols[i]], 
                title="Opt: $(umdata.dept_name), Group: $(String(cols[i])) data",
                linewidth=2, label="data")
        plot!(p, umdata.optimization_df[:, :year], umdata.optimization_df[:, optcols[i]],
                linewidth=2, label="opt")        
        savefig(p, "tmp.pdf")
        append_pdf!(newfn, "tmp.pdf", cleanup=true)
    end
    # for (c1, c2) in all_combos
    #     p2 = plot(umdata.processed_data[:, c1], umdata.processed_data[:, c2],
    #                 title="Phase portrait $(String(c1)) versus $(String(c2))",
    #                 linewidth=2, label = "spline")
    #     savefig(p2, "tmp.pdf")
    #     append_pdf!(newfn, "tmp.pdf", cleanup=true)
    # end

    # all_combos = combinations(deriv_cols, 2) 
    # for (c1, c2) in all_combos
    #     p2 = plot(umdata.processed_data[:, c1], umdata.processed_data[:, c2],
    #                 title="Derivative Phase Portrait $(String(c1)) versus $(String(c2))",
    #                 linewidth=2, label = "spline")
    #     savefig(p2, "tmp.pdf")
    #     append_pdf!(newfn, "tmp.pdf", cleanup=true)
    # end


end;

function preprocess_um_data(df::DataFrame, first_year::Integer, num_years::Integer, audit_config::JuliaGendUniv_Types.AbstractDataChecks)
    um_data = _setup_um_preprocessing(df, first_year, num_years, audit_config)
    _process_department_numbers!(um_data, audit_config)
    _process_department_hires_attr_promotion!(um_data, audit_config)
    _process_summary_data!(um_data)
    _process_parameter_data!(um_data)
    _process_clusterpoint_vector!(um_data)
    _process_sindy_matrix!(um_data)
    _process_bootstrap_data!(um_data)
    _process_spline_data!(um_data)
    #_process_cluster_vectors!(um_data)
    # _optimization_dde(um_data, audit_config)
    # _finalize_audit(um_data, audit_config) 
    return um_data
end;

