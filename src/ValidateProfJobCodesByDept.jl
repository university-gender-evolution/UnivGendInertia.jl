
function _validation_adjust_depts(univ_data::GendUnivData)

    #=
        This is the overall function to resolve data issues in department names and 
        job codes, etc. 
    =# 

    _validation_adjust_dept_names(univ_data)

end


function _validation_adjust_dept_names(univ_data::GendUnivData)

    #= 
        In many cases, department names were changed over time. This function will 
        harmonize those names, so that department analysis looks over the complete timespan. 

    =#
    

    replace!(univ_data._raw_df.orgname, "POLITICAL SCIENCE DEPARTMENT" => "LSA POLITICAL SCIENCE")
    replace!(univ_data._raw_df.orgname, "LS&A POLITICAL SCIENCE DEPT" => "LSA POLITICAL SCIENCE")

    replace!(univ_data._raw_df.orgname, "ECONOMICS DEPARTMENT" => "LSA ECONOMICS")
    replace!(univ_data._raw_df.orgname, "LS&A ECONOMICS DEPARTMENT" => "LSA ECONOMICS")

    replace!(univ_data._raw_df.orgname, "SOCIOLOGY DEPARTMENT" => "LSA SOCIOLOGY")
    replace!(univ_data._raw_df.orgname, "LS&A SOCIOLOGY DEPARTMENT" => "LSA SOCIOLOGY")

    replace!(univ_data._raw_df.orgname, "PHYSICS DEPARTMENT" => "LSA PHYSICS")
    replace!(univ_data._raw_df.orgname, "LS&A PHYSICS DEPARTMENT" => "LSA PHYSICS")

    replace!(univ_data._raw_df.orgname, "CHEMISTRY DEPARTMENT" => "LSA CHEMISTRY")
    replace!(univ_data._raw_df.orgname, "LS&A CHEMISTRY DEPARTMENT" => "LSA CHEMISTRY")

    replace!(univ_data._raw_df.orgname, "SCHOOL OF BUSINESS ADMIN" => "ROSS SCHOOL OF BUSINESS")

    replace!(univ_data._raw_df.orgname, "LS&A DEPARTMENT OF LINGUISTICS" => "LSA LINGUISTICS")

    replace!(univ_data._raw_df.orgname, "LS&A CLASSICAL STUDIES DEPT" => "LSA CLASSICAL STUDIES")
    replace!(univ_data._raw_df.orgname, "CLASSICAL STUDIES DEPARTMENT" => "LSA CLASSICAL STUDIES")

    replace!(univ_data._raw_df.orgname, "LS&A CHEMISTRY DEPARTMENT" => "LSA CHEMISTRY")

    replace!(univ_data._raw_df.orgname, "CHEMISTRY DEPARTMENT" => "LSA CHEMISTRY")
    replace!(univ_data._raw_df.orgname, "LS&A CHEMISTRY DEPARTMENT" => "LSA CHEMISTRY")


    #Computer science, engineering.

end








