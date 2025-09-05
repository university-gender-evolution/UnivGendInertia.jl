using UnivGendInertia
using Test
using TestItems


@testitem "[UnivGendInertia] environment setup" begin
    cd(@__DIR__)
    @show pwd()
    #@test isfile("michigan1979to2009_wGender.dta")
    @test UM() isa AbstractGendUnivDataConfiguration 
    @test DataAudit() isa AbstractDataChecks
    @test NoAudit() isa AbstractDataChecks
end


@testitem "[UnivGendInertia] preprocess UM data No Audit" begin
    
using Test
    
cd(@__DIR__)
@show pwd()

    t_preprocess_um_noaudit = preprocess_data("michigan1979to2009_wGender.dta", 
                                        1979, 30, UM(); audit_config=NoAudit());

    t_preprocess_um_deptname = preprocess_data("michigan1979to2009_wGender.dta", 
                            "PEDIATRIC SURGERY SECTION", UM(); audit_config=NoAudit());

    t_preprocess_um_deptindex = preprocess_data("michigan1979to2009_wGender.dta", 
                            165, UM(); audit_config=NoAudit());

    t_preprocess_um_deptname_year = preprocess_data("michigan1979to2009_wGender.dta",
                                "PEDIATRIC SURGERY SECTION", 1985, 20, UM(); 
                                audit_config=NoAudit());

    t_preprocess_um_deptindex_year = preprocess_data("michigan1979to2009_wGender.dta", 
                            165, 1985, 20, UM(); audit_config=NoAudit());

    @test t_preprocess_um_noaudit isa JuliaGendUniv_Types.UMData
    @test t_preprocess_um_noaudit.num_years == 30
    @test size(t_preprocess_um_noaudit._valid_dept_summary) == (525, 5)
    @test length(t_preprocess_um_noaudit.department_names) == 73
    @test t_preprocess_um_deptname.department_names[1] == "PEDIATRIC SURGERY SECTION"
    @test t_preprocess_um_deptindex.department_names[1] == "PEDIATRIC SURGERY SECTION"
    @test size(t_preprocess_um_deptname_year.processed_df) == (21, 39)
    @test minimum(t_preprocess_um_deptindex_year.processed_df.year) == t_preprocess_um_deptindex_year.first_year
    @test_throws DomainError preprocess_data("michigan1979to2009_wGender.dta", 165, 1985, 50, UM(); audit_config=NoAudit())
end




