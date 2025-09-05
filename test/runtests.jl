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


@testitem "[JuliaGendUniv] preprocess UM data No Audit" begin
    
using JuliaGendUniv_Types, Test
    
cd(@__DIR__)
@show pwd()


end




