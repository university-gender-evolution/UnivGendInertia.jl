using UnivGendInertia
using Documenter

DocMeta.setdocmeta!(UnivGendInertia, :DocTestSetup, :(using UnivGendInertia); recursive=true)

makedocs(;
    modules=[UnivGendInertia],
    authors="Krishna Bhogaonker",
    sitename="UnivGendInertia.jl",
    format=Documenter.HTML(;
        canonical="https://00krishna.github.io/UnivGendInertia.jl",
        edit_link="main",
        assets=String[],
    ),
    pages=[
        "Home" => "index.md",
    ],
)

deploydocs(;
    repo="github.com/00krishna/UnivGendInertia.jl",
    devbranch="main",
)
