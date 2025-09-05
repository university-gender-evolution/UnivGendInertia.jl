
function _process_clusterpoint_vector!(umdata::UMDeptData)
    # stack data for clustering                         
    cluster_pt = vcat(umdata.processed_data.act_normf1, 
                        umdata.processed_data.act_normf2, 
                        umdata.processed_data.act_normf3, 
                        umdata.processed_data.act_normm1, 
                        umdata.processed_data.act_normm2, 
                        umdata.processed_data.act_normm3)
    cluster_pt .= ifelse.(isnan.(cluster_pt), 0, cluster_pt)
    umdata.cluster_vector = cluster_pt
end;


function _process_cluster_vector_agg_norm!(umdata::UMDeptData)
    # stack data for clustering                         
    cluster_pt = vcat(umdata.processed_data.act_norm_f,
                        umdata.processed_data.act_norm_m)
    cluster_pt .= ifelse.(isnan.(cluster_pt), 0, cluster_pt)
    umdata.cluster_data.cluster_vector_agg_norm = cluster_pt
end;


function _process_cluster_vector_agg_ynorm!(umdata::UMDeptData)
    # stack data for clustering                         
    cluster_pt = vcat(umdata.processed_data.act_ynorm_f,
                        umdata.processed_data.act_ynorm_m)
    cluster_pt .= ifelse.(isnan.(cluster_pt), 0, cluster_pt)
    umdata.cluster_data.cluster_vector_agg_ynorm = cluster_pt
end;


function _process_cluster_vector_detail_norm!(umdata::UMDeptData)
    # stack data for clustering                         
    cluster_pt = vcat(umdata.processed_data.act_normf1, 
                        umdata.processed_data.act_normf2, 
                        umdata.processed_data.act_normf3, 
                        umdata.processed_data.act_normm1, 
                        umdata.processed_data.act_normm2, 
                        umdata.processed_data.act_normm3)
    cluster_pt .= ifelse.(isnan.(cluster_pt), 0, cluster_pt)
    umdata.cluster_data.cluster_vector_detail_norm = cluster_pt
end;


function _process_cluster_vector_detail_ynorm!(umdata::UMDeptData)
    # stack data for clustering                         
    cluster_pt = vcat(umdata.processed_data.act_ynorm_f1, 
                        umdata.processed_data.act_ynorm_f2, 
                        umdata.processed_data.act_ynorm_f3, 
                        umdata.processed_data.act_ynorm_m1, 
                        umdata.processed_data.act_ynorm_m2, 
                        umdata.processed_data.act_ynorm_m3)
    cluster_pt .= ifelse.(isnan.(cluster_pt), 0, cluster_pt)
    umdata.cluster_data.cluster_vector_detail_ynorm = cluster_pt
end;



function _process_cluster_vector_spline_agg_norm!(umdata::UMDeptData)
    # stack data for clustering                         
    cluster_pt = vcat(umdata.bootstrap_df.spline_norm_f,
                        umdata.bootstrap_df.spline_norm_m)
    cluster_pt .= ifelse.(isnan.(cluster_pt), 0, cluster_pt)
    umdata.cluster_data.cluster_vector_spline_agg_norm = cluster_pt
end;



function _process_cluster_vector_spline_agg_ynorm!(umdata::UMDeptData)
    # stack data for clustering                         
    cluster_pt = vcat(umdata.bootstrap_df.spline_ynorm_f,
                        umdata.bootstrap_df.spline_ynorm_m)
    cluster_pt .= ifelse.(isnan.(cluster_pt), 0, cluster_pt)
    umdata.cluster_data.cluster_vector_spline_agg_ynorm = cluster_pt
end;



function _process_cluster_vector_spline_detail_norm!(umdata::UMDeptData)
    # stack data for clustering                         
    cluster_pt = vcat(umdata.bootstrap_df.spline_norm_f1,
                        umdata.bootstrap_df.spline_norm_f2,
                        umdata.bootstrap_df.spline_norm_f3,
                        umdata.bootstrap_df.spline_norm_m1,
                        umdata.bootstrap_df.spline_norm_m2,
                        umdata.bootstrap_df.spline_norm_m3)
    cluster_pt .= ifelse.(isnan.(cluster_pt), 0, cluster_pt)
    umdata.cluster_data.cluster_vector_spline_detail_norm = cluster_pt
end;


function _process_cluster_vector_spline_detail_ynorm!(umdata::UMDeptData)
    # stack data for clustering                         
    cluster_pt = vcat(umdata.bootstrap_df.spline_ynorm_f1,
                        umdata.bootstrap_df.spline_ynorm_f2,
                        umdata.bootstrap_df.spline_ynorm_f3,
                        umdata.bootstrap_df.spline_ynorm_m1,
                        umdata.bootstrap_df.spline_ynorm_m2,
                        umdata.bootstrap_df.spline_ynorm_m3)
    cluster_pt .= ifelse.(isnan.(cluster_pt), 0, cluster_pt)
    umdata.cluster_data.cluster_vector_spline_detail_ynorm = cluster_pt
end;


function _process_cluster_vector_act_norm_deptn!(umdata::UMDeptData)
    # stack data for clustering                         
    cluster_pt = umdata.processed_data.act_norm_deptn
    cluster_pt .= ifelse.(isnan.(cluster_pt), 0, cluster_pt)
    umdata.cluster_data.cluster_vector_act_norm_deptn = cluster_pt
end;


function _process_cluster_vector_spline_norm_deptn!(umdata::UMDeptData)
    # stack data for clustering                         
    cluster_pt = umdata.bootstrap_df.spline_norm_deptn
    cluster_pt .= ifelse.(isnan.(cluster_pt), 0, cluster_pt)
    umdata.cluster_data.cluster_vector_spline_norm_deptn = cluster_pt
end;


function _process_cluster_vectors!(umdata::UMDeptData)
    _process_cluster_vector_agg_norm!(umdata)
    _process_cluster_vector_agg_ynorm!(umdata)
    _process_cluster_vector_detail_norm!(umdata)
    _process_cluster_vector_detail_ynorm!(umdata)
    _process_cluster_vector_spline_agg_norm!(umdata)
    _process_cluster_vector_spline_agg_ynorm!(umdata)
    _process_cluster_vector_spline_detail_norm!(umdata)
    _process_cluster_vector_spline_detail_ynorm!(umdata)
    _process_cluster_vector_act_norm_deptn!(umdata)
    _process_cluster_vector_spline_norm_deptn!(umdata)
end;
    


function aggregate_cluster_vectors_to_matrix(univdata::JuliaGendUniv_Types.GendUnivData)
    d_agg_norm = [univdata.dept_data_vector[i].cluster_data.cluster_vector_agg_norm for i in 1:length(univdata.dept_data_vector)]
    d_agg_ynorm = [univdata.dept_data_vector[i].cluster_data.cluster_vector_agg_ynorm for i in 1:length(univdata.dept_data_vector)]
    d_detail_norm = [univdata.dept_data_vector[i].cluster_data.cluster_vector_detail_norm for i in 1:length(univdata.dept_data_vector)]
    d_detail_ynorm = [univdata.dept_data_vector[i].cluster_data.cluster_vector_detail_ynorm for i in 1:length(univdata.dept_data_vector)]
    d_spline_agg_norm = [univdata.dept_data_vector[i].cluster_data.cluster_vector_spline_agg_norm for i in 1:length(univdata.dept_data_vector)]
    d_spline_agg_ynorm = [univdata.dept_data_vector[i].cluster_data.cluster_vector_spline_agg_ynorm for i in 1:length(univdata.dept_data_vector)]
    d_spline_detail_norm = [univdata.dept_data_vector[i].cluster_data.cluster_vector_spline_detail_norm for i in 1:length(univdata.dept_data_vector)]
    d_spline_detail_ynorm = [univdata.dept_data_vector[i].cluster_data.cluster_vector_spline_detail_ynorm for i in 1:length(univdata.dept_data_vector)]
    d_act_norm_deptn = [univdata.dept_data_vector[i].cluster_data.cluster_vector_act_norm_deptn for i in 1:length(univdata.dept_data_vector)]
    d_spline_norm_deptn = [univdata.dept_data_vector[i].cluster_data.cluster_vector_spline_norm_deptn for i in 1:length(univdata.dept_data_vector)]

    univdata.clustering.aggregated_norm.raw_matrix = reduce(hcat, d_agg_norm)
    univdata.clustering.aggregated_ynorm.raw_matrix = reduce(hcat, d_agg_ynorm)
    univdata.clustering.detail_norm.raw_matrix = reduce(hcat, d_detail_norm)
    univdata.clustering.detail_ynorm.raw_matrix = reduce(hcat, d_detail_ynorm)
    univdata.clustering.spline_aggregated_norm.raw_matrix = reduce(hcat, d_spline_agg_norm)
    univdata.clustering.spline_aggregated_ynorm.raw_matrix = reduce(hcat, d_spline_agg_ynorm)
    univdata.clustering.spline_detail_norm.raw_matrix = reduce(hcat, d_spline_detail_norm)
    univdata.clustering.spline_detail_ynorm.raw_matrix = reduce(hcat, d_spline_detail_ynorm)
    univdata.clustering.act_norm_deptn.raw_matrix = reduce(hcat, d_act_norm_deptn)
    univdata.clustering.spline_norm_deptn.raw_matrix = reduce(hcat, d_spline_norm_deptn)
end;


function fit_pca!(clgroup)

        datamatrix = clgroup.raw_matrix
        M = MultivariateStats.fit(PCA, datamatrix; maxoutdim=3)
        clgroup.pca_matrix = MultivariateStats.predict(M, datamatrix)
end;


function fit_distance_matrix!(clgroup)

    datamatrix = clgroup.pca_matrix
    clgroup.distance_matrix = pairwise(Euclidean(), datamatrix)

end;


function fit_optimal_clusters!(clgroup)
    datamatrix = clgroup.pca_matrix

    try 
        nbclust2 = R"""
            library(NbClust)            
            pdf(file = NULL)

            hush=function(code){
                sink("/dev/null") # use /dev/null in UNIX
                tmp = code
                sink()
                return(tmp)
                }

            d = t($datamatrix)
            res = hush(NbClust(data=d, distance = "euclidean",
                    min.nc = 2, max.nc = 10, 
                    method = "complete", index ="all"));
            dev.off()
            res
            """;
        res = rcopy(nbclust2);
        clgroup.optimal_clustering = Int(median(res[:Best_nc][1, :]));
    catch e
        print(e)
        clgroup.optimal_clustering = 10
        # clgroup.optimal_clustering = 1
    end

end;


function fit_kmeans!(clgroup)
    datamatrix = clgroup.pca_matrix
    km = kmeans(datamatrix, clgroup.optimal_clustering; maxiter=200)
    clgroup.kmeans.cluster_sizes = km.counts
    clgroup.kmeans.weighted_cluster_sizes = km.wcounts
    clgroup.kmeans.assignments = km.assignments
    clgroup.kmeans.centers = km.centers
    clgroup.kmeans.dict["pca_model"]= MultivariateStats.fit(PCA, clgroup.raw_matrix; maxoutdim=3)
    clgroup.kmeans._graph["plt"] = _make_cluster_plot_pca(datamatrix, km, "Kmeans Clustering")
end;


function fit_kmedoids!(clgroup)
    datamatrix = clgroup.distance_matrix
    km = kmedoids(datamatrix, clgroup.optimal_clustering; maxiter=200)
    clgroup.kmedoids.cluster_sizes = km.counts
    clgroup.kmedoids.assignments = km.assignments
    med = Float64.(km.medoids)
    clgroup.kmedoids.centers = reshape(med, length(med), 1)
    clgroup.kmedoids._graph = _make_cluster_plot_pca(datamatrix, km, "Kmedoids Clustering")


end;


function fit_hierarchical_clustering!(clgroup)

    dist_matrix = clgroup.distance_matrix 
    clgroup.hierarchical_clustering.dict["hierarchical_clustering"] = hclust(dist_matrix, linkage=:single)

end;


function fit_dbscan!(clgroup)
    dist_matrix = clgroup.distance_matrix
    res = dbscan(dist_matrix, 1.0, min_neighbors=10, metric=nothing)
    clgroup.dbscan_clustering.assignments = res.assignments
    clgroup.dbscan_clustering.dict["clusters"] = res.clusters
end;


function fit_affinityprop!(clgroup)

    dist_matrix = clgroup.distance_matrix
    res = affinityprop(dist_matrix)
    ctr = Float64.(res.exemplars)
    clgroup.affinity_propagation.centers = reshape(ctr, length(ctr), 1)
    clgroup.affinity_propagation.assignments = res.assignments

end;

function _make_cluster_plot_pca(datamatrix, clustering_data, plot_title)
    a = assignments(clustering_data)
    datamatrix = map(x -> x + 0.5*rand(), datamatrix)

    p = scatter(datamatrix[1, :], datamatrix[2,:], datamatrix[3, :], 
        markersize=6,
        markercolor=a, 
        palette=:seaborn_colorblind, 
        title = plot_title)
    return Dict("plt0_0" => p)
end



function _process_clustering_analysis!(univdata::JuliaGendUniv_Types.GendUnivData)

    clgroups = [getfield(univdata.clustering, i) for i in fieldnames(typeof(univdata.clustering))]
    
    fit_pca!.(clgroups)
    fit_distance_matrix!.(clgroups)
    fit_optimal_clusters!.(clgroups)
    fit_kmeans!.(clgroups)
    fit_kmedoids!.(clgroups)
    fit_hierarchical_clustering!.(clgroups)
    fit_affinityprop!.(clgroups)
    fit_dbscan!.(clgroups)


end;