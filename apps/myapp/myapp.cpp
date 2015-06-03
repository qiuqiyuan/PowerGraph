#include <graphlab.hpp>
#include <stdlib.h>

//qqiu: inherit from POD do not require save and load to serialize
typedef struct node : public graphlab::IS_POD_TYPE {
    double x, y, z, mass;
} vertex_data_type;

class knn{
    public:
        edge_dir_type gather_edges(icontext_type& context, 
                const vertex_type& vertex) const{
            return graphlab::ALL_EDGES;
        }
        gather_type gather(){
        }
};

//how do you set this to gather type
//nothing special just return directly
//This is the object that is being passed by gather to apply
struct dist_vec{
    vector<float> dists; 
};

class setDistance{
    public:
    //for each connected point, calculate the distance and
    gather_type gather(icontext_type& context,
            const vertex_type& vertex) const{
        dist_vec vec;
        float cnt_dist = calculate_dist(edge.source(), vertex);
        vec.dists.push_back(cnt_dist);
        return dists;
    }

    apply(, , allneighbors){
        size_t allneighbors.dists.size();
        for all in-edges:
            dists.insert(cnt_dist);
        return success; 
    }

};

typedef graphlab::empty edge_data_type;

typedef graphlab::distributed_graph<vertex_data_type, 
        edge_data_type> graph_type;

//input file will need to specify number of points.
int main(int argc, char** argv) {
    //read in the graph somewhere here
    graphlab::mpi_tools::init(argc, argv);
    graphlab::distributed_control dc;

    graph_type graph;
    graphlab::distributed_control dc(graphtype, cml_opt);
    graphlab::omni_engine<setEdgeValue> setDistance(dc, graph, exec_type, clopts);

    //load graph;
    //graph.finalize();

    setDistance.signal_all(); //This marks all points as "active"
    setDistance.start();


    graphlab::mpi_tools::finalize();
    return EXIT_SUCCESS; //from stdlib.h
}
