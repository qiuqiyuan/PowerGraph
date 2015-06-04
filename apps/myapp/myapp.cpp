#include <graphlab.hpp>
#include <stdlib.h>
#include <algorithm>
#include <vector>
#include <utility> //for pair
#include <iostream>
#include <string>

using std::vector;
using std::pair;
using std::string;
using std::cout;
using std::cin;
using namespace graphlab;

//This should be modified by the commandline option.
static const int K = 4;

//qqiu: inherit from POD do not require save and load to serialize
struct collection_gather: public graphlab::IS_POD_TYPE {
    double x, y, z, mass;
    vector< double > KNN;

    collection_gather(){

    }

    collection_gather& operator+=(const collection_gather &other) {
        return *this;
    }
};




typedef graphlab::empty edge_data_type;

typedef graphlab::distributed_graph<collection_gather, 
        edge_data_type> graph_type;

class setDistance : public graphlab::ivertex_program<graph_type, vector<double>>{
    //double computeDist(const vertex_data_type& x, const vertex_data_type& y){
    //    return sqrt( pow(x.x-y.x, 2) + pow(x.y-y.y, 2) + pow(x.z-y.z, 2));
    //}
    public:
        //gather only inedges
        edge_dir_type gather_edges(icontext_type &context, 
                const vertex_type& vertex) const {
            return graphlab::IN_EDGES; 
        }
        //for each connected point, calculate the distance and
        gather_type gather(icontext_type &context, const vertex_type &vertex, const edge_type &edge) {
            vector<double> v;
            return v;
        }

        void apply(icontext_type& context, vertex_type& vertex, const gather_type &allneighbors){
            return;
        }

};



//input file will need to specify number of points.
int main(int argc, char** argv) 
{
    graphlab::mpi_tools::init(argc, argv);
    graphlab::distributed_control dc;

    graphlab::command_line_options clopts("KNN algorithm");


    //have to have a dc for some graph
    graph_type graph(dc);
    //graphlab::omni_engine<setDistance>engineSetDistance(dc, graph, "sync", clopts); 
    string path = "/u/qqiu/PowerGraph/release/apps/myapp/input/full";
    string format = "adj";
    graph.load_format(path,format);
    graph.finalize();

    //engineSetDistance.signal_all(); //This marks all points as "active"
    //engineSetDistance.start();


    graphlab::mpi_tools::finalize();
    return EXIT_SUCCESS; //from stdlib.h
}
