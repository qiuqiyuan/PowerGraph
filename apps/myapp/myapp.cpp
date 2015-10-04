#include <graphlab.hpp>
#include <stdlib.h>
#include <algorithm>
#include <vector>
#include <utility> //for pair
#include <iostream>
#include <string>
#include <assert.h>
#include <ctime>

using std::vector;
using std::pair;
using std::sort;
using std::string;
using std::cout;
using std::cin;
using std::min;
using std::endl;
using std::make_pair;
using std::stringstream;
using namespace graphlab;

//KNN dimension
static size_t KNN_D = 0;
//Number of S points in input file
static size_t NUM_S = 0;
static size_t K = 0;


// data structure on vertex
typedef struct{ 
    vector<double> cord;
    double mass;
    bool isS;
    //knn --> result after computation, initially empty 
    //pair <double, vertex_id_type>
    //double --> distance
    //vertex_id_type --> the v's id
    vector< pair<double, vertex_id_type> > knn;

    //serialize to disk 
    void save(graphlab::oarchive& oarc) const{
        oarc << cord << mass << isS << knn; 
    }

    //unserilize from disk to RAM
    void load(graphlab::iarchive& iarc) {
        iarc >> cord >> mass >> isS >> knn ; 
    }
} vertex_data_type;


// used as ?? 
struct set_union_gather{
    vector< pair<double, vertex_id_type> > dist_vid;

    //how to reduce two union_gather together. 
    set_union_gather& operator+=(const set_union_gather& other){
        for(size_t i=0; i< other.dist_vid.size(); i++){
            dist_vid.push_back(other.dist_vid[i]);
        }
        sort(dist_vid.begin(), dist_vid.end());
        dist_vid.resize(K);
        return *this;
    }

    //serialize to disk 
    void save(graphlab::oarchive& oarc) const{
        oarc << dist_vid; 
    }

    //unserilize from disk to RAM
    void load(graphlab::iarchive& iarc) {
        iarc >> dist_vid;
    }
};

typedef graphlab::empty edge_data_type;
typedef graphlab::distributed_graph<vertex_data_type, edge_data_type> graph_type;

class setDistance: public graphlab::ivertex_program<graph_type, set_union_gather>, 
    public graphlab::IS_POD_TYPE{

        double _getDist(const vector<double> &a, const vector<double>&b) const{
            assert(a.size() == b.size());
            double res = 0.0;
            for(size_t i=0;i<a.size();i++){
                res += pow(a[i] - b[i], 2);
            }
            return sqrt(res);
        }

        double getDist(vertex_data_type &a, vertex_data_type &b ){
            return _getDist(a.cord, b.cord);
        }

        template <typename T>
            void printArr( const vector<T> &arr, const string & msg ) const {
                cout<< msg <<endl;
                for (size_t i = 0; i < arr.size(); ++i )
                    cout << "dist: " << arr[i].first << " vid: " <<
                        arr[i].second << " ";
                cout << endl;
            }

        public:
        edge_dir_type gather_edges(icontext_type &context, 
                vertex_type vertex) const{
            return graphlab::OUT_EDGES;
        }

        //this happens for each edge 
        gather_type gather(icontext_type &context, 
                vertex_type &vertex,
                edge_type &edge){
            set_union_gather gather;
            double dist = getDist(edge.source().data(), edge.target().data());
            vertex_id_type vid = edge.target().id();
            gather.dist_vid.push_back(make_pair(dist, vid));
            return gather;
        }

        void apply(icontext_type &context, vertex_type &vertex, 
                const gather_type &neighbor){
            //should save result into graph
            if(vertex.id() == NUM_S + 1)
                printArr(neighbor.dist_vid, "knn");
        }

        void scatter(icontext_type& context,
                const vertex_type& vertex,
                edge_type& edge) const {
        }
    };

inline bool graph_loader(graph_type& graph,
        const std::string& filename,
        const std::string& line) {
    stringstream strm(line);

    graphlab::vertex_id_type vid(-1);

    strm >> vid;

    vertex_data_type data;
    for(size_t i=0;i<KNN_D; i++){
        double x;
        strm >> x;
        data.cord.push_back(x);
    }

    double mass;
    strm >> mass;
    data.mass = mass;

    data.isS = true;

    if(vid > NUM_S){
        data.isS = false;
    }
    graph.add_vertex(vid, data);

    if(vid > NUM_S)
        for(size_t i=1; i<=NUM_S ; i++){
            graph.add_edge(vid, i);
        }

    return true; // successful load
} // end of graph_loader

bool selectVertices(const graph_type::vertex_type &vertex){
    return (vertex.id() > NUM_S);
}

int main(int argc, char** argv) 
{
    graphlab::mpi_tools::init(argc, argv);
    graphlab::distributed_control dc;
    string input;

    graphlab::command_line_options clopts("KNN algorithm");
    clopts.attach_option("input", input, "input point sets inclues S and R");
    clopts.attach_option("K", K, "the K in knn");
    clopts.attach_option("num_s", NUM_S, "number of points in S set");
    clopts.attach_option("knn_d", KNN_D, "knn dimension");

    if(!clopts.parse(argc, argv) || input == ""){
        cout<< "Error in parsing command line arguments." <<endl;
        clopts.print_description();
        return EXIT_FAILURE;
    }

    dc.cout() << "Loading graph" <<endl;
    graphlab::timer timer;

    graph_type graph(dc, clopts);
    graph.load(input, graph_loader);
    dc.cout() << "Loading s points. Finished in "
        << timer.current_time() << endl;


    dc.cout() << "Finalizing graph. " << endl;
    timer.start();
    graph.finalize();
    dc.cout() << "Finalizing graph. Finished in "
        << timer.current_time() <<endl;

    dc.cout() << "Creating engine" << endl;
    graphlab::async_consistent_engine<setDistance>engineSetDistance(dc, graph, clopts); 

    //Start computation with R set
    graphlab::vertex_set r_set = graph.select(selectVertices);
    engineSetDistance.signal_vset(r_set); 
    engineSetDistance.start();


    graphlab::mpi_tools::finalize();
    return EXIT_SUCCESS; //from stdlib.h
}
