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
using std::string;
using std::cout;
using std::cin;
using std::endl;
using std::make_pair;
using namespace graphlab;



typedef struct : public graphlab::IS_POD_TYPE 
{
    vector<double> cord;
    double mass;
} vertex_data_type;


struct set_union_gather{
    vector<double> dist;
    vector<vertex_id_type> vid;

    set_union_gather& operator+=(const set_union_gather& other){
        for(size_t i=0; i< other.dist.size(); i++){
            dist.push_back(other.dist[i]);
            vid.push_back(other.vid[i]);
        }
        return *this;
    }

    //serialize to disk 
    void save(graphlab::oarchive& oarc) const{
        oarc << dist << vid; 
    }

    //unserilize from disk to RAM
    void load(graphlab::iarchive& iarc) {
        iarc >> dist >> vid;
    }
};

typedef graphlab::empty edge_data_type;
typedef graphlab::distributed_graph<vertex_data_type, edge_data_type> graph_type;

class setDistance: public graphlab::ivertex_program<graph_type, 
    set_union_gather>, 
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

        template<typename T>
            void mySwap(T &x, T &y){
                T t;
                t = x;
                x = y;
                y = t;
            }

        template<typename T, typename T2>
            int partition (vector<T> &arr, vector<T2> &arr2,  int l, int h)
            {
                T x = arr[h];
                int i = (l - 1);

                for (int j = l; j <= h- 1; j++)
                {
                    if (arr[j] <= x)
                    {
                        i++;
                        mySwap (arr[i], arr[j]);
                    }
                }
                mySwap (arr[i + 1], arr[h]);
                return (i + 1);
            }

        template<typename T, typename T2>
            void myQuickSort(vector<T> &arr, vector<T2> &arr2, int l, int h)
            {
                int stack[ h - l + 1 ];
                int top = -1;
                stack[ ++top ] = l;
                stack[ ++top ] = h;
                while ( top >= 0 )
                {
                    h = stack[ top-- ];
                    l = stack[ top-- ];

                    // Set pivot element at its correct position in sorted array
                    int p = partition( arr, arr2, l, h );

                    // If there are elements on left side of pivot, then push left
                    // side to stack
                    if ( p-1 > l )
                    {
                        stack[ ++top ] = l;
                        stack[ ++top ] = p - 1;
                    }

                    // If there are elements on right side of pivot, then push right
                    // side to stack
                    if ( p+1 < h )
                    {
                        stack[ ++top ] = p + 1;
                        stack[ ++top ] = h;
                    }
                }
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
            gather.dist.push_back(dist);
            gather.vid.push_back(edge.target().id());
            return gather;
        }

        void apply(icontext_type &context, vertex_type &vertex, 
                const gather_type &neighbor){
            size_t ndist = neighbor.dist.size();
            size_t nvid = neighbor.vid.size();
            assert(ndist == nvid);
            gather_type cp_neighbor = neighbor;
            //my quick sort here
            myQuickSort(cp_neighbor.dist,cp_neighbor.vid,  0, ndist - 1);
            //resize and done
        }

        void scatter(icontext_type& context,
                const vertex_type& vertex,
                edge_type& edge) const {
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

    graphlab::async_consistent_engine<setDistance>engineSetDistance(dc, graph, clopts); 
    engineSetDistance.signal_all(); //This marks all points as "active"
    engineSetDistance.start();


    graphlab::mpi_tools::finalize();
    return EXIT_SUCCESS; //from stdlib.h
}
