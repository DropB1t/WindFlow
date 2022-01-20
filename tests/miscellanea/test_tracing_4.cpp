/**************************************************************************************
 *  Copyright (c) 2019- Gabriele Mencagli
 *  
 *  This file is part of WindFlow.
 *  
 *  WindFlow is free software dual licensed under the GNU LGPL or MIT License.
 *  You can redistribute it and/or modify it under the terms of the
 *    * GNU Lesser General Public License as published by
 *      the Free Software Foundation, either version 3 of the License, or
 *      (at your option) any later version
 *    OR
 *    * MIT License: https://github.com/ParaGroup/WindFlow/blob/vers3.x/LICENSE.MIT
 *  
 *  WindFlow is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Lesser General Public License for more details.
 *  You should have received a copy of the GNU Lesser General Public License and
 *  the MIT License along with WindFlow. If not, see <http://www.gnu.org/licenses/>
 *  and <http://opensource.org/licenses/MIT/>.
 **************************************************************************************
 */

/*  
 *  Test 4 tracing support and interaction with web dashboard.
 *  
 *  +-------------------------------------------------------------------------+
 *  |  +-----+     +-----+     +-----+     +-----+     +-------+     +-----+  |
 *  |  |  S  |     |  F  |     |  M  |     |  M  |     | KW_TB |     |  S  |  |
 *  |  | CPU +---->+ CPU +---->+ GPU +---->+ GPU +---->+  CPU  +---->+ CPU |  |
 *  |  | (*) |     | (*) |     | (*) |     | (*) |     |  (*)  |     | (*) |  |
 *  |  +-----+     +-----+     +-----+     +-----+     +-------+     +-----+  |
 *  +-------------------------------------------------------------------------+
 */ 

// includes
#include<string>
#include<random>
#include<iostream>
#include<math.h>
#include<ff/ff.hpp>
#include<windflow.hpp>
#include<windflow_gpu.hpp>
#include"misc_common.hpp"

using namespace std;
using namespace chrono;
using namespace wf;

// main
int main(int argc, char *argv[])
{
    int option = 0;
    size_t stream_len = 0;
    size_t win_len = 0;
    size_t win_slide = 0;
    size_t n_keys = 1;
    // arguments from command line
    if (argc != 9) {
        cout << argv[0] << " -l [stream_length] -k [n_keys] -w [win length usec] -s [win slide usec]" << endl;
        exit(EXIT_SUCCESS);
    }
    while ((option = getopt(argc, argv, "l:k:w:s:")) != -1) {
        switch (option) {
            case 'l': stream_len = atoi(optarg);
                     break;
            case 'k': n_keys = atoi(optarg);
                     break;
            case 'w': win_len = atoi(optarg);
                     break;
            case 's': win_slide = atoi(optarg);
                     break;
            default: {
                cout << argv[0] << " -l [stream_length] -k [n_keys] -w [win length usec] -s [win slide usec]" << endl;
                exit(EXIT_SUCCESS);
            }
        }
    }
    // set random seed
    mt19937 rng;
    rng.seed(std::random_device()());
    size_t min = 1;
    size_t max = 9;
    std::uniform_int_distribution<std::mt19937::result_type> dist_p(min, max);
    std::uniform_int_distribution<std::mt19937::result_type> dist_b(100, 200);
    int filter_degree, map1_degree, map2_degree, map3_degree, kw_degree;
    size_t source_degree, sink_degree;
    long last_result = 0;
    source_degree = 1; // dist_p(rng);
    filter_degree = dist_p(rng);
    map1_degree = dist_p(rng);
    map2_degree = dist_p(rng);
    map3_degree = dist_p(rng);
    kw_degree = dist_p(rng);
    sink_degree = dist_p(rng);
    std::cout << "+-------------------------------------------------------------------------+" << std::endl;
    std::cout << "|  +-----+     +-----+     +-----+     +-----+     +-------+     +-----+  |" << std::endl;
    std::cout << "|  |  S  |     |  F  |     |  M  |     |  M  |     | KW_TB |     |  S  |  |" << std::endl;
    std::cout << "|  | CPU +---->+ CPU +---->+ GPU +---->+ GPU +---->+  CPU  +---->+ CPU |  |" << std::endl;
    std::cout << "|  | (" << source_degree << ") |     | (" << filter_degree << ") |     | (" << map1_degree << ") |     | (" << map2_degree << ") |     |  (" << kw_degree << ")  |     | (" << sink_degree << ") |  |" << std::endl;
    std::cout << "|  +-----+     +-----+     +-----+     +-----+     +-------+     +-----+  |" << std::endl;
    std::cout << "+-------------------------------------------------------------------------+" << std::endl;
    // prepare the test
    PipeGraph graph("test_tracing_4", Execution_Mode_t::DEFAULT, Time_Policy_t::EVENT_TIME);
    Source_Positive_Functor source_functor(stream_len, n_keys, true);
    Source source = Source_Builder(source_functor)
                        .withName("source")
                        .withParallelism(source_degree)
                        .withOutputBatchSize(dist_b(rng))
                        .build();
    MultiPipe &mp = graph.add_source(source);
    Filter_Functor_KB filter_functor;
    Filter filter = Filter_Builder(filter_functor)
                        .withName("filter")
                        .withParallelism(filter_degree)
                        .withKeyBy([](const tuple_t &t) -> size_t { return t.key; })
                        .withOutputBatchSize(dist_b(rng))
                        .build();
    mp.chain(filter);
    Map_Functor_GPU map_functor1;
    Map_GPU mapgpu1 = MapGPU_Builder(map_functor1)
                        .withName("mapgpu1")
                        .withParallelism(map1_degree)
                        .build();
    mp.chain(mapgpu1);
    Map_Functor_GPU_KB map_functor_gpu2;
    Map_GPU mapgpu2 = MapGPU_Builder(map_functor_gpu2)
                            .withName("mapgpu2")
                            .withParallelism(map2_degree)
                            .withKeyBy([] __host__ __device__ (const tuple_t &t) -> size_t { return t.key; })
                            .build();
    mp.chain(mapgpu2);
    Win_Functor win_functor;
    Keyed_Windows kwins = Keyed_Windows_Builder(win_functor)
                            .withName("keyed_wins")
                            .withParallelism(kw_degree)
                            .withKeyBy([](const tuple_t &t) -> size_t { return t.key; })
                            .withTBWindows(microseconds(win_len), microseconds(win_slide))
                            .withOutputBatchSize(dist_b(rng))
                            .build();
    mp.add(kwins);
    Sink_Functor2 sink_functor;
    Sink sink = Sink_Builder(sink_functor)
                    .withName("sink")
                    .withParallelism(sink_degree)
                    .build();
    mp.chain_sink(sink);
    // run the application
    graph.run();
    return 0;
}
