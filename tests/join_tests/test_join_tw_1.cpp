/**************************************************************************************
 *  Copyright (c) 2023- Gabriele Mencagli and Yuriy Rymarchuk
 *  
 *  This file is part of WindFlow.
 *  
 *  WindFlow is free software dual licensed under the GNU LGPL or MIT License.
 *  You can redistribute it and/or modify it under the terms of the
 *    * GNU Lesser General Public License as published by
 *      the Free Software Foundation, either version 3 of the License, or
 *      (at your option) any later version
 *    OR
 *    * MIT License: https://github.com/ParaGroup/WindFlow/blob/master/LICENSE.MIT
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
 *  Test 1 of the Interval Join operator.
 *  
 *  +---------------------+                                   +-----------+
 *  |  +-----+   +-----+  |                                   |  +-----+  |
 *  |  |  S  |   |  M  |  |                                   |  |  S  |  |
 *  |  | (*) +-->+ (*) |  +--+   +---------------------+  +-->+  | (*) |  |
 *  |  +-----+   +-----+  |  |   |  +-----+   +-----+  |  |   |  +-----+  |
 *  +---------------------+  |   |  |  J  |   |  F  |  |  |   +-----------+
 *                           +-->|  | (*) +-->| (*) |  +--+
 *  +---------------------+  |   |  +-----+   +-----+  |  |   +-----------+
 *  |  +-----+   +-----+  |  |   +---------------------+  |   |  +-----+  |
 *  |  |  S  |   |  M  |  |  |                            |   |  |  S  |  |
 *  |  | (*) +-->+ (*) |  +--+                            +-->+  | (*) |  |
 *  |  +-----+   +-----+  |                                   |  +-----+  |
 *  +---------------------+                                   +-----------+
 */ 

// include
#include<random>
#include<iostream>
#include<windflow.hpp>
#include"join_common.hpp"

using namespace std;
using namespace chrono;
using namespace wf;

// global variable for the result
extern atomic<long> global_sum;


// struct key descriptor
struct Key_d
{
    size_t key;
    double prob = 0;
};

// struct representing a Joiner (i.e., Interval_Join replica)
struct Joiner
{
    std::vector<size_t> assigned_keys;
    double load = 0.0;
};

// function to check the actual balanceness
double balancing(const std::vector<Joiner> &joiners)
{
    double totalLoad = 0.0;
    double maxLoad = 0.0;
    for (const auto &j: joiners) {
        totalLoad += j.load;
        if (j.load > maxLoad) {
            maxLoad = j.load;
        }
    }
    double averageLoad = totalLoad / joiners.size();
    return (maxLoad / averageLoad);
}

// function to check that all joiners have a positive load
bool allLoaded(const std::vector<Joiner> &joiners)
{
    bool loaded = true;
    for (auto &j: joiners) {
        if (j.load == 0) {
            loaded = false;
        }
    }
    return loaded;
}

// check that for all keys the max splitting degree has been reached
bool allMaxSplit(std::vector<int> splits,
                 int max_splitting)
{
    for (auto &d: splits) {
        if (d < max_splitting) {
            return false;
        }
    }
    return true;
}

// function to print the assignment
void printAssignment(std::vector<Joiner> &joiners)
{
    std::cout << "Assignment Joiners -> Keys" << std::endl;
    int idx = 0;
    for (auto &j: joiners) {
        std::cout << "Joiner " << idx++ << " has load " << j.load << ", keys { ";
        for (auto k: j.assigned_keys) {
            std::cout << k << ", ";
        }
        std::cout << "}" << std::endl;
    }
}

// compute average splitting degree
double averageSplitting(std::vector<int> splits)
{
    double res = 0;
    for (auto &s: splits) {
        res += s;
    }
    return (res / splits.size());
}

// extract the key ID as an integer
int extractKeyID(const std::string &input)
{
    std::string prefix = "key_";
    size_t pos = input.find(prefix);
    if (pos != std::string::npos) {
        std::string numberStr = input.substr(pos + prefix.length());
        return std::stoi(numberStr);
    }
    else {
        return -1;
    }
}

// function to assign keys to joiners
void assignKeys(int numJoiners,
                double threshold,
                std::vector<double> &probs,
                std::unordered_map<size_t, std::vector<int>> &keyToJoiners,
                std::vector<Joiner> &joiners,
                size_t max_splitting,
                int num_keys)
{
    assert(probs.size() == num_keys); // sanity check
    assert(joiners.size() == numJoiners); // sanity check
    std::vector<int> splitDegrees;
    splitDegrees.assign(num_keys, 0);
    std::vector<Key_d> sortedKeys;
    for (int i=1; i<=num_keys; i++) {
        sortedKeys.push_back({(size_t)i, probs[i]});
    }
    // sort keys in descending order or probabilities
    std::sort(sortedKeys.begin(), sortedKeys.end(), [](const Key_d k1, const Key_d k2) {
        return k1.prob > k2.prob;
    });
    // FIRST PASS: assign each key to a single Joiner
    for (const auto &key_d: sortedKeys) {
        // find the Joiner with the minimum load
        auto minJoiner = std::min_element(joiners.begin(), joiners.end(), [](Joiner j1, Joiner j2) {
            return j1.load < j2.load;
        });
        // assign the key to this joiner
        (minJoiner->assigned_keys).push_back(key_d.key);
        // update the load of this joiner
        minJoiner->load += key_d.prob;
        // update the splitting degree of the key
        splitDegrees[key_d.key] = 1;
        // update the key to joiner map
        keyToJoiners[key_d.key].push_back(minJoiner - joiners.begin());
    }

    if (num_keys == 1) {
        return;
    }
    // Check if the load is sufficiently balanced
    if ((balancing(joiners) <= threshold) && allLoaded(joiners)) {
        std::cout << "STOP FIRST PASS -> final balancing is: " << balancing(joiners) << ", threshold was: " << threshold << ", average splitting degree: " << averageSplitting(splitDegrees) << std::endl;
        return;
    }
    // SECOND PASS: reassign keys to multiple joiners if needed
    bool balanced = false;
    while (!balanced) {
        for (const auto &key_d: sortedKeys) {
            if (splitDegrees[key_d.key] < max_splitting) {
                // find the joiner with the minimum load that does not already have this key
                auto minJoiner = std::min_element(joiners.begin(), joiners.end(), [&key_d, &keyToJoiners, &joiners](const Joiner &j1, const Joiner &j2) {
                    auto &vec = keyToJoiners[key_d.key];
                    bool j1HasKey = std::find(vec.begin(), vec.end(), &j1 - &joiners[0]) != vec.end();
                    bool j2HasKey = std::find(vec.begin(), vec.end(), &j2 - &joiners[0]) != vec.end();
                    if (j1HasKey && !j2HasKey) return false;
                    if (!j1HasKey && j2HasKey) return true;
                    return j1.load < j2.load;
                });
                // ensure the selected worker does not already have the key
                if (std::find(keyToJoiners[key_d.key].begin(), keyToJoiners[key_d.key].end(), minJoiner - joiners.begin()) == keyToJoiners[key_d.key].end()) {
                    for (auto &j: joiners) {
                        if (std::find((j.assigned_keys).begin(), (j.assigned_keys).end(), key_d.key) != (j.assigned_keys).end()) {
                            j.load = j.load - (key_d.prob / splitDegrees[key_d.key]) + (key_d.prob / (splitDegrees[key_d.key] + 1));
                        }
                    }
                    // assign the key to this joiner
                    (minJoiner->assigned_keys).push_back(key_d.key);
                    // update the load of this joiner
                    minJoiner->load += key_d.prob / (splitDegrees[key_d.key] + 1);
                    // update the splitting degree of the key
                    splitDegrees[key_d.key]++;
                    // update the key to joiner map
                    keyToJoiners[key_d.key].push_back(minJoiner - joiners.begin());
                    if ((balancing(joiners) <= threshold) && allLoaded(joiners)) {
                        balanced = true;
                        break;
                    }
                }
                else {
                    abort(); // max_splitting has been reached, absurd!
                }
            }
        }
        // check if the load is now sufficiently balanced
        if (((balancing(joiners) <= threshold) && allLoaded(joiners)) || allMaxSplit(splitDegrees, max_splitting)) {
            balanced = true;
        }
    }
    for (Joiner &j: joiners) {
        std::sort((j.assigned_keys).begin(), (j.assigned_keys).end(), [](size_t k1, size_t k2) {
            return k1 < k2;
        });
    }
    std::cout << "STOP SECOND PASS -> final balancing is: " << balancing(joiners) << ", threshold was: " << threshold << ", average splitting degree: " << averageSplitting(splitDegrees) << std::endl;
    return;
}

// main
int main(int argc, char *argv[])
{
    int option = 0;
    size_t runs = 1;
    size_t stream_len = 0;
    size_t n_keys = 1;
    int64_t hybrid_deg = 0;
    uint64_t win_len = 0;
    // initalize global variable
    global_sum = 0;
    // arguments from command line
    if (argc != 11) {
        cout << argv[0] << " -r [runs] -l [stream_length] -k [n_keys] -h [hybrid_degree] -W [window length in msec]" << endl;
        exit(EXIT_SUCCESS);
    }
    while ((option = getopt(argc, argv, "r:l:k:h:W:")) != -1) {
        switch (option) {
            case 'r': runs = atoi(optarg);
                     break;
            case 'l': stream_len = atoi(optarg);
                     break;
            case 'k': n_keys = atoi(optarg);
                     break;
            case 'h': hybrid_deg = atoi(optarg);
                    break;
            case 'W': win_len = atoi(optarg);
                    break;
            default: {
                cout << argv[0] << " -r [runs] -l [stream_length] -k [n_keys] -h [hybrid_degree] -W [window length in msec]" << endl;
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
    std::uniform_int_distribution<std::mt19937::result_type> dist_b(0, 10);
    int map1_degree, map2_degree, join_degree, filter_degree, sink1_degree, sink2_degree;
    size_t source1_degree = 4; //dist_p(rng);
    size_t source2_degree = 2; //dist_p(rng);
    long last_result = 0;

    /* std::vector<double> probs(n_keys, 1.0/(n_keys));
    std::unordered_map<size_t, std::vector<int>> keyToJoiners;
    std::vector<Joiner> joiners(join_degree);
    assignKeys(join_degree, 1.2, probs, keyToJoiners, joiners, join_degree, n_keys);
    printAssignment(joiners); */

    // executes the runs in DEFAULT mode
    for (size_t i=0; i<runs; i++) {
        map1_degree = dist_p(rng);
        map2_degree = dist_p(rng);
        join_degree = 7; //dist_p(rng);
        filter_degree = dist_p(rng);
        sink1_degree = dist_p(rng);
        sink2_degree = dist_p(rng);
        cout << "Run " << i << endl;
        cout << "+---------------------+                                   +-----------+" << endl;
        cout << "|  +-----+   +-----+  |                                   |  +-----+  |" << endl;
        cout << "|  |  S  |   |  M  |  |                                   |  |  S  |  |" << endl;
        cout << "|  | (" << source1_degree << ") +-->+ (" << map1_degree << ") |  +--+   +---------------------+  +-->+  | (" << sink1_degree << ") |  |" << endl;
        cout << "|  +-----+   +-----+  |  |   |  +-----+   +-----+  |  |   |  +-----+  |" << endl;
        cout << "+---------------------+  |   |  |  J  |   |  F  |  |  |   +-----------+" << endl;
        cout << "                         +-->+  | (" << join_degree << ") +-->| (" << filter_degree << ") |  +--+" << endl;
        cout << "+---------------------+  |   |  +-----+   +-----+  |  |   +-----------+" << endl;
        cout << "|  +-----+   +-----+  |  |   +---------------------+  |   |  +-----+  |" << endl;
        cout << "|  |  S  |   |  M  |  |  |                            |   |  |  S  |  |" << endl;
        cout << "|  | (" << source2_degree << ") +-->+ (" << map2_degree << ") |  +--+                            +-->+  | (" << sink2_degree << ") |  |" << endl;
        cout << "|  +-----+   +-----+  |                                   |  +-----+  |" << endl;
        cout << "+---------------------+                                   +-----------+" << endl;
        // compute the total parallelism degree of the PipeGraph
        size_t check_degree = source1_degree;
        if (source1_degree != map1_degree) {
            check_degree += map1_degree;
        }
        check_degree += source2_degree;
        if (source2_degree != map2_degree) {
            check_degree += map2_degree;
        }
        check_degree += join_degree;
        if (join_degree != filter_degree) {
            check_degree += filter_degree;
        }
        check_degree += (sink1_degree + sink2_degree);
        // prepare the test
        PipeGraph graph("test_join_tw_1 (DEFAULT)", Execution_Mode_t::DEFAULT, Time_Policy_t::EVENT_TIME);
        // prepare the first MultiPipe
        Source_Positive_Functor source_functor_positive(stream_len, n_keys, true);
        Source source1 = Source_Builder(source_functor_positive)
                            .withName("source1")
                            .withParallelism(source1_degree)
                            .withOutputBatchSize(dist_b(rng))
                            .build();
        MultiPipe &pipe1 = graph.add_source(source1);
        Map_Functor map_functor1;
        Map map1 = Map_Builder(map_functor1)
                        .withName("map1")
                        .withParallelism(map1_degree)
                        .withOutputBatchSize(dist_b(rng))
                        .build();
        pipe1.chain(map1);
        // prepare the second MultiPipe
        Source_Positive_Functor source_functor_negative(stream_len, n_keys, true);
        Source source2 = Source_Builder(source_functor_negative)
                            .withName("source2")
                            .withParallelism(source2_degree)
                            .withOutputBatchSize(dist_b(rng))
                            .build();
        MultiPipe &pipe2 = graph.add_source(source2);
        Map_Functor map_functor2;
        Map map2 = Map_Builder(map_functor2)
                        .withName("map2")
                        .withParallelism(map2_degree)
                        .withOutputBatchSize(dist_b(rng))
                        .build();
        pipe2.chain(map2);
        // prepare the third MultiPipe
        MultiPipe &pipe3 = pipe1.merge(pipe2);
        Join_Functor join_functor;

        Window_Join join = Window_Join_Builder(join_functor)
                                    .withName("join")
                                    .withParallelism(join_degree)
                                    .withOutputBatchSize(dist_b(rng))
                                    .withKeyBy([](const tuple_t &t) -> size_t { return t.key; })
                                    .withTumblingWindows(milliseconds(win_len))//, milliseconds(347))
                                    //.withDPMode()
                                    .withHPMode(hybrid_deg)
                                    .build();
        pipe3.add(join);
        Filter_Functor filter_functor(2);
        Filter filter = Filter_Builder(filter_functor)
                        .withName("filter1")
                        .withParallelism(filter_degree)
                        .withOutputBatchSize(dist_b(rng))
                        .build();
        pipe3.chain(filter);
        // split
        pipe3.split([](const tuple_t &t) {
            if (t.value % 4 == 0) {
                return 0;
            }
            else {
                return 1;
            }
        }, 2);
        // prepare the fourth MultiPipe
        MultiPipe &pipe4 = pipe3.select(0);
        Sink_Functor sink_functor1;
        Sink sink1 = Sink_Builder(sink_functor1)
                        .withName("sink1")
                        .withParallelism(sink1_degree)
                        .build();
        pipe4.chain_sink(sink1);
        // prepare the fifth MultiPipe
        MultiPipe &pipe5 = pipe3.select(1);
        Sink_Functor sink_functor2;
        Sink sink2 = Sink_Builder(sink_functor2)
                        .withName("sink2")
                        .withParallelism(sink2_degree)
                        .build();
        pipe5.chain_sink(sink2);
        assert(graph.getNumThreads() == check_degree);

        // run the application
        graph.run();
        if (i == 0) {
            last_result = global_sum;
            cout << "Result is --> " << GREEN << "OK" << DEFAULT_COLOR << " value " << global_sum.load() << endl;
        }
        else {
            if (last_result == global_sum) {
                cout << "Result is --> " << GREEN << "OK" << DEFAULT_COLOR << " value " << global_sum.load() << endl;
            }
            else {
                cout << "Result is --> " << RED << "FAILED" << DEFAULT_COLOR << " value " << global_sum.load() << endl;
                abort();
            }
        }
        global_sum = 0;
    }
    // executes the runs in DETERMINISTIC mode
    for (size_t i=0; i<0; i++) {
        map1_degree = dist_p(rng);
        map2_degree = dist_p(rng);
        join_degree = 7; //dist_p(rng);
        filter_degree = dist_p(rng);
        sink1_degree = dist_p(rng);
        sink2_degree = dist_p(rng);
        cout << "Run " << i << endl;
        cout << "+---------------------+                                   +-----------+" << endl;
        cout << "|  +-----+   +-----+  |                                   |  +-----+  |" << endl;
        cout << "|  |  S  |   |  M  |  |                                   |  |  S  |  |" << endl;
        cout << "|  | (" << source1_degree << ") +-->+ (" << map1_degree << ") |  +--+   +---------------------+  +-->+  | (" << sink1_degree << ") |  |" << endl;
        cout << "|  +-----+   +-----+  |  |   |  +-----+   +-----+  |  |   |  +-----+  |" << endl;
        cout << "+---------------------+  |   |  |  J  |   |  F  |  |  |   +-----------+" << endl;
        cout << "                         +-->+  | (" << join_degree << ") +-->| (" << filter_degree << ") |  +--+" << endl;
        cout << "+---------------------+  |   |  +-----+   +-----+  |  |   +-----------+" << endl;
        cout << "|  +-----+   +-----+  |  |   +---------------------+  |   |  +-----+  |" << endl;
        cout << "|  |  S  |   |  M  |  |  |                            |   |  |  S  |  |" << endl;
        cout << "|  | (" << source2_degree << ") +-->+ (" << map2_degree << ") |  +--+                            +-->+  | (" << sink2_degree << ") |  |" << endl;
        cout << "|  +-----+   +-----+  |                                   |  +-----+  |" << endl;
        cout << "+---------------------+                                   +-----------+" << endl;
        // compute the total parallelism degree of the PipeGraph
        size_t check_degree = source1_degree;
        if (source1_degree != map1_degree) {
            check_degree += map1_degree;
        }
        check_degree += source2_degree;
        if (source2_degree != map2_degree) {
            check_degree += map2_degree;
        }
        check_degree += join_degree;
        if (join_degree != filter_degree) {
            check_degree += filter_degree;
        }
        check_degree += (sink1_degree + sink2_degree);
        // prepare the test
        PipeGraph graph("test_join_tw_1 (DETERMINISTIC)", Execution_Mode_t::DETERMINISTIC, Time_Policy_t::EVENT_TIME);
        // prepare the first MultiPipe
        Source_Positive_Functor source_functor_positive(stream_len, n_keys, false);
        Source source1 = Source_Builder(source_functor_positive)
                            .withName("source1")
                            .withParallelism(source1_degree)
                            .build();
        MultiPipe &pipe1 = graph.add_source(source1);
        Map_Functor map_functor1;
        Map map1 = Map_Builder(map_functor1)
                        .withName("map1")
                        .withParallelism(map1_degree)
                        .build();
        pipe1.chain(map1);
        // prepare the second MultiPipe
        Source_Positive_Functor source_functor_negative(stream_len, n_keys, false);
        Source source2 = Source_Builder(source_functor_negative)
                            .withName("source2")
                            .withParallelism(source2_degree)
                            .build();
        MultiPipe &pipe2 = graph.add_source(source2);
        Map_Functor map_functor2;
        Map map2 = Map_Builder(map_functor2)
                        .withName("map2")
                        .withParallelism(map2_degree)
                        .build();
        pipe2.chain(map2);
        // prepare the third MultiPipe
        MultiPipe &pipe3 = pipe1.merge(pipe2);
        Join_Functor join_functor;
        Window_Join join = Window_Join_Builder(join_functor)
                                    .withName("join")
                                    .withParallelism(join_degree)
                                    .withKeyBy([](const tuple_t &t) -> size_t { return t.key; })
                                    .withTumblingWindows(milliseconds(win_len))
                                    //.withDPMode()
                                    .withHPMode(hybrid_deg)
                                    .build();
        pipe3.add(join);
        Filter_Functor filter_functor(2);
        Filter filter = Filter_Builder(filter_functor)
                        .withName("filter1")
                        .withParallelism(filter_degree)
                        .build();
        pipe3.chain(filter);
        // split
        pipe3.split([](const tuple_t &t) {
            if (t.value % 4 == 0) {
                return 0;
            }
            else {
                return 1;
            }
        }, 2);
        // prepare the fourth MultiPipe
        MultiPipe &pipe4 = pipe3.select(0);
        Sink_Functor sink_functor1;
        Sink sink1 = Sink_Builder(sink_functor1)
                        .withName("sink1")
                        .withParallelism(sink1_degree)
                        .build();
        pipe4.chain_sink(sink1);
        // prepare the fifth MultiPipe
        MultiPipe &pipe5 = pipe3.select(1);
        Sink_Functor sink_functor2;
        Sink sink2 = Sink_Builder(sink_functor2)
                        .withName("sink2")
                        .withParallelism(sink2_degree)
                        .build();
        pipe5.chain_sink(sink2);
        assert(graph.getNumThreads() == check_degree);
        // run the application
        graph.run();
        if (i == 0) {
            last_result = global_sum;
            cout << "Result is --> " << GREEN << "OK" << DEFAULT_COLOR << " value " << global_sum.load() << endl;
        }
        else {
            if (last_result == global_sum) {
                cout << "Result is --> " << GREEN << "OK" << DEFAULT_COLOR << " value " << global_sum.load() << endl;
            }
            else {
                cout << "Result is --> " << RED << "FAILED" << DEFAULT_COLOR << " value " << global_sum.load() << endl;
                abort();
            }
        }
        global_sum = 0;
    }
    return 0;
}
