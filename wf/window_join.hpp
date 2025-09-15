/**************************************************************************************
 *  Copyright (c) 2024- Gabriele Mencagli and Yuriy Rymarchuk
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

/** 
 *  @file    window_join.hpp
 *  @author  Gabriele Mencagli and Yuriy Rymarchuk
 *  
 *  @brief Window Join operator
 *  
 *  @section Window Join (Description)
 *  
 *  This file implements the Window Join operator able to execute joins over two streams of tuples
 *  producing x output per input, where x is the number of asserted predicates that lies in the same temporal window
 */ 

#ifndef WINDOW_JOIN_H
#define WINDOW_JOIN_H

/// includes
#include<string>
#include<cstdint>
#include<iomanip>
#include<functional>
#include<context.hpp>
#include<batch_t.hpp>
#include<single_t.hpp>
#if defined (WF_TRACING_ENABLED)
    #include<stats_record.hpp>
#endif
#include<iterable.hpp>
#include<join_archive.hpp>
#include<window_structure.hpp>
#include<basic_emitter.hpp>
#include<basic_operator.hpp>

namespace wf {

//@cond DOXY_IGNORE

// class WJoin_Replica
template<typename join_func_t, typename keyextr_func_t>
class WJoin_Replica: public Basic_Replica
{
private:
    template<typename T1, typename T2> friend class Window_Join;
    join_func_t func; // functional logic used by the Interval Join replica
    keyextr_func_t key_extr; // logic to extract the key attribute from the tuple_t
    using tuple_t = decltype(get_tuple_t_Join(func)); // extracting the tuple_t type and checking the admissible signatures
    using result_t = decltype(get_result_t_Join(func)); // extracting the result_t type and checking the admissible signatures
    using key_t = decltype(get_key_t_KeyExtr(key_extr)); // extracting the key_t type and checking the admissible singatures
    // static predicates to check the type of the functional logic to be invoked
    static constexpr bool isNonRiched = std::is_invocable<decltype(func), const tuple_t &, const tuple_t &>::value;
    static constexpr bool isRiched = std::is_invocable<decltype(func), const tuple_t &, const tuple_t &, RuntimeContext &>::value;
    // check the presence of a valid functional logic
    static_assert(isNonRiched || isRiched,
        "WindFlow Compilation Error - WJoin_Replica does not have a valid functional logic:\n");
    using wrapper_t = wrapper_tuple_t<tuple_t>; // alias for the wrapped tuple type
    using container_t = typename std::deque<wrapper_t>; // container type for underlying archive's buffer structure
    using iterator_t = typename container_t::iterator; // iterator type for accessing wrapped tuples in the archive
    using win_t = JoinWindow<key_t>; // window type used by the Window_Replica
    using compare_func_t = std::function<bool(const wrapper_t &, const uint64_t &)>; // function type to compare wrapped tuple to an uint64

    template<typename tuple_t, typename compare_func_t>
    struct Key_Descriptor // struct of a key descriptor
    {
        JoinArchive<tuple_t, compare_func_t> archiveA; // archive of stream A tuples of this key
        JoinArchive<tuple_t, compare_func_t> archiveB; // archive of stream B tuples of this key
        std::vector<win_t> wins; // open windows of this key
        uint64_t next_lwid; // next window to be opened of this key (lwid)
        int64_t last_lwid; // last window closed of this key (lwid)
        uint64_t partitioning_counter; // counter used in DP/HP mode to establish which replica will save the given tuple

        std::vector<int> assigned_replicas; // vector of replicas assigned to this key (used for window trigger initialization)

        // Constructor
        Key_Descriptor(compare_func_t _compare_func):
                       archiveA(_compare_func),
                       archiveB(_compare_func),
                       next_lwid(0),
                       last_lwid(-1),
                       partitioning_counter(0) {
                            wins.reserve(WF_DEFAULT_VECTOR_CAPACITY);
                       }
    };
    using key_d_t = Key_Descriptor<tuple_t, compare_func_t>; // key descriptor type

    compare_func_t compare_func; // function to compare wrapper to an uint64 that rapresents a timestamp or a watermark
    size_t ignored_tuples; // number of ignored tuples

    uint64_t win_len; // window size expressed in time unit
    uint64_t slide_len; // sliding length expressed in time unit

    Join_Window_t join_win_type; // type of the window join
    Join_Mode_t join_mode; // Interval Join operating mode
    std::unordered_map<key_t, key_d_t> keyMap; // hash table that maps a descriptor for each key
    uint64_t last_wm; // last received watermark or timestamp
    size_t id_inner; // id_inner value
    size_t num_inner; // num_inner value
    size_t num_replicas; // number of replicas of the operator used for window assignment
    
    size_t hybrid_degree; // hybrid degree of the emitter in case of hybrid parallelism
    std::unordered_map<key_t, std::vector<int>> keyToJoiners; // mapping keys to replicas
    std::unordered_map<key_t, uint64_t> last_wms; // last watermark received for each key, used in Hybrid Parallelism

    // Checks if the given Join_Stream_t is Stream A
    bool isStreamA(Join_Stream_t stream) const
    {
        return stream == Join_Stream_t::A;
    }

    // Inserts a wrapper object into the buffer of a given key descriptor
    void insertIntoBuffer(key_d_t &_key_d,
                          wrapper_t _wt,
                          Join_Stream_t stream)
    {
        isStreamA(stream) ? (_key_d.archiveA).insert(_wt) : (_key_d.archiveB).insert(_wt);
    }

    // Purges the archives of the given key descriptor
    void purgeArchives(key_d_t &_key_d, uint64_t check_point)
    {
        size_t purged_a = (_key_d.archiveA).purge(check_point);
        size_t purged_b = (_key_d.archiveB).purge(check_point);
        //std::cout << "Purged " << purged_a << " tuples from A and " << purged_b << " tuples from B" << std::endl;
    }

    // Purges the keyMap by removing any archived data associated with each key
    void purgeWithPunct()
    {
        for (auto &k: keyMap) {
            key_d_t &key_d = (k.second);
            uint64_t wm = join_mode == Join_Mode_t::HP ? last_wms[k.first] : last_wm;
            purgeArchives(key_d, wm);
        }
    }

public:
    // Constructor
    WJoin_Replica(join_func_t _func,
                  keyextr_func_t _key_extr,
                  std::string _opName,
                  RuntimeContext _context,
                  std::function<void(RuntimeContext &)> _closing_func,
                  uint64_t _win_len,
                  uint64_t _slide_len,
                  Join_Window_t _join_win_type,
                  Join_Mode_t _join_mode,
                  size_t _hybrid_degree,
                  std::unordered_map<key_t, std::vector<int>> _keyToJoiners):
                  Basic_Replica(_opName, _context, _closing_func, false),
                  func(_func),
                  key_extr(_key_extr),
                  win_len(_win_len),
                  slide_len(_slide_len),
                  join_win_type(_join_win_type),
                  join_mode(_join_mode),
                  last_wm(0),
                  ignored_tuples(0),
                  hybrid_degree(_hybrid_degree),
                  keyToJoiners(_keyToJoiners)
    {
        compare_func = [](const wrapper_t &w1, const uint64_t &_idx) { // comparator function of wrapped tuples
            return w1.index < _idx;
        };
        num_inner = _context.getParallelism();
        id_inner = _context.getReplicaIndex();
        if (join_mode == Join_Mode_t::DP) {
            num_replicas = num_inner;
        } else if (join_mode == Join_Mode_t::HP && !keyToJoiners.size()) {
            num_replicas = hybrid_degree;
            assert(num_replicas <= num_inner);
        } else {
            num_replicas = 1;
        }
    }

    // Copy Constructor
    WJoin_Replica(const WJoin_Replica &_other):
                  Basic_Replica(_other),
                  func(_other.func),
                  key_extr(_other.key_extr),
                  compare_func(_other.compare_func),
                  win_len(_other.win_len),
                  slide_len(_other.slide_len),
                  join_mode(_other.join_mode),
                  last_wm(_other.last_wm),
                  ignored_tuples(_other.ignored_tuples),
                  id_inner(_other.id_inner),
                  num_inner(_other.num_inner),
                  hybrid_degree(_other.hybrid_degree),
                  keyToJoiners(_other.keyToJoiners) {}

    // svc (utilized by the FastFlow runtime)
    void *svc(void *_in) override
    {
        this->startStatsRecording();
        if (this->input_batching) { // receiving a batch
            Batch_t<tuple_t> *batch_input = reinterpret_cast<Batch_t<tuple_t> *>(_in);
            if (batch_input->isPunct()) { // if it is a punctuaton
                (this->emitter)->propagate_punctuation(batch_input->getWatermark((this->context).getReplicaIndex()), this); // propagate the received punctuation
                if (join_mode == Join_Mode_t::HP) {
                    key_t key = key_extr(batch_input->getTupleAtPos(0)); // get the key attribute of the punctuation
                    assert(last_wms.find(key) != last_wms.end()); // sanity check
                    assert(last_wms[key] <= batch_input->getWatermark((this->context).getReplicaIndex())); // sanity check
                    last_wms[key] = batch_input->getWatermark((this->context).getReplicaIndex());
                } else {
                    assert(last_wm <= batch_input->getWatermark((this->context).getReplicaIndex())); // sanity check
                    last_wm = batch_input->getWatermark((this->context).getReplicaIndex());
                }
                purgeWithPunct();
                deleteBatch_t(batch_input); // delete the punctuation
                return this->GO_ON;
            }
#if defined (WF_TRACING_ENABLED)
            (this->stats_record).inputs_received += batch_input->getSize();
            (this->stats_record).bytes_received += batch_input->getSize() * sizeof(tuple_t);
#endif
            for (size_t i=0; i<batch_input->getSize(); i++) { // process all the inputs within the received batch
                process_input(batch_input->getTupleAtPos(i), batch_input->getTimestampAtPos(i), batch_input->getWatermark((this->context).getReplicaIndex()), batch_input->getStreamTag());
            }
            deleteBatch_t(batch_input); // delete the input batch
        }
        else { // receiving a single input
            Single_t<tuple_t> *input = reinterpret_cast<Single_t<tuple_t> *>(_in);
            if (input->isPunct()) { // if it is a punctuaton
                (this->emitter)->propagate_punctuation(input->getWatermark((this->context).getReplicaIndex()), this); // propagate the received punctuation
                if (join_mode == Join_Mode_t::HP) {
                    key_t key = key_extr(input->tuple); // get the key attribute of the punctuation
                    assert(last_wms.find(key) != last_wms.end()); // sanity check
                    assert(last_wms[key] <= input->getWatermark((this->context).getReplicaIndex())); // sanity check
                    last_wms[key] = input->getWatermark((this->context).getReplicaIndex());
                } else {
                    assert(last_wm <= input->getWatermark((this->context).getReplicaIndex())); // sanity check
                    last_wm = input->getWatermark((this->context).getReplicaIndex());
                }
                purgeWithPunct();
                deleteSingle_t(input); // delete the punctuation
                return this->GO_ON;
            }
#if defined (WF_TRACING_ENABLED)
            (this->stats_record).inputs_received++;
            (this->stats_record).bytes_received += sizeof(tuple_t);
#endif
            process_input(input->tuple, input->getTimestamp(), input->getWatermark((this->context).getReplicaIndex()), input->getStreamTag());
            deleteSingle_t(input); // delete the input Single_t
        }
        this->endStatsRecording();
        return this->GO_ON;
    }

    // Process a single input
    void process_input(tuple_t &_tuple,
                       uint64_t _timestamp,
                       uint64_t _watermark,
                       Join_Stream_t _tag)
    {
        if (this->execution_mode == Execution_Mode_t::DEFAULT && join_mode != Join_Mode_t::HP && _timestamp < last_wm) { // if the input is out-of-order
#if defined (WF_TRACING_ENABLED)
            stats_record.inputs_ignored++;
#endif
            ignored_tuples++;
            return;
        }
        auto key = key_extr(_tuple); // get the key attribute of the input tuple

        if (this->execution_mode == Execution_Mode_t::DEFAULT && join_mode == Join_Mode_t::HP && _timestamp < last_wms[key]) { // if the input is out-of-order
#if defined (WF_TRACING_ENABLED)
            stats_record.inputs_ignored++;
#endif
            ignored_tuples++;
            return;
        }

        auto it = keyMap.find(key);
        if (it == keyMap.end()) {
            auto p = keyMap.insert(std::make_pair(key, key_d_t(compare_func)));
            it = p.first;
            last_wms[key] = 0;

            std::vector<int> assigned_replicas;
            if (join_mode == Join_Mode_t::HP) {
                if (keyToJoiners.count(key)) {
                    assigned_replicas = keyToJoiners[key];
                } else {
                    size_t hashcode = std::hash<key_t>()(key);
                    for (size_t i = 0; i < hybrid_degree; i++) {
                        assigned_replicas.push_back((hashcode + i) % num_inner);
                    }
                }
            } else if (join_mode == Join_Mode_t::DP) {
                for (size_t i = 0; i < num_inner; i++) {
                    assigned_replicas.push_back(i);  // Contiguous: [0, 1, 2, ..., num_inner-1]
                }
            } else {
                assigned_replicas.push_back(id_inner);
            }
            (it->second).assigned_replicas = assigned_replicas;

        }
        key_d_t &key_d = (*it).second;
        
        bool should_store_tuple = false;
        uint64_t ts = _timestamp; // the timestamp of the current tuple
        if (join_mode == Join_Mode_t::HP && keyToJoiners.size()) {
            num_replicas = keyToJoiners[key].size();
        }
        
        uint64_t min_boundary = (key_d.last_lwid >= 0) ? win_len + (key_d.last_lwid  * slide_len) : 0;
        if (ts < min_boundary) { // if the tuple is related to a closed window -> IGNORED
            if (key_d.last_lwid >= 0) {
#if defined (WF_TRACING_ENABLED)
                stats_record.inputs_ignored++;
#endif
                ignored_tuples++;
            }
            return;
        }

        long last_w = -1; // determine the lwid of the last window containing t
        if (win_len >= slide_len) { // sliding or tumbling windows
            last_w = ceil(((double) ts + 1)/((double) slide_len)) - 1;
        }
        else { // hopping windows
            uint64_t n = floor((double) (ts) / slide_len);
            last_w = n;
        }

        std::vector<win_t> &wins = key_d.wins; // reference to the open windows of the key id_inner, key_d.assigned_replicas
        for (long lwid = key_d.next_lwid; lwid <= last_w; lwid++) { // create all the new opened windows
            uint64_t gwid = (lwid * num_replicas); // translate lwid -> gwid
            // Calculate the actual window length for this specific window
            uint64_t actual_win_len;
            if (win_len >= slide_len) {
                // For the first few windows, use growing window size
                uint64_t full_windows_threshold = win_len / slide_len; // Number of slides to reach full window size
                if (lwid < full_windows_threshold) {
                    // Growing window: window i has length (i+1) * slide_len
                    actual_win_len = (lwid + 1) * slide_len;
                } else {
                    // Full-size window
                    actual_win_len = win_len;
                }
            } else {
                // Hopping windows - use full window length
                actual_win_len = win_len;
            }
            wins.push_back(win_t(key, lwid, gwid, actual_win_len, slide_len, Win_Type_t::TB, id_inner, key_d.assigned_replicas, Triggerer_Join_TB(actual_win_len, slide_len, lwid)));
            key_d.next_lwid++;
        }
        
        size_t cnt_fired = 0;
        uint64_t purge_wm = 0;
        uint64_t emit_ts, emit_wm;
        std::optional<result_t> output;

        for (win_t &win: wins) { // evaluate all the open windows of the key
            win_event_t event = win.onJoinTuple(ts, _tag); // get the event
            if (event == win_event_t::IN) { // window is not fired
                auto bound_pair = win.getPartitionBounds();
                // In DP check if the current tuple is in the time partition of the replica
                if (join_mode == Join_Mode_t::DP || join_mode == Join_Mode_t::HP) {
                    if ( bound_pair.first <= ts && ts < bound_pair.second ) {
                        should_store_tuple = true;
                    }
                }
                if (isStreamA(_tag)) {
                    std::pair<iterator_t, iterator_t> its;
                    its = (key_d.archiveB).getJoinRange(bound_pair.first, bound_pair.second);
                    Iterable<tuple_t> iter_b(its.first, its.second);
                    /* std::cout << "Window event IN: " << win.getLWID() << " with win num_tuples: " << win.getSize() <<
                        " and win ts: " << win.getResultTimestamp() << " and wm: " << last_wm <<
                        " with B size: " << iter_b.size() << " from replica: " << id_inner << " tuple ts: " << ts << std::endl; */
                    for (auto &t_b: iter_b) { // iterate over the tuples in the archive of stream B
                        if constexpr (isNonRiched) {
                            output = func(_tuple, t_b);
                        }
                        if constexpr (isRiched)  { // inplace riched version
                            (this->context).setContextParameters(ts, _watermark);
                            output = func(_tuple, t_b, this->context);
                        }
                        if (output) {
                            emit_ts = win.getResultTimestamp();
                            if (join_mode == Join_Mode_t::HP) {
                                emit_wm = std::min_element(last_wms.begin(), last_wms.end(), [](const auto &p1, const auto &p2) {
                                    return p1.second < p2.second;
                                })->second;
                            } else {
                                emit_wm = _watermark;
                            }
                            this->doEmit(this->emitter, &(*output), 0, emit_ts, emit_wm, this); // emit the pair
#if defined (WF_TRACING_ENABLED)
                            (this->stats_record).outputs_sent++;
                            (this->stats_record).bytes_sent += sizeof(result_t);
#endif
                        }
                    }
                } else {
                    std::pair<iterator_t, iterator_t> its;
                    its = (key_d.archiveA).getJoinRange(bound_pair.first, bound_pair.second);
                    Iterable<tuple_t> iter_a(its.first, its.second);
                    /* std::cout << "Window event IN: " << win.getLWID() << " with win num_tuples: " << win.getSize() <<
                        " and win ts: " << win.getResultTimestamp() << " and wm: " << last_wm <<
                        " with A size: " << iter_a.size() << " from replica: " << id_inner << " tuple ts: " << ts << std::endl; */
                    for (auto &t_a: iter_a) { // iterate over the tuples in the archive of stream A
                        if constexpr (isNonRiched) {
                            output = func(t_a, _tuple);
                        }
                         if constexpr (isRiched)  { // inplace riched version
                            (this->context).setContextParameters(ts, _watermark);
                            output = func(t_a, _tuple, this->context);
                        }
                        if (output) {
                            emit_ts = win.getResultTimestamp();
                            if (join_mode == Join_Mode_t::HP) {
                                emit_wm = std::min_element(last_wms.begin(), last_wms.end(), [](const auto &p1, const auto &p2) {
                                    return p1.second < p2.second;
                                })->second;
                            } else {
                                emit_wm = _watermark;
                            }
                            this->doEmit(this->emitter, &(*output), 0, emit_ts, emit_wm, this); // emit the pair
#if defined (WF_TRACING_ENABLED)
                            (this->stats_record).outputs_sent++;
                            (this->stats_record).bytes_sent += sizeof(result_t);
#endif
                        }
                    }

                }
            }
            else if (event == win_event_t::FIRED) { // window is fired
                // TODO: Second condition could involve lateness
                if ((join_mode == Join_Mode_t::HP && this->execution_mode == Execution_Mode_t::DEFAULT && win.getResultTimestamp() <= last_wms[key]) ||
                    (join_mode != Join_Mode_t::HP && win.getResultTimestamp() <= last_wm)) {
                    cnt_fired++;
                    purge_wm = join_win_type == Join_Window_t::TUMB ? win.getResultTimestamp() : win.getStartTimestamp();
                    key_d.last_lwid++;
                }
            }
        }
        if (cnt_fired) {
            wins.erase(wins.begin(), wins.begin() + cnt_fired); // purge the fired windows
            purgeArchives(key_d, purge_wm); // purge the archives using the watermark of the last fired window
            //std::cout << "Purged " << cnt_fired << " windows from key: " << key << " in replica: " << id_inner << std::endl;
        }

        if (join_mode == Join_Mode_t::KP) { // KP
            should_store_tuple = true;
        }

        if (should_store_tuple) {
            //std::cout << "Storing tuple in DP mode with id_inner: " << id_inner << " stream: " << (_tag == Join_Stream_t::A ? "A" : "B") << std::endl;
            insertIntoBuffer(key_d, wrapper_t(_tuple, _timestamp), _tag);
        }

        if (this->execution_mode == Execution_Mode_t::DEFAULT && join_mode == Join_Mode_t::HP){
            assert(last_wms[key] <= _watermark); // sanity check
            last_wms[key] = _watermark;
        }
        else if (this->execution_mode == Execution_Mode_t::DEFAULT) {
            assert(last_wm <= _watermark); // sanity check
            last_wm = _watermark;
        }
        else {
            if (last_wm < _timestamp)  last_wm = _timestamp;
        }
    }

    // Get the number of ignored tuples
    size_t getNumIgnoredTuples() const
    {
        return ignored_tuples;
    }

    WJoin_Replica(WJoin_Replica &&) = delete; ///< Move constructor is deleted
    WJoin_Replica &operator=(const WJoin_Replica &) = delete; ///< Copy assignment operator is deleted
    WJoin_Replica &operator=(WJoin_Replica &&) = delete; ///< Move assignment operator is deleted
};

//@endcond

/** 
 *  \class Window Join
 *  
 *  \brief Window Join operator
 *  
 *  The Window Join operator performs a join operation over two streams based on a specified window size and sliding length.
 *  It takes a functional Boolean condition logic and a key extractor logic as input. The operator operates in
 *  either Key-Parallelism (KP) or Data-Parallelism (DP) or Hybrid-Parallelism mode.
 */ 
template<typename join_func_t, typename keyextr_func_t>
class Window_Join: public Basic_Operator
{
private:
    friend class MultiPipe;
    friend class PipeGraph;
    join_func_t func; // functional boolean condition logic used by the Window Join
    keyextr_func_t key_extr; // logic to extract the key attribute from the tuple_t
    std::vector<WJoin_Replica<join_func_t, keyextr_func_t>*> replicas; // vector of pointers to the replicas of the Window Join

    uint64_t win_size; // window size expressed in time unit
    uint64_t slide_len; // sliding length expressed in time unit

    Join_Window_t join_win_type; // type of the join windows
    Join_Mode_t join_mode; // Window Join operating mode
    using tuple_t = decltype(get_tuple_t_Join(func)); // extracting the tuple_t type and checking the admissible signatures
    using result_t = decltype(get_result_t_Join(func)); // extracting the result_t type and checking the admissible signatures
    using key_t = decltype(get_key_t_KeyExtr(key_extr)); // extracting the key_t type and checking the admissible singatures
    static constexpr op_type_t op_type = op_type_t::BASIC;
    
    size_t hybrid_parallelism; // parallelism of the hybrid partitioning mode
    std::unordered_map<key_t, std::vector<int>> keyToJoiners; // mapping keys to replicas

    // Configure the Window Join to receive batches instead of individual inputs
    void receiveBatches(bool _input_batching) override
    {
        for (auto *r: replicas) {
            r->receiveBatches(_input_batching);
        }
    }

    // Set the emitter used to route outputs from the Window Join
    void setEmitter(Basic_Emitter *_emitter) override
    {
        replicas[0]->setEmitter(_emitter);
        for (size_t i=1; i<replicas.size(); i++) {
            replicas[i]->setEmitter(_emitter->clone());
        }
    }

    // Check whether the Window Join has terminated
    bool isTerminated() const override
    {
        bool terminated = true;
        for(auto *r: replicas) { // scan all the replicas to check their termination
            terminated = terminated && r->isTerminated();
        }
        return terminated;
    }

    // Set the execution mode of the Window Join
    void setExecutionMode(Execution_Mode_t _execution_mode)
    {
        if (this->getOutputBatchSize() > 0 && _execution_mode != Execution_Mode_t::DEFAULT) {
            std::cerr << RED << "WindFlow Error: Window Join is trying to produce a batch in non DEFAULT mode" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        for (auto *r: replicas) {
            r->setExecutionMode(_execution_mode);
        }
    }

    // Get the logic to extract the key attribute from the tuple_t
    keyextr_func_t getKeyExtractor() const
    {
        return key_extr;
    }

    // Get the hybrid parallelism degree
    size_t getHybridParallelism() const override
    {
        return hybrid_parallelism;
    }

    // Get a pointer to the map between keys and replicas
    void *getKeysToJoiner() const override
    {
        return (void *) &keyToJoiners;
    }

#if defined (WF_TRACING_ENABLED)
    // Append the statistics (JSON format) of the Window Join to a PrettyWriter
    void appendStats(rapidjson::PrettyWriter<rapidjson::StringBuffer> &writer) const override
    {
        writer.StartObject(); // create the header of the JSON file
        writer.Key("Operator_name");
        writer.String((this->name).c_str());
        writer.Key("Operator_type");
        writer.String("Window_Join");
        writer.Key("Distribution");
        if (this->getInputRoutingMode() == Routing_Mode_t::KEYBY) {
            writer.String("KEYBY");
        }
        else if (this->getInputRoutingMode() == Routing_Mode_t::BROADCAST) {
            writer.String("BROADCAST");
        }
        else if (this->getInputRoutingMode() == Routing_Mode_t::HYBRID_JOIN) {
            writer.String("HYBRID_JOIN");
        }
        writer.Key("isTerminated");
        writer.Bool(this->isTerminated());
        writer.Key("isWindowed");
        writer.Bool(false);
        writer.Key("isGPU");
        writer.Bool(false);
        writer.Key("Parallelism");
        writer.Uint(this->parallelism);
        writer.Key("OutputBatchSize");
        writer.Uint(this->outputBatchSize);
        writer.Key("Window_Size");
        writer.Uint(this->win_size);
        writer.Key("Sliding_Length");
        writer.Uint(this->slide_len);
        writer.Key("Join_Mode");
        if (this->join_mode == Join_Mode_t::KP) {
            writer.String("Key-Parallelism");
        }
        else if (this->join_mode == Join_Mode_t::DP) {
            writer.String("Data-Parallelism");
        }
        else if (this->join_mode == Join_Mode_t::HP) {
            writer.String("Hybrid-Parallelism");
        }
        writer.Key("Replicas");
        writer.StartArray();
        for (auto *r: replicas) { // append the statistics from all the replicas of the Map
            Stats_Record record = r->getStatsRecord();
            record.appendStats(writer);
        }
        writer.EndArray();
        writer.EndObject();
    }
#endif

public:
    /** 
     *  \brief Constructor
     *  
     *  \param _func functional Boolean condition logic of the Window Join (a function or any callable type)
     *  \param _key_extr key extractor (a function or any callable type)
     *  \param _parallelism internal parallelism of the Window Join
     *  \param _name name of the Window Join
     *  \param _input_routing_mode input routing mode of the Window Join
     *  \param _outputBatchSize size (in num of tuples) of the batches produced by this operator (0 for no batching)
     *  \param _closing_func closing functional logic of the Window Join (a function or any callable type)
     *  \param _win_size window size expressed in time unit
     *  \param _slide sliding length expressed in time unit
     *  \param _join_mode Window Join operating mode
     *  \param _hybrid_parallelism parallelism of the hybrid partitioning mode
     *  \param _keyToJoiners reference to a mapping between keys and replicas
     */ 
    Window_Join(join_func_t _func,
                  keyextr_func_t _key_extr,
                  size_t _parallelism,
                  std::string _name,
                  Routing_Mode_t _input_routing_mode,
                  size_t _outputBatchSize,
                  std::function<void(RuntimeContext &)> _closing_func,
                  uint64_t _win_size,
                  uint64_t _slide_len,
                  Join_Window_t _join_win_type,
                  Join_Mode_t _join_mode,
                  size_t _hybrid_parallelism,
                  const std::unordered_map<key_t, std::vector<int>> &_keyToJoiners):
                  Basic_Operator(_parallelism, _name, _input_routing_mode, _outputBatchSize),
                  func(_func),
                  key_extr(_key_extr),
                  win_size(_win_size),
                  slide_len(_slide_len),
                  join_win_type(_join_win_type),
                  join_mode(_join_mode),
                  hybrid_parallelism(_hybrid_parallelism),
                  keyToJoiners(_keyToJoiners)
    {
        if (this->join_mode == Join_Mode_t::HP) {
            if (this->hybrid_parallelism > this->parallelism) {
                std::cerr << RED << "WindFlow Error: hybrid parallelism cannot be greater than the parallelism of the Window Join" << DEFAULT_COLOR << std::endl;
                exit(EXIT_FAILURE);
            }
        }
        for (size_t i=0; i<this->parallelism; i++) { // create the internal replicas of the Interval Join
            replicas.push_back(new WJoin_Replica<join_func_t, keyextr_func_t>(this->func,
                                                                              this->key_extr,
                                                                              this->name,
                                                                              RuntimeContext(this->parallelism, i),
                                                                              _closing_func,
                                                                              this->win_size,
                                                                              this->slide_len,
                                                                              this->join_win_type,
                                                                              this->join_mode,
                                                                              this->hybrid_parallelism,
                                                                              this->keyToJoiners));
        }
    }

    /// Copy constructor
    Window_Join(const Window_Join &_other):
                  Basic_Operator(_other),
                  func(_other.func),
                  key_extr(_other.key_extr),
                  win_size(_other.win_size),
                  slide_len(_other.slide_len),
                  join_win_type(_other.join_win_type),
                  join_mode(_other.join_mode),
                  hybrid_parallelism(_other.hybrid_parallelism),
                  keyToJoiners(_other.keyToJoiners)
    {
        for (size_t i=0; i<this->parallelism; i++) { // deep copy of the pointers to the Interval Join replicas
            replicas.push_back(new WJoin_Replica<join_func_t, keyextr_func_t>(*(_other.replicas[i])));
        }
    }

    // Destructor
    ~Window_Join() override
    {
        for (auto *r: replicas) { // delete all the replicas
            delete r;
        }
    }

    /** 
     *  \brief Get the type of the Window Join as a string
     *  \return type of the Window Join
     */ 
    std::string getType() const override
    {
        std::string join_mode_str = "Window_Join_";
        switch (join_mode) {
            case Join_Mode_t::KP:
                join_mode_str += "KP";
                break;
            case Join_Mode_t::DP:
                join_mode_str += "DP";
                break;
            case Join_Mode_t::HP:
                join_mode_str += "HP";
                break;
        }
        return join_mode_str;
    }

    Window_Join(Window_Join &&) = delete; ///< Move constructor is deleted
    Window_Join &operator=(const Window_Join &) = delete; ///< Copy assignment operator is deleted
    Window_Join &operator=(Window_Join &&) = delete; ///< Move assignment operator is deleted
};

} // namespace wf

#endif
