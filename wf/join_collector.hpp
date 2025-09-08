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
 *  @file    join_collector.hpp
 *  @author  Gabriele Mencagli and Yuriy Rymarchuk
 *  
 *  @brief Collector used for managing input streams received by the Interval Join operator
 *         in data parallelism and hybrid parallelism modes.
 *  
 *  @section Join_Collector (Description)
 *  
 *  This class implements a FastFlow multi-input node able to receive inputs with
 *  watermarks, and to send them in output by adjusting watermarks and stream tags
 *  for join operators in a correct manner. The collector is used in data parallelism
 *  and hybrid parallelism modes together with Interval Join Operator. In case of hybrid
 *  parallelism, the collector is able to manage multiple keys and to dispatch tuples
 *  in a correct order to the join operator (key-based round robin dispatching).
 */ 

#ifndef JOIN_COLLECTOR_H
#define JOIN_COLLECTOR_H

// includes
#include<queue>
#include<unordered_map>
#include<ff/multinode.hpp>
#include<basic.hpp>
#include<batch_t.hpp>
#include<single_t.hpp>

namespace wf {

// class Join_Collector
template<typename keyextr_func_t>
class Join_Collector: public ff::ff_minode
{
private:
    keyextr_func_t key_extr; // key extractor
    using tuple_t = decltype(get_tuple_t_KeyExtr(key_extr)); // extracting the tuple_t type and checking the admissible singatures
    using key_t = decltype(get_key_t_KeyExtr(key_extr)); // extracting the key_t type and checking the admissible singatures
    bool input_batching; // true if the collector expects to receive batches, false otherwise
    ordering_mode_t ordering_mode; // ordering mode used by the Join_Collector
    Execution_Mode_t execution_mode; // execution mode of the PipeGraph
    Join_Mode_t interval_join_mode; // interval join mode
    size_t id_collector; // identifier of the Join_Collector
    size_t eos_received; // number of received EOS messages
    size_t separator_id; // stream separator meaningful to join operators
    std::vector<size_t> channel_ids; // vector containing the ids of the input channels ordered in a round-robin fashion
    size_t id; // selected channel id to forward the output from
    // Useful attributes for Data Parallelism
    std::unordered_map<size_t, std::queue<void *>> channelMap; // hash table mapping channel ids onto tuples queues of that channel
    std::vector<bool> enabled; // enable[i] is true if channel i is enabled
    std::vector<uint64_t> maxs; // maxs[i] constains the highest watermark received from the i-th input channel
    size_t next_id; // next index, for channel_ids vector, which will be used to select the next channel to forward the output from

    // Struct of a key dispatcher, useful in Hybrid Parallelism
    struct Key_Dispatcher
    {
        bool input_batching; // true if the collector expects to receive batches, false otherwise
        size_t id_collector; // identifier of the Join_Collector
        size_t num_channels; // number of input channels
        size_t next_id; // next index, for channel_ids vector of a specific key, which will be used to select the next channel to forward the output from
        uint64_t min_ch_wm; // minimum watermark among the enabled channels of a specific key
        uint64_t min_queue_wm; // minimum watermark among all tuples queues of a specific key
        bool queues_empty; // true if all the queues of the key dispatcher are empty, false otherwise
        std::vector<std::queue<void *>> key_channelMap; // vector of tuples queues of a specific key for each channel
        std::vector<uint64_t> ch_maxs; // vector of the highest watermarks received from each channel of a specific key
        std::vector<bool> ch_enabled; // vector of booleans indicating if a channel is enabled or not

        // Constructor
        Key_Dispatcher(size_t _num_channels, bool _input_batching, size_t _id_collector):
                       input_batching(_input_batching),
                       id_collector(_id_collector),
                       num_channels(_num_channels),
                       min_ch_wm(0),
                       min_queue_wm(0),
                       queues_empty(true),
                       next_id(0),
                       key_channelMap(_num_channels),
                       ch_maxs(_num_channels),
                       ch_enabled(_num_channels)
        {
            for(size_t i=0; i<_num_channels; i++) {
                key_channelMap[i] = std::queue<void *>();
                ch_maxs[i] = 0;
                ch_enabled[i] = true;
            }
        }

        // Get watermark in the i-th queue
        uint64_t getQueueWatermark(size_t i) 
        {
            if (!input_batching) {
                return reinterpret_cast<Single_t<tuple_t> *>(key_channelMap[i].front())->getWatermark(id_collector);
            }
            else {
                return reinterpret_cast<Batch_t<tuple_t> *>(key_channelMap[i].front())->getWatermark(id_collector);
            }
        }

        // Push a tuple/batch in a queue
        void push(size_t id, void *tuple)
        {
            assert(id < num_channels); // sanity check
            key_channelMap[id].push(tuple);
        }

        // Pop a tuple/batch from a queue
        void pop(size_t id)
        {
            assert(id < num_channels); // sanity check
            key_channelMap[id].pop();
        }

        // Get a tuple/batch at the beginning of a queue
        void *front(size_t id)
        {
            assert(id < num_channels); // sanity check
            return key_channelMap[id].front();
        }

        // Check if a queue is empty
        bool empty(size_t id)
        {
            assert(id < num_channels); // sanity check
            return key_channelMap[id].empty();
        }

        // Get total size of the queues
        size_t totalQueueSize()
        {
            size_t total_size = 0;
            for(size_t i=0; i<num_channels; i++) {
                total_size += key_channelMap[i].size();
            }
            return total_size;
        }

        // Increment the indentifier
        size_t incrementId()
        {
            next_id = (next_id + 1) % num_channels;
            return next_id;
        }

        // Get next id of a channel
        size_t getNextId()
        {
            return next_id;
        }

        // Get minimum watermark
        uint64_t getMinWM()
        {
            uint64_t min_wm;
            bool first = true;
            for (size_t i=0; i<num_channels; i++) {
                if (!key_channelMap[i].empty() && first) {
                    min_wm = getQueueWatermark(i);
                    first = false;
                }
                else if (ch_enabled[i] && first) {
                    min_wm = ch_maxs[i];
                    first = false;
                }
                else if (!key_channelMap[i].empty() && (getQueueWatermark(i) < min_wm)) {
                    min_wm = getQueueWatermark(i);
                }
                else if (ch_enabled[i] && (ch_maxs[i] < min_wm)) {
                    min_wm = ch_maxs[i];
                }
            }
            assert(first == false); // sanity check
            return min_wm;
        }

        // Update maximum watermarks of a channel
        void updateMaxs(size_t id, uint64_t wm)
        {
            assert(id < num_channels); // sanity check
            assert(ch_maxs[id] <= wm); // sanity check
            ch_maxs[id] = wm;
        }

        // Disable a channel
        void disableChannel(size_t id)
        {
            assert(id < num_channels); // sanity check
            ch_enabled[id] = false;
        }

    };

    std::unordered_map<key_t, Key_Dispatcher> key_dispatcherMap; // hash table mapping keys onto key dispatchers

    // Get the minimum watermark first among the channel's queue of tuples to be dispatched, then among the enabled channels
    uint64_t getMinimumWM()
    {
        if (interval_join_mode == Join_Mode_t::HP)
        {
            uint64_t min_wm = std::numeric_limits<uint64_t>::max();
            for (auto &k: key_dispatcherMap) {
                Key_Dispatcher &key_d = (k.second);
                uint64_t key_min_wm = key_d.getMinWM();
                if (key_min_wm < min_wm) {
                    min_wm = key_min_wm;
                }
            }
            assert(min_wm != std::numeric_limits<uint64_t>::max());
            return min_wm;
        }
        
        uint64_t min_wm;
        bool first = true;
        for (size_t i=0; i<this->get_num_inchannels(); i++) {
            if (!channelMap[i].empty() && first) {
                min_wm = getMinChannelWM(i);
                first = false;
            }
            else if (enabled[i] && first) {
                min_wm = maxs[i];
                first = false;
            }
            else if (!channelMap[i].empty() && (getMinChannelWM(i) < min_wm)) {
                min_wm = getMinChannelWM(i);
            }
            else if (enabled[i] && (maxs[i] < min_wm)) {
                min_wm = maxs[i];
            }
        }
        assert(first == false); // sanity check
        return min_wm;
    }

    // getMinChannelWM method (used in Data Parallelism)
    uint64_t getMinChannelWM(size_t id)
    {
        if (!input_batching){
            return reinterpret_cast<Single_t<tuple_t> *>(channelMap[id].front())->getWatermark(id_collector);
        }
        else {
            return reinterpret_cast<Batch_t<tuple_t> *>(channelMap[id].front())->getWatermark(id_collector);
        }
    }

    // Prepare a tuple for transmission
    template<typename in_t>
    inline void setup_tuple(in_t _in, size_t _source_id)
    {
        uint64_t min_wm = getMinimumWM();
        assert(maxs[_source_id] <= _in->getWatermark(id_collector)); // sanity check
        maxs[_source_id] = _in->getWatermark(id_collector); // watermarks are received ordered on the same input channel
        _in->setWatermark(min_wm, id_collector); // replace the watermark with the right one to use
        _in->setStreamTag(_source_id < separator_id ? Join_Stream_t::A : Join_Stream_t::B);
    }

    // Prepare a tuple for transmission (hybrid mode)
    template<typename in_t>
    inline void hybrid_setup_tuple(Key_Dispatcher &key_d, in_t _in, size_t _source_id)
    {
        uint64_t min_wm = getMinimumWM();
        key_d.updateMaxs(_source_id, _in->getWatermark(id_collector));
        _in->setWatermark(min_wm, id_collector);
        _in->setStreamTag(_source_id < separator_id ? Join_Stream_t::A : Join_Stream_t::B);
    }

public:
    // Constructor
    Join_Collector(keyextr_func_t _key_extr,
                   ordering_mode_t _ordering_mode,
                   Execution_Mode_t _execution_mode,
                   Join_Mode_t _interval_join_mode,
                   size_t _id_collector,
                   bool _input_batching=false,
                   size_t _separator_id=0):
                   key_extr(_key_extr),
                   input_batching(_input_batching),
                   ordering_mode(_ordering_mode),
                   execution_mode(_execution_mode),
                   interval_join_mode(_interval_join_mode),
                   id_collector(_id_collector),
                   eos_received(0),
                   separator_id(_separator_id),
                   id(0),
                   next_id(0)
    {
        assert(_execution_mode == Execution_Mode_t::DEFAULT && _ordering_mode == ordering_mode_t::TS && (_interval_join_mode == Join_Mode_t::DP || _interval_join_mode == Join_Mode_t::HP)); // sanity check
    }

    // svc_init method (utilized by the FastFlow runtime)
    int svc_init() override
    {
        maxs.clear();
        enabled.clear();
        for (size_t i=0; i<this->get_num_inchannels(); i++) {
            maxs.push_back(0);
            enabled.push_back(true);
            channelMap.insert(std::make_pair(i, std::queue<void * >()));
        }
        size_t idxA = 1;
        size_t idxB = separator_id;
        channel_ids.push_back(0);
        // the for loop will fill the channel_ids vector with the ids of the input channels in Round Robin fashion
        for(size_t i=1; i<this->get_num_inchannels(); i++) {
            if (channel_ids[i-1] >= separator_id) {
                if (idxA != separator_id) {
                    channel_ids.push_back(idxA);
                    idxA++;
                }
                else {
                    channel_ids.push_back(idxB);
                    idxB++;
                }
            }
            else {
                if (idxB != this->get_num_inchannels()) {
                    channel_ids.push_back(idxB);
                    idxB++;
                }
                else {
                    channel_ids.push_back(idxA);
                    idxA++;
                }
            }
        }
        return 0;
    }

    // svc method (utilized by the FastFlow runtime)
    void *svc(void *_in) override
    {
        size_t source_id = this->get_channel_id(); // get the index of the source stream
        if (interval_join_mode == Join_Mode_t::HP) {
            dispatch_hp(_in, source_id);
            return this->GO_ON;
        }
        if (!input_batching) { // non batching mode
            Single_t<tuple_t> * input = reinterpret_cast<Single_t<tuple_t> *>(_in); // cast the input to a Single_t structure
            id = channel_ids[next_id];
            if (source_id != id) {
                channelMap[source_id].push(input);
                return this->GO_ON;
            }
            else if (!channelMap[id].empty()) {
                channelMap[id].push(input);
                input = reinterpret_cast<Single_t<tuple_t> *>(channelMap[id].front());
                setup_tuple(input, id);
                channelMap[id].pop();
                this->ff_send_out(input);
            }
            else {
                setup_tuple(input, id);
                this->ff_send_out(input);
            }
            next_id = (next_id + 1) % this->get_num_inchannels();
            id = channel_ids[next_id];
            while (!channelMap[id].empty()) {
                input = reinterpret_cast<Single_t<tuple_t> *>(channelMap[id].front());
                setup_tuple(input, id);
                channelMap[id].pop();
                this->ff_send_out(input);
                next_id = (next_id + 1) % this->get_num_inchannels();
                id = channel_ids[next_id];
            }
            return this->GO_ON;
        }
        else { // batching mode
            Batch_t<tuple_t> *batch_input = reinterpret_cast<Batch_t<tuple_t> *>(_in); // cast the input to a Batch_t structure
            id = channel_ids[next_id];
            if (source_id != id) {
                channelMap[source_id].push(batch_input);
                return this->GO_ON;
            }
            else if (!channelMap[id].empty()) {
                channelMap[id].push(batch_input);
                batch_input = reinterpret_cast<Batch_t<tuple_t> *>(channelMap[id].front());
                setup_tuple(batch_input, id);
                channelMap[id].pop();
                this->ff_send_out(batch_input);
            }
            else {
                setup_tuple(batch_input, id);
                this->ff_send_out(batch_input);
            }
            next_id = (next_id + 1) % this->get_num_inchannels();
            id = channel_ids[next_id];
            while (!channelMap[id].empty()) {
                batch_input = reinterpret_cast<Batch_t<tuple_t> *>(channelMap[id].front());
                setup_tuple(batch_input, id);
                channelMap[id].pop();
                this->ff_send_out(batch_input);
                next_id = (next_id + 1) % this->get_num_inchannels();
                id = channel_ids[next_id];
            }
            return this->GO_ON;
        }
    }

    // Method to dispatch a tuple/Batch in hybrid mode
    void dispatch_hp(void *_in, size_t source_id) {
        if (!input_batching) {
            Single_t<tuple_t> *input = reinterpret_cast<Single_t<tuple_t> *>(_in);
            key_t key = key_extr(input->tuple);
            if (key_dispatcherMap.find(key) == key_dispatcherMap.end()) {
                key_dispatcherMap.insert(std::make_pair(key, Key_Dispatcher(this->get_num_inchannels(), input_batching, id_collector)));
            }
            auto &key_d = key_dispatcherMap.at(key);
            id = channel_ids[key_d.getNextId()];
            if (source_id != id) {
                key_d.push(source_id, input);
                return;
            }
            else if (!key_d.empty(id)) {
                key_d.push(source_id, input);
                input = reinterpret_cast<Single_t<tuple_t> *>(key_d.front(id));
                hybrid_setup_tuple(key_d, input, id);
                key_d.pop(id);
                this->ff_send_out(input);
            }
            else {
                hybrid_setup_tuple(key_d, input, id);
                this->ff_send_out(input);
            }
            id = channel_ids[key_d.incrementId()];
            while (!key_d.empty(id)) {
                input = reinterpret_cast<Single_t<tuple_t> *>(key_d.front(id));
                hybrid_setup_tuple(key_d, input, id);
                key_d.pop(id);
                this->ff_send_out(input);
                id = channel_ids[key_d.incrementId()];
            }
        }
        else {
            Batch_t<tuple_t> *batch_input = reinterpret_cast<Batch_t<tuple_t> *>(_in);
            key_t key = key_extr(batch_input->getTupleAtPos(0));
            if (key_dispatcherMap.find(key) == key_dispatcherMap.end()) {
                key_dispatcherMap.insert(std::make_pair(key, Key_Dispatcher(this->get_num_inchannels(), input_batching, id_collector)));
            }
            auto &key_d = key_dispatcherMap.at(key);
            id = channel_ids[key_d.getNextId()];
            if (source_id != id) {
                key_d.push(source_id, batch_input);
                return;
            }
            else if (!key_d.empty(id)) {
                key_d.push(source_id, batch_input);
                batch_input = reinterpret_cast<Batch_t<tuple_t> *>(key_d.front(id));
                hybrid_setup_tuple(key_d, batch_input, id);
                key_d.pop(id);
                this->ff_send_out(batch_input);
            }
            else {
                hybrid_setup_tuple(key_d, batch_input, id);
                this->ff_send_out(batch_input);
            }
            id = channel_ids[key_d.incrementId()];
            while (!key_d.empty(id)) {
                batch_input = reinterpret_cast<Batch_t<tuple_t> *>(key_d.front(id));
                hybrid_setup_tuple(key_d, batch_input, id);
                key_d.pop(id);
                this->ff_send_out(batch_input);
                id = channel_ids[key_d.incrementId()];
            }
        }
    }

    // method to manage the EOS (utilized by the FastFlow runtime)
    void eosnotify(ssize_t id) override
    {
        assert(id < this->get_num_inchannels()); // sanity check
        eos_received++;
        enabled[id] = false; // disable the channel where we received the EOS
        if (interval_join_mode == Join_Mode_t::HP) {
            for (auto &k: key_dispatcherMap) {
                Key_Dispatcher &key_d = (k.second);
                key_d.disableChannel(id);
            }
        }
        if (eos_received != this->get_num_inchannels()) { // check the number of received EOS messages
            return;
        }
        if (interval_join_mode == Join_Mode_t::HP) {
            size_t total_size = 0;
            for (auto &k: key_dispatcherMap) {
                Key_Dispatcher &key_d = (k.second);
                total_size += key_d.totalQueueSize();
            }
            if (total_size == 0) {
                return;
            }
            while (total_size > 0) {
                for (auto it = key_dispatcherMap.begin(); it != key_dispatcherMap.end(); ) {
                    Key_Dispatcher &key_d = (it->second);
                    size_t key_total_size = key_d.totalQueueSize();
                    if (key_total_size == 0) {
                        it = key_dispatcherMap.erase(it);
                        continue;
                    }
                    id = channel_ids[key_d.getNextId()];
                    while(key_total_size > 0) {
                        if (!key_d.empty(id)) {
                            if (!input_batching) {
                                Single_t<tuple_t> *out = reinterpret_cast<Single_t<tuple_t> *>(key_d.front(id));
                                hybrid_setup_tuple(key_d, out, id);
                                key_d.pop(id);
                                this->ff_send_out(out);
                            }
                            else {
                                Batch_t<tuple_t> *out = reinterpret_cast<Batch_t<tuple_t> *>(key_d.front(id));
                                hybrid_setup_tuple(key_d, out, id);
                                key_d.pop(id);
                                this->ff_send_out(out);
                            }
                            key_total_size--;
                            total_size--;
                        }
                        id = channel_ids[key_d.incrementId()];
                    }
                    it = key_dispatcherMap.erase(it);
                }
            }
            return;
        }
        size_t total_size = 0;
        for(size_t i=0; i<this->get_num_inchannels(); i++) {
            total_size += channelMap[i].size();
        }
        if (total_size == 0) {
            return;
        }
        while (total_size > 0) {
            id = channel_ids[next_id];
            if (!channelMap[id].empty()) {
                if (!input_batching) {
                    Single_t<tuple_t> *out = reinterpret_cast<Single_t<tuple_t> *>(channelMap[id].front());
                    setup_tuple(out, id);
                    channelMap[id].pop();
                    this->ff_send_out(out);
                }
                else {
                    Batch_t<tuple_t> *out = reinterpret_cast<Batch_t<tuple_t> *>(channelMap[id].front());
                    setup_tuple(out, id);
                    channelMap[id].pop();
                    this->ff_send_out(out);
                }
                total_size--;
            }
            next_id = (next_id + 1) % this->get_num_inchannels();
        }
        return;
    }

    // svc_end method (utilized by the FastFlow runtime)
    void svc_end() override
    {
        for (auto &q: channelMap) { // check that the all the channel queues are empty
            auto &channel_queue = (q.second);
            assert((channel_queue).size() == 0);
        }
        for (auto &k: key_dispatcherMap) {
            Key_Dispatcher &key_d = (k.second);
            for(size_t i=0; i<key_d.num_channels; i++) {
                assert(key_d.key_channelMap[i].size() == 0);
            }
        }
    }
};

} // namespace wf

#endif
