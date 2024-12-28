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
 *  @file    hybrid_join_emitter.hpp
 *  @author  Gabriele Mencagli
 *  
 *  @brief Emitter implementing the hybrid join (HJ) distribution
 *  
 *  @section HybridJoin_Emitter (Description)
 *  
 *  The emitter delivers each received tuple to M destinations (without copies), where M is equal
 *  to hybrid parallelism degree.
 *  The emitter can be configured to work without batching (using Single_t structures)
 *  or in batched mode (using Batch_CPU_t structures).
 */ 

#ifndef HJ_EMITTER_H
#define HJ_EMITTER_H

// includes
#include<basic.hpp>
#include<single_t.hpp>
#include<batch_cpu_t.hpp>
#include<basic_emitter.hpp>

namespace wf {

// class HybridJoin_Emitter
template<typename keyextr_func_t>
class HybridJoin_Emitter: public Basic_Emitter
{
private:
    keyextr_func_t key_extr; // functional logic to extract the key attribute from the tuple_t
    using tuple_t = decltype(get_tuple_t_KeyExtr(key_extr)); // extracting the tuple_t type and checking the admissible signatures
    using key_t = decltype(get_key_t_KeyExtr(key_extr)); // extracting the key_t type and checking the admissible signatures
    size_t num_dests; // number of destinations connected in output to the emitter
    size_t hybrid_degree; // hybrid parallelism degree
    size_t size; // if >0 the emitter works in batched more, otherwise in a per-tuple basis
    bool useTreeMode; // true if the emitter is used in tree-based mode
    std::vector<std::pair<void *, size_t>> output_queue; // vector of pairs (messages and destination identifiers)
    std::unordered_map<key_t, Batch_CPU_t<tuple_t> *> batches_output; // map of the output batches one per destination channel (meaningful if size > 0)
    std::vector<uint64_t> last_sent_wms; // vector of the last sent watermarks, one per destination channel

public:
    // Constructor
    HybridJoin_Emitter(keyextr_func_t _key_extr,
                      size_t _num_dests,
                      size_t _hybrid_degree,
                      size_t _size=0):
                      key_extr(_key_extr),
                      num_dests(_num_dests),
                      hybrid_degree(_hybrid_degree),
                      size(_size),
                      useTreeMode(false),
                      last_sent_wms(_num_dests, 0)  {}

    // Copy Constructor
    HybridJoin_Emitter(const HybridJoin_Emitter &_other):
                      Basic_Emitter(_other),
                      key_extr(_other.key_extr),
                      num_dests(_other.num_dests),
                      hybrid_degree(_other.hybrid_degree),
                      size(_other.size),
                      useTreeMode(_other.useTreeMode),
                      last_sent_wms(_other.last_sent_wms) {}

    // Destructor
    ~HybridJoin_Emitter() override
    {
        assert(output_queue.size() == 0); // sanity check
        assert(batches_output.empty()); // sanity check
        /* for (const auto& [key, b] : batches_output) {
            assert(b == nullptr); // sanity check
        } */
        if (size == 0) { // delete all the Single_t items in the recycling queue
            Single_t<tuple_t> *msg = nullptr;
            while ((this->queue)->pop((void **) &msg)) {
                delete msg;
            }
        }
        else { // delete all the batches in the recycling queue
            Batch_t<tuple_t> *batch = nullptr;
            while ((this->queue)->pop((void **) &batch)) {
                delete batch;
            }
        }
    }

    // Create a clone of the emitter
    HybridJoin_Emitter *clone() const override
    {
        HybridJoin_Emitter<keyextr_func_t> *copy = new HybridJoin_Emitter<keyextr_func_t>(*this);
        return copy;
    }

    // Get the number of destinations of the emitter
    size_t getNumDestinations() const override
    {
        return num_dests;
    }

    // Set the emitter to work in tree-based mode
    void setTreeMode(bool _useTreeMode) override
    {
        useTreeMode = _useTreeMode;
    }

    // Get a reference to the vector of output messages used by the emitter
    std::vector<std::pair<void *, size_t>> &getOutputQueue() override
    {
        return output_queue;
    }

    // Static doEmit to call the right emit method
    static void doEmit(Basic_Emitter *_emitter,
                       void * _tuple,
                       uint64_t _identifier,
                       uint64_t _timestamp,
                       uint64_t _watermark,
                       ff::ff_monode *_node)
    {
        auto *_casted_emitter = static_cast<HybridJoin_Emitter<keyextr_func_t> *>(_emitter);
        _casted_emitter->emit(_tuple, _identifier, _timestamp, _watermark, _node);
    }

    // Get the pointer to the doEmit method
    doEmit_t get_doEmit() const override
    {
        return HybridJoin_Emitter<keyextr_func_t>::doEmit;
    }

    // Emit method (non in-place version)
    void emit(void *_out,
              uint64_t _identifier,
              uint64_t _timestamp,
              uint64_t _watermark,
              ff::ff_monode *_node)
    {
        tuple_t *tuple = reinterpret_cast<tuple_t *>(_out);
        if (size == 0) { // no batching
            Single_t<tuple_t> *output = allocateSingle_t(std::move(*tuple), _identifier, _timestamp, _watermark, this->queue);
            routing(output, _node);
        }
        else { // batching
            routing_batched(*tuple, _timestamp, _watermark, _node);
        }
    }

    // Static doEmit_inplace to call the right emit_inplace method
    static void emit_inplace(Basic_Emitter *_emitter,
                             void * _tuple,
                             ff::ff_monode *_node)
    {
        auto *_casted_emitter = static_cast<HybridJoin_Emitter<keyextr_func_t> *>(_emitter);
        _casted_emitter->emit_inplace(_tuple, _node);
    }

    // Get the pointer to the doEmit_inplace method
    doEmit_inplace_t get_doEmit_inplace() const override
    {
        return HybridJoin_Emitter<keyextr_func_t>::emit_inplace;
    }

    // Emit method (in-place version)
    void emit_inplace(void *_out, ff::ff_monode *_node)
    {
        Single_t<tuple_t> *output = reinterpret_cast<Single_t<tuple_t> *>(_out);
        if (size == 0) { // no batching
            routing(output, _node);
        }
        else { // batching
            routing_batched(output->tuple, output->getTimestamp(), output->getWatermark(), _node);
            deleteSingle_t(output); // delete the input Single_t
        }
    }

    // Routing method
    void routing(Single_t<tuple_t> *_output, ff::ff_monode *_node)
    {
        (_output->delete_counter).fetch_add(hybrid_degree-1);
        assert((_output->fields).size() == 3); // sanity check
        (_output->fields).insert((_output->fields).end(), num_dests-1, (_output->fields)[2]); // copy the watermark (having one per destination)
        
        auto key = key_extr(_output->tuple); // extract the key attribute
        size_t hashcode = std::hash<key_t>()(key); // compute the hashcode of the key
        size_t i = hashcode % num_dests; // compute the initial destination identifier (master_id)
        size_t sends = hybrid_degree;
        while(sends > 0) {
            assert(last_sent_wms[i] <= _output->getWatermark(i)); // sanity check
            last_sent_wms[i] = _output->getWatermark(i); // save the last watermark emitted to this destination
            if (!useTreeMode) { // real send
                _node->ff_send_out_to(_output, i);
            }
            else { // output is buffered
                output_queue.push_back(std::make_pair(_output, i));
            }
            i = (i+1) % num_dests;
            sends--;
        }
    }

    // Routing method to be used in batched mode
    void routing_batched(tuple_t &_tuple,
                         uint64_t _timestamp,
                         uint64_t _watermark,
                         ff::ff_monode *_node)
    {
        auto key = key_extr(_tuple); // extract the key attribute
        auto it = batches_output.find(key); // find the corresponding key_descriptor (or allocate it if does not exist)
        if (it == batches_output.end()) {
            auto p = batches_output.insert(std::make_pair(key, allocateBatch_CPU_t<tuple_t>(size, this->queue)));
            it = p.first;
        }
        Batch_CPU_t<tuple_t> *batch = (*it).second;
        batch->addTuple(std::move(_tuple), _timestamp, _watermark);
        
        if (batch->getSize() == size) { // batch is ready to be sent
            assert((batch->watermarks).size() == 1); // sanity check
            // copy the watermark (having one per destination)
            (batch->watermarks).insert((batch->watermarks).end(), num_dests-1, (batch->watermarks)[0]);
            (batch->delete_counter).fetch_add(hybrid_degree-1);
            
            size_t hashcode = std::hash<key_t>()(key); // compute the hashcode of the key
            size_t master_id = hashcode % num_dests; // compute the initial destination identifier (master_id)
            size_t i = master_id;
            size_t sends = hybrid_degree;
            while(sends > 0) {
                assert(last_sent_wms[i] <= (batch->watermarks)[i]); // sanity check
                last_sent_wms[i] = (batch->watermarks)[i]; // save the last watermark emitted to this destination
                if (!useTreeMode) { // real send
                    _node->ff_send_out_to(batch, i);
                }
                else { // batch_output is buffered
                    output_queue.push_back(std::make_pair(batch, i));
                }
                i = (i+1) % num_dests;
                sends--;
            }
            batches_output.erase(it); // delete the batch from the map
        }
    }

    // Punctuation propagation method
    void propagate_punctuation(uint64_t _watermark, ff::ff_monode *_node) override
    {
        if (size == 0) { // no batching
            tuple_t t; // create an empty tuple (default constructor needed!)
            Single_t<tuple_t> *punc = allocateSingle_t(std::move(t), 0, 0, _watermark, this->queue);
            (punc->delete_counter).fetch_add(num_dests-1);
            assert((punc->fields).size() == 3); // sanity check
            (punc->fields).insert((punc->fields).end(), num_dests-1, (punc->fields)[2]); // copy the watermark (having one per destination)
            punc->isPunctuation = true;
            for (size_t i=0; i<num_dests; i++) {
                if (!useTreeMode) { // real send
                    _node->ff_send_out_to(punc, i);
                }
                else { // punctuation is buffered
                    output_queue.push_back(std::make_pair(punc, i));
                }
            }
        }
        else { // batching
            flush(_node); // flush the internal partially filled batch (if any)
            tuple_t t; // create an empty tuple (default constructor needed!)
            Batch_CPU_t<tuple_t> *punc = allocateBatch_CPU_t<tuple_t>(size, this->queue);
            punc->addTuple(std::move(t), 0, _watermark);
            (punc->delete_counter).fetch_add(num_dests-1);
            assert((punc->watermarks).size() == 1); // sanity check
            (punc->watermarks).insert((punc->watermarks).end(), num_dests-1, (punc->watermarks)[0]); // copy the watermark (having one per destination)
            punc->isPunctuation = true;
            for (size_t i=0; i<num_dests; i++) {
                if (!useTreeMode) { // real send
                    _node->ff_send_out_to(punc, i);
                }
                else { // punctuation is buffered
                    output_queue.push_back(std::make_pair(punc, i));
                }
            }
        }
    }

    // Flushing method
    void flush(ff::ff_monode *_node) override
    {
        if (size > 0) { // only batching
            for (auto it = batches_output.begin(); it != batches_output.end(); ++it) {
                key_t key = it->first;
                Batch_CPU_t<tuple_t> *batch = it->second;
                size_t hashcode = std::hash<key_t>()(key); // compute the hashcode of the key
                size_t master_id = hashcode % num_dests; // compute the initial destination identifier (master_id)

                assert((batch->watermarks).size() == 1); // sanity check
                // copy the watermark (having one per destination)
                (batch->watermarks).insert((batch->watermarks).end(), num_dests-1, (batch->watermarks)[0]);
                (batch->delete_counter).fetch_add(hybrid_degree-1);

                size_t i = master_id;
                size_t sends = hybrid_degree;
                while(sends > 0) {
                    assert(last_sent_wms[i] <= (batch->watermarks)[i]); // sanity check
                    last_sent_wms[i] = (batch->watermarks)[i]; // save the last watermark emitted to this destination
                    if (!useTreeMode) { // real send
                        _node->ff_send_out_to(batch, i);
                    }
                    else { // batch_output is buffered
                        output_queue.push_back(std::make_pair(batch, i));
                    }
                    i = (i+1) % num_dests;
                    sends--;
                }
                //batches_output.erase(it); // delete the batch from the map
                //std::cout << "Erased" << std::endl;
            }
            batches_output.clear();           
            /* for (size_t i=0; i<num_dests; i++) {
                if (batches_output[i] != nullptr) {
                    assert(batches_output[i]->getSize() > 0); // sanity check
                    (batches_output[i]->delete_counter).fetch_add(hybrid_degree-1);
                    (batches_output[i]->watermarks).insert((batches_output[i]->watermarks).end(), num_dests-1, (batches_output[i]->watermarks)[0]); // copy the watermark (having one per destination)
                    size_t j = i; // master_id
                    size_t sends = hybrid_degree;
                    while(sends > 0) {
                        if (!useTreeMode) { // real send
                            _node->ff_send_out_to(batches_output[i], j);
                        }
                        else { // output is buffered
                            output_queue.push_back(std::make_pair(batches_output[i], j));
                        }
                        j = (j+1) % num_dests;
                        sends--;
                    }
                    batches_output[i] = nullptr;
                }
            } */
        }
    }

    HybridJoin_Emitter(HybridJoin_Emitter &&) = delete; ///< Move constructor is deleted
    HybridJoin_Emitter &operator=(const HybridJoin_Emitter &) = delete; ///< Copy assignment operator is deleted
    HybridJoin_Emitter &operator=(HybridJoin_Emitter &&) = delete; ///< Move assignment operator is deleted
};

} // namespace wf

#endif
