/******************************************************************************
 *  This program is free software; you can redistribute it and/or modify it
 *  under the terms of the GNU Lesser General Public License version 3 as
 *  published by the Free Software Foundation.
 *  
 *  This program is distributed in the hope that it will be useful, but WITHOUT
 *  ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 *  FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public
 *  License for more details.
 *  
 *  You should have received a copy of the GNU Lesser General Public License
 *  along with this program; if not, write to the Free Software Foundation,
 *  Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 ******************************************************************************
 */

/** 
 *  @file    win_seqffat.hpp
 *  @author  Elia Ruggeri and Gabriele Mencagli
 *  @date    10/03/2020
 *  
 *  @brief Win_SeqFFAT operator executing a windowed query on a multi-core CPU
 *         with the algorithm in the FlatFAT data structure
 *  
 *  @section Win_SeqFFAT (Description)
 *  
 *  This file implements the Win_SeqFFAT operator able to execute windowed queries on a
 *  multicore. The operator executes streaming windows in a serial fashion on a CPU
 *  core. The algorithm is the one implemented by the FlatFAT data structure.
 *  
 *  The template parameters tuple_t and result_t must be default constructible, with
 *  a copy Constructor and copy assignment operator, and they must provide and implement
 *  the setControlFields() and getControlFields() methods.
 */ 

#ifndef WIN_SEQFFAT_H
#define WIN_SEQFFAT_H

/// includes
#include <deque>
#include <vector>
#include <string>
#include <unordered_map>
#include <math.h>
#include <ff/node.hpp>
#include <ff/multinode.hpp>
#include <meta.hpp>
#include <flatfat.hpp>
#include <meta_gpu.hpp>

namespace wf {

/** 
 *  \class Win_SeqFFAT
 *  
 *  \brief Win_SeqFFAT operator executing a windowed query on a multi-core CPU
 *         using the algorithm in the FlatFAT data structure
 *  
 *  This class implements the Win_SeqFFAT operator executing windowed queries on a multicore
 *  in a serial fashion using the algorithm in the FlatFAT data structure.
 */ 
template<typename tuple_t, typename result_t>
class Win_SeqFFAT: public ff::ff_minode_t<tuple_t, result_t>
{
public:
    /// type of the lift function
    using winLift_func_t = std::function<void(const tuple_t &, result_t &)>;
    /// type of the rich lift function
    using rich_winLift_func_t = std::function<void(const tuple_t &, result_t &, RuntimeContext &)>;
    /// type of the combine function
    using winComb_func_t = std::function<void(const result_t &, const result_t &, result_t &)>;
    /// type of the rich combine function
    using rich_winComb_func_t = std::function<void(const result_t &, const result_t &, result_t &, RuntimeContext &)>;
    /// type of the closing function
    using closing_func_t = std::function<void(RuntimeContext &)>;

private:
    // type of the FlatFAT
    using fat_t = FlatFAT<tuple_t, result_t>;
    tuple_t tmp; // never used
    // key data type
    using key_t = typename std::remove_reference<decltype(std::get<0>(tmp.getControlFields()))>::type;
    // friendships with other classes in the library
    template<typename T1, typename T2>
    friend class Key_FFAT;
    // struct of a key descriptor
    struct Key_Descriptor
    {
        fat_t fat; // FlatFAT of this key
        std::vector<result_t> pending_tuples; // vector of pending tuples of this key
        std::deque<result_t> acc_results; // deque of acculumated results
        uint64_t cb_id; // identifier used in the count-based translation
        uint64_t last_quantum; // identifier of the last quantum
        uint64_t rcv_counter; // number of tuples received of this key
        uint64_t slide_counter; // counter of the tuples in the last slide
        uint64_t ts_rcv_counter; // counter of received tuples (count-based translation)
        uint64_t next_lwid; // next window to be opened of this key (lwid)

        // Constructor I
        Key_Descriptor(winComb_func_t *_winComb_func,
                       size_t _win_len,
                       key_t _key,
                       RuntimeContext *_context):
                       fat(_winComb_func, false /* not commutative by default */, _win_len, _key, _context),
        			   cb_id(0),
                       last_quantum(0),
                       rcv_counter(0),
                       slide_counter(0),
                       ts_rcv_counter(0),
                       next_lwid(0) {}

        // Constructor II
        Key_Descriptor(rich_winComb_func_t *_rich_winComb_func,
                       size_t _win_len,
                       key_t _key,
                       RuntimeContext *_context):
                       fat(_rich_winComb_func, false /* not commutative by default */, _win_len, _key, _context),
                       cb_id(0),
                       last_quantum(0),
                       rcv_counter(0),
                       slide_counter(0),
                       ts_rcv_counter(0),
                       next_lwid(0) {}

        // move Constructor
        Key_Descriptor(Key_Descriptor &&_k):
                       fat(std::move(_k.fat)),
                       pending_tuples(std::move(_k.pending_tuples)),
                       acc_results(std::move(_k.acc_results)),
                       cb_id(_k.cb_id),
                       last_quantum(_k.last_quantum),
                       rcv_counter(_k.rcv_counter),
                       slide_counter(_k.slide_counter),
                       ts_rcv_counter(_k.ts_rcv_counter),
                       next_lwid(_k.next_lwid) {}
    };
    winLift_func_t winLift_func; // lift function
    winComb_func_t winComb_func; // combine function
    rich_winLift_func_t rich_winLift_func; // rich lift function
    rich_winComb_func_t rich_winComb_func; // rich combine function
    closing_func_t closing_func; // closing function
    uint64_t quantum; // quantum value (for time-based windows only)
    uint64_t win_len; // window length (no. of tuples or in time units)
    uint64_t slide_len; // slide length (no. of tuples or in time units)
    uint64_t triggering_delay; // triggering delay in time units (meaningful for TB windows only)
    win_type_t winType; // window type (CB or TB)
    std::string name; // string of the unique name of the operator
    bool isRichLift; // flag stating whether the lift function is riched
    bool isRichCombine; // flag stating whether the combine function is riched
    RuntimeContext context; // RuntimeContext
    OperatorConfig config; // configuration structure of the Win_SeqFFAT operator
    std::unordered_map<key_t, Key_Descriptor> keyMap; // hash table that maps a descriptor for each key
    size_t dropped_tuples; // number of dropped tuples
    size_t eos_received; // number of received EOS messages
#if defined(TRACE_WINDFLOW)
    unsigned long rcvTuples = 0;
    double avg_td_us = 0;
    double avg_ts_us = 0;
    volatile unsigned long startTD, startTS, endTD, endTS;
    ofstream *logfile = nullptr;
#endif

    // function to compute the gcd (std::gcd is available only in C++17)
    uint64_t gcd(uint64_t u, uint64_t v) {
        while (v != 0) {
            unsigned long r = u % v;
            u = v;
            v = r;
        }
        return u;
    };

    // private initialization method
    void init()
    {
        // check the validity of the windowing parameters
        if (win_len == 0 || slide_len == 0) {
            std::cerr << RED << "WindFlow Error: window length or slide cannot be zero" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // check the use of sliding windows
        if (slide_len >= win_len) {
            std::cerr << RED << "WindFlow Error: Win_SeqFFAT can be used with sliding windows only (s<w)" << DEFAULT_COLOR << std::endl;
            exit(EXIT_FAILURE);
        }
        // set the quantum value (for time-based windows only)
        if (winType == TB) {
        	quantum = gcd(win_len, slide_len);
            win_len = win_len / quantum;
            slide_len = slide_len / quantum;
        }
        else {
        	quantum = 0; // zero, quantum is never used
        }
    }

public:
    /** 
     *  \brief Constructor I
     *  
     *  \param _winLift_func the lift function to translate a tuple into a result
     *  \param _winComb_func the combine function to combine two results into a result
     *  \param _win_len window length (in no. of tuples or in time units)
     *  \param _slide_len slide length (in no. of tuples or in time units)
     *  \param _triggering_delay (triggering delay in time units, meaningful for TB windows only otherwise it must be 0)
     *  \param _winType window type (count-based CB or time-based TB)
     *  \param _name string with the unique name of the operator
     *  \param _closing_func closing function
     *  \param _context RuntimeContext object to be used
     *  \param _config configuration of the operator
     */ 
    Win_SeqFFAT(winLift_func_t _winLift_func,
                winComb_func_t _winComb_func,
                uint64_t _win_len,
                uint64_t _slide_len,
                uint64_t _triggering_delay,
                win_type_t _winType,
                std::string _name,
                closing_func_t _closing_func,
                RuntimeContext _context,
    		    OperatorConfig _config):
                winLift_func(_winLift_func),
                winComb_func(_winComb_func),
                win_len(_win_len),
                slide_len(_slide_len),
                triggering_delay(_triggering_delay),
                winType(_winType),
                name(_name),
                closing_func(_closing_func),
                context(_context),
                config(_config),
                isRichLift(false),
                isRichCombine(false),
                dropped_tuples(0),
                eos_received(0)
   	{
   		init();
   	}

    /** 
     *  \brief Constructor II
     *  
     *  \param _rich_winLift_func the rich lift function to translate a tuple into a result
     *  \param _winComb_func the combine function to combine two results into a result
     *  \param _win_len window length (in no. of tuples or in time units)
     *  \param _slide_len slide length (in no. of tuples or in time units)
     *  \param _triggering_delay (triggering delay in time units, meaningful for TB windows only otherwise it must be 0)
     *  \param _winType window type (count-based CB or time-based TB)
     *  \param _name string with the unique name of the operator
     *  \param _closing_func closing function
     *  \param _context RuntimeContext object to be used
     *  \param _config configuration of the operator
     */ 
    Win_SeqFFAT(rich_winLift_func_t _rich_winLift_func,
                winComb_func_t _winComb_func,
                uint64_t _win_len,
                uint64_t _slide_len,
                uint64_t _triggering_delay,
                win_type_t _winType,
                std::string _name,
                closing_func_t _closing_func,
                RuntimeContext _context,
                OperatorConfig _config):
                rich_winLift_func(_rich_winLift_func),
                winComb_func(_winComb_func),
                win_len(_win_len),
                slide_len(_slide_len),
                triggering_delay(_triggering_delay),
                winType(_winType),
                name(_name),
                closing_func(_closing_func),
                context(_context),
                config(_config),
                isRichLift(true),
                isRichCombine(false),
                dropped_tuples(0),
                eos_received(0)
    {
        init();
    }

    /** 
     *  \brief Constructor III
     *  
     *  \param _winLift_func the lift function to translate a tuple into a result
     *  \param _rich_winComb_func the rich combine function to combine two results into a result
     *  \param _win_len window length (in no. of tuples or in time units)
     *  \param _slide_len slide length (in no. of tuples or in time units)
     *  \param _triggering_delay (triggering delay in time units, meaningful for TB windows only otherwise it must be 0)
     *  \param _winType window type (count-based CB or time-based TB)
     *  \param _name string with the unique name of the operator
     *  \param _closing_func closing function
     *  \param _context RuntimeContext object to be used
     *  \param _config configuration of the operator
     */ 
    Win_SeqFFAT(winLift_func_t _winLift_func,
                rich_winComb_func_t _rich_winComb_func,
                uint64_t _win_len,
                uint64_t _slide_len,
                uint64_t _triggering_delay,
                win_type_t _winType,
                std::string _name,
                closing_func_t _closing_func,
                RuntimeContext _context,
                OperatorConfig _config):
                winLift_func(_winLift_func),
                rich_winComb_func(_rich_winComb_func),
                win_len(_win_len),
                slide_len(_slide_len),
                triggering_delay(_triggering_delay),
                winType(_winType),
                name(_name),
                closing_func(_closing_func),
                context(_context),
                config(_config),
                isRichLift(false),
                isRichCombine(true),
                dropped_tuples(0),
                eos_received(0)
    {
        init();
    }

    /** 
     *  \brief Constructor IV
     *  
     *  \param _rich_winLift_func the rich lift function to translate a tuple into a result
     *  \param _rich_winComb_func the rich combine function to combine two results into a result
     *  \param _win_len window length (in no. of tuples or in time units)
     *  \param _slide_len slide length (in no. of tuples or in time units)
     *  \param _triggering_delay (triggering delay in time units, meaningful for TB windows only otherwise it must be 0)
     *  \param _winType window type (count-based CB or time-based TB)
     *  \param _name string with the unique name of the operator
     *  \param _closing_func closing function
     *  \param _context RuntimeContext object to be used
     *  \param _config configuration of the operator
     */ 
    Win_SeqFFAT(rich_winLift_func_t _rich_winLift_func,
                rich_winComb_func_t _rich_winComb_func,
                uint64_t _win_len,
                uint64_t _slide_len,
                uint64_t _triggering_delay,
                win_type_t _winType,
                std::string _name,
                closing_func_t _closing_func,
                RuntimeContext _context,
                OperatorConfig _config):
                rich_winLift_func(_rich_winLift_func),
                rich_winComb_func(_rich_winComb_func),
                win_len(_win_len),
                slide_len(_slide_len),
                triggering_delay(_triggering_delay),
                winType(_winType),
                name(_name),
                closing_func(_closing_func),
                context(_context),
                config(_config),
                isRichLift(true),
                isRichCombine(true),
                dropped_tuples(0),
                eos_received(0)
    {
        init();
    }

//@cond DOXY_IGNORE

    // svc_init method (utilized by the FastFlow runtime)
    int svc_init()
    {
#if defined(TRACE_WINDFLOW)
        logfile = new std::ofstream();
        name += "_" + std::to_string(this->get_my_id()) + "_" + std::to_string(getpid()) + ".log";
#if defined(LOG_DIR)
        std::string filename = std::string(STRINGIFY(LOG_DIR)) + "/" + name;
        std::string log_dir = std::string(STRINGIFY(LOG_DIR));
#else
        std::string filename = "log/" + name;
        std::string log_dir = std::string("log");
#endif
        // create the log directory
        if (mkdir(log_dir.c_str(), 0777) != 0) {
            struct stat st;
            if((stat(log_dir.c_str(), &st) != 0) || !S_ISDIR(st.st_mode)) {
                std::cerr << RED << "WindFlow Error: directory for log files cannot be created" << DEFAULT_COLOR << std::endl;
                exit(EXIT_FAILURE);
            }
        }
        logfile->open(filename);
#endif
        return 0;
    }

    // svc method (utilized by the FastFlow runtime)
    result_t *svc(tuple_t *t)
    {
        // two separate logics depending on the window type
        if (winType == CB) {
            svcCBWindows(t);
        }
        else {
            svcTBWindows(t);
        }
        return this->GO_ON;
    }

    // processing logic with count-based windows
    inline void svcCBWindows(tuple_t *t)
    {
#if defined (TRACE_WINDFLOW)
        startTS = current_time_nsecs();
        if (rcvTuples == 0)
            startTD = current_time_nsecs();
        rcvTuples++;
#endif
        // extract the key and id fields from the input tuple
        auto key = std::get<0>(t->getControlFields()); // key
        size_t hashcode = std::hash<decltype(key)>()(key); // compute the hashcode of the key
        uint64_t id = std::get<1>(t->getControlFields()); // identifier
        // access the descriptor of the input key
        auto it = keyMap.find(key);
        if (it == keyMap.end()) {
            if (!isRichCombine) {
                keyMap.insert(std::make_pair(key, Key_Descriptor(&winComb_func, win_len, key, &context)));
            }
            else {
                keyMap.insert(std::make_pair(key, Key_Descriptor(&rich_winComb_func, win_len, key, &context)));
            }
            it = keyMap.find(key);
        }
        Key_Descriptor &key_d = (*it).second;
        // gwid of the first window of that key assigned to this Win_SeqFFAT
        uint64_t first_gwid_key = ((config.id_inner - (hashcode % config.n_inner) + config.n_inner) % config.n_inner) * config.n_outer + (config.id_outer - (hashcode % config.n_outer) + config.n_outer) % config.n_outer;
        key_d.rcv_counter++;
        key_d.slide_counter++;
        // convert the input tuple to a result with the lift function
        result_t res;
        res.setControlFields(key, 0, std::get<2>(t->getControlFields()));
        if (!isRichLift) {
            winLift_func(*t, res);
        }
        else {
            rich_winLift_func(*t, res, context);
        }
        (key_d.pending_tuples).push_back(res);
        // check whether the current window has been fired
        bool fired = false;
        uint64_t gwid;
        if (key_d.rcv_counter == win_len) { // first window when it is complete
            fired = true;
            uint64_t lwid = key_d.next_lwid;
            gwid = first_gwid_key + (lwid * config.n_outer * config.n_inner);
            key_d.next_lwid++;
            key_d.slide_counter = 0;
        }
        else if ((key_d.rcv_counter > win_len) && (key_d.slide_counter % slide_len == 0)) { // other windows when the slide is complete
            fired = true;
            uint64_t lwid = key_d.next_lwid;
            gwid = first_gwid_key + (lwid * config.n_outer * config.n_inner);
            key_d.next_lwid++;
            key_d.slide_counter = 0;
        }
        // if a window has been fired
        if (fired) {
            // add all the pending tuples to the FlatFAT
            (key_d.fat).insert(key_d.pending_tuples);
            // clear the vector of pending tuples
            (key_d.pending_tuples).clear();
            // get the result of the fired window
            result_t *out;
            out = (key_d.fat).getResult();
            // purge the tuples in the last slide from FlatFAT
            (key_d.fat).remove(slide_len);
            // send the window result
            out->setControlFields(std::get<0>(out->getControlFields()), gwid, std::get<2>(out->getControlFields()));
            this->ff_send_out(out);
        }
        // delete the input
        delete t;
#if defined(TRACE_WINDFLOW)
        endTS = current_time_nsecs();
        endTD = current_time_nsecs();
        double elapsedTS_us = ((double) (endTS - startTS)) / 1000;
        avg_ts_us += (1.0 / rcvTuples) * (elapsedTS_us - avg_ts_us);
        double elapsedTD_us = ((double) (endTD - startTD)) / 1000;
        avg_td_us += (1.0 / rcvTuples) * (elapsedTD_us - avg_td_us);
        startTD = current_time_nsecs();
#endif
    }

    // processing logic with time-based windows
    inline void svcTBWindows(tuple_t *t)
    {
#if defined (TRACE_WINDFLOW)
        startTS = current_time_nsecs();
        if (rcvTuples == 0)
            startTD = current_time_nsecs();
        rcvTuples++;
#endif
        // extract the key and timestamp fields from the input tuple
        auto key = std::get<0>(t->getControlFields()); // key
        uint64_t ts = std::get<2>(t->getControlFields()); // timestamp
        // access the descriptor of the input key
        auto it = keyMap.find(key);
        if (it == keyMap.end()) {
            if (!isRichCombine) {
                keyMap.insert(std::make_pair(key, Key_Descriptor(&winComb_func, win_len, key, &context)));
            }
            else {
                keyMap.insert(std::make_pair(key, Key_Descriptor(&rich_winComb_func, win_len, key, &context)));
            }
            it = keyMap.find(key);
        }
        Key_Descriptor &key_d = (*it).second;
        // compute the identifier of the quantum containing the input tuple
        uint64_t quantum_id = ts / quantum;
        // check if the tuple must be dropped
        if (quantum_id < key_d.last_quantum) {
            dropped_tuples++;
            delete t;
            return;
        }
        key_d.rcv_counter++;
        auto &acc_results = key_d.acc_results;
        int64_t distance = quantum_id - key_d.last_quantum;
        // resize acc_results properly
        for (size_t i=acc_results.size(); i<=distance; i++){
            result_t r;
            r.setControlFields(key, key_d.cb_id, ((key_d.last_quantum+i+1) * quantum)-1);
            key_d.cb_id++;
            acc_results.push_back(r);
        }
        // add the input tuple to the correct quantum
        result_t tmp;
        tmp.setControlFields(key, 0, ts);
        if (!isRichLift) {
            winLift_func(*t, tmp);
        }
        else {
            rich_winLift_func(*t, tmp, context);
        }
        // compute the identifier of the corresponding quantum
        size_t id = quantum_id - key_d.last_quantum;
        result_t tmp2;
        tmp2.setControlFields(key, 0, std::max(std::get<2>((acc_results[id]).getControlFields()), std::get<2>((tmp).getControlFields())));
        if (!isRichCombine) {
            winComb_func(acc_results[id], tmp, tmp2);
        }
        else {
            rich_winComb_func(acc_results[id], tmp, tmp2, context);
        }
        acc_results[id] = tmp2;
        // check whether there are complete quantums by taking into account the triggering delay
        size_t n_completed = 0;
        for (size_t i=0; i<acc_results.size(); i++) {
            uint64_t final_ts = ((key_d.last_quantum+i+1) * quantum)-1;
            if (final_ts + triggering_delay < ts) {
                n_completed++;
                processWindows(key_d, acc_results[i]);
                key_d.last_quantum++;
            }
            else {
                break;
            }
        }
        // remove the accumulated results of all the complete quantums
        acc_results.erase(acc_results.begin(), acc_results.begin() + n_completed);
        // delete the input
        delete t;
#if defined(TRACE_WINDFLOW)
        endTS = current_time_nsecs();
        endTD = current_time_nsecs();
        double elapsedTS_us = ((double) (endTS - startTS)) / 1000;
        avg_ts_us += (1.0 / rcvTuples) * (elapsedTS_us - avg_ts_us);
        double elapsedTD_us = ((double) (endTD - startTD)) / 1000;
        avg_td_us += (1.0 / rcvTuples) * (elapsedTD_us - avg_td_us);
        startTD = current_time_nsecs();
#endif
    }

    // process a window (for time-based logic)
    inline void processWindows(Key_Descriptor &key_d, result_t &r)
    {
        auto key = std::get<0>(r.getControlFields()); // key
        uint64_t id = std::get<1>(r.getControlFields()); // identifier
        size_t hashcode = std::hash<decltype(key)>()(key); // compute the hashcode of the key
        // gwid of the first window of that key assigned to this Win_SeqFFAT
        uint64_t first_gwid_key = ((config.id_inner - (hashcode % config.n_inner) + config.n_inner) % config.n_inner) * config.n_outer + (config.id_outer - (hashcode % config.n_outer) + config.n_outer) % config.n_outer;
        (key_d.pending_tuples).push_back(r);
        key_d.ts_rcv_counter++;
        key_d.slide_counter++;
        // check whether the current window has been fired
        bool fired = false;
        uint64_t gwid;
        if (key_d.ts_rcv_counter == win_len) { // first window when it is complete
            fired = true;
            uint64_t lwid = key_d.next_lwid;
            gwid = first_gwid_key + (lwid * config.n_outer * config.n_inner);
            key_d.next_lwid++;
            key_d.slide_counter = 0;
        }
        else if ((key_d.ts_rcv_counter > win_len) && (key_d.slide_counter % slide_len == 0)) { // other windows when the slide is complete
            fired = true;
            uint64_t lwid = key_d.next_lwid;
            gwid = first_gwid_key + (lwid * config.n_outer * config.n_inner);
            key_d.next_lwid++;
            key_d.slide_counter = 0;
        }
        // if a window has been fired
        if (fired) {
            // add all the pending tuples to the FlatFAT
            (key_d.fat).insert(key_d.pending_tuples);
            // clear the vector of pending tuples
            (key_d.pending_tuples).clear();
            // get the result of the fired window
            result_t *out;
            out = (key_d.fat).getResult();
            // purge the tuples in the last slide from FlatFAT
            (key_d.fat).remove(slide_len);
            // send the window result
            out->setControlFields(std::get<0>(out->getControlFields()), gwid, std::get<2>(out->getControlFields()));
            this->ff_send_out(out);
        }
    }

    // method to manage the EOS (utilized by the FastFlow runtime)
    void eosnotify(ssize_t id)
    {
        eos_received++;
        // check the number of received EOS messages
        if ((eos_received != this->get_num_inchannels()) && (this->get_num_inchannels() != 0)) { // workaround due to FastFlow
            return;
        }
        // two separate logics depending on the window type
        if (winType == CB) {
            eosnotifyCBWindows(id);
        }
        else {
            eosnotifyTBWindows(id);
        }
    }

    // eosnotify with count-based windows
    inline void eosnotifyCBWindows(ssize_t id)
    {
        // iterate over all the keys
        for (auto &k: keyMap) {
            // iterate over all the existing windows of the key
            auto key = k.first;
            size_t hashcode = std::hash<decltype(key)>()(key); // compute the hashcode of the key
            auto &key_d = k.second;
            auto &fat = key_d.fat;
            // add all the pending tuples to the FlatFAT
            fat.insert(key_d.pending_tuples);
            // loop until the FlatFAT is empty
            while (!fat.is_Empty()) {
                uint64_t first_gwid_key = ((config.id_inner - (hashcode % config.n_inner) + config.n_inner) % config.n_inner) * config.n_outer + (config.id_outer - (hashcode % config.n_outer) + config.n_outer) % config.n_outer;
                uint64_t lwid = key_d.next_lwid;
                uint64_t gwid = first_gwid_key + (lwid * config.n_outer * config.n_inner);
                key_d.next_lwid++;
                // get the result of the partial window
                result_t *out;
                out = fat.getResult();
                // purge the tuples in the last slide from FlatFAT
                fat.remove(slide_len);
                // send the window result
                out->setControlFields(std::get<0>(out->getControlFields()), gwid, std::get<2>(out->getControlFields()));
                this->ff_send_out(out);
            }
        }
    }

    // eosnotify with time-based windows
    inline void eosnotifyTBWindows(ssize_t id)
    {
        // iterate over all the keys
        for (auto &k: keyMap) {
            auto key = k.first;
            size_t hashcode = std::hash<decltype(key)>()(key); // compute the hashcode of the key
            auto &key_d = k.second;
            auto &fat = key_d.fat;
            auto &acc_results = key_d.acc_results;
            // add all the accumulated results
            for (size_t i=0; i<acc_results.size(); i++) {
               processWindows(key_d, acc_results[i]);
               key_d.last_quantum++;
            }
            // add all the pending tuples to the FlatFAT
            fat.insert(key_d.pending_tuples);
            // loop until the FlatFAT is empty
            while (!fat.is_Empty()) {
                uint64_t first_gwid_key = ((config.id_inner - (hashcode % config.n_inner) + config.n_inner) % config.n_inner) * config.n_outer + (config.id_outer - (hashcode % config.n_outer) + config.n_outer) % config.n_outer;
                uint64_t lwid = key_d.next_lwid;
                uint64_t gwid = first_gwid_key + (lwid * config.n_outer * config.n_inner);
                key_d.next_lwid++;
                // get the result of the partial window
                result_t *out;
                out = fat.getResult();
                // purge the tuples from Flat FAT
                fat.remove(slide_len);
                // send the window result
                out->setControlFields(std::get<0>(out->getControlFields()), gwid, std::get<2>(out->getControlFields()));
                this->ff_send_out(out);
            }
        }
    }

    // svc_end method (utilized by the FastFlow runtime)
    void svc_end()
    {
        // call the closing function
        closing_func(context);
#if defined(TRACE_WINDFLOW)
        std::ostringstream stream;
        stream << "************************************LOG************************************\n";
        stream << "No. of received tuples: " << rcvTuples << "\n";
        stream << "Average service time: " << avg_ts_us << " usec \n";
        stream << "Average inter-departure time: " << avg_td_us << " usec \n";
        stream << "Dropped tuples: " << dropped_tuples << "\n";
        stream << "***************************************************************************\n";
        *logfile << stream.str();
        logfile->close();
        delete logfile;
#endif
    }

//@endcond

    /** 
     *  \brief Get the window type (CB or TB) utilized by the operator
     *  \return adopted windowing semantics (count- or time-based)
     */ 
    win_type_t getWinType() const
    {
        return winType;
    }

    /** 
     *  \brief Get the number of dropped tuples by the Win_SeqFFAT
     *  \return number of tuples dropped during the processing by the Win_SeqFFAT
     */ 
    size_t getNumDroppedTuples() const
    {
        return dropped_tuples;
    }

    /// Method to start the operator execution asynchronously
    virtual int run(bool)
    {
        return ff::ff_minode::run();
    }

    /// Method to wait the operator termination
    virtual int wait()
    {
        return ff::ff_minode::wait();
    }
};

} // namespace wf

#endif
