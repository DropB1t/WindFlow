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
 *  @file    map.hpp
 *  @author  Gabriele Mencagli
 *  @date    08/01/2019
 *  
 *  @brief Map pattern executing a one-to-one transformation on the input stream
 *  
 *  @section Map (Description)
 *  
 *  This file implements the Map pattern able to execute a one-to-one transformation
 *  on each tuple of the input data stream. The transformation should be stateless and
 *  must produce one output result for each input tuple consumed.
 *  
 *  The template arguments tuple_t and result_t must be default constructible, with a copy constructor
 *  and copy assignment operator, and they must provide and implement the setInfo() and
 *  getInfo() methods.
 */ 

#ifndef MAP_H
#define MAP_H

// includes
#include <string>
#include <ff/node.hpp>
#include <ff/farm.hpp>
#include <builders.hpp>

using namespace ff;

/** 
 *  \class Map
 *  
 *  \brief Map pattern executing a one-to-one transformation on the input stream
 *  
 *  This class implements the Map pattern executing a one-to-one stateless transformation
 *  on each tuple of the input stream.
 */ 
template<typename tuple_t, typename result_t>
class Map: public ff_farm
{
public:
    /// type of the map function (in-place version)
    using map_func_ip_t = function<void(tuple_t &)>;
    /// type of the map function (not in-place version)
    using map_func_nip_t = function<void(const tuple_t &, result_t &)>;
private:
    // class Map_Node
    class Map_Node: public ff_node_t<tuple_t, result_t>
    {
    private:
        map_func_ip_t func_ip; // in-place map function
        map_func_nip_t func_nip; // not in-place map function
        string name; // string of the unique name of the pattern
        bool isIP; // flag stating if the in-place map function should be used (otherwise the not in-place version)
#if defined(LOG_DIR)
        unsigned long rcvTuples = 0;
        double avg_td_us = 0;
        double avg_ts_us = 0;
        volatile unsigned long startTD, startTS, endTD, endTS;
        ofstream logfile;
#endif
    public:
        // Constructor I (in-place version)
        template <typename T=string>
        Map_Node(typename enable_if<is_same<T,T>::value && is_same<tuple_t,result_t>::value, map_func_ip_t>::type _func, T _name):
                 func_ip(_func), name(_name), isIP(true) {}

        // Constructor II (not in-place version)
        template <typename T=string>
        Map_Node(typename enable_if<is_same<T,T>::value && !is_same<tuple_t,result_t>::value, map_func_nip_t>::type _func, T _name):
                 func_nip(_func), name(_name), isIP(false) {}

        // svc_init method (utilized by the FastFlow runtime)
        int svc_init()
        {
#if defined(LOG_DIR)
            name += "_node_" + to_string(ff_node_t<tuple_t, result_t>::get_my_id()) + ".log";
            string filename = string(STRINGIFY(LOG_DIR)) + "/" + name;
            logfile.open(filename);
#endif
            return 0;
        }

        // svc method (utilized by the FastFlow runtime)
        result_t *svc(tuple_t *t)
        {
#if defined (LOG_DIR)
            startTS = current_time_nsecs();
            if (rcvTuples == 0)
                startTD = current_time_nsecs();
            rcvTuples++;
#endif
            result_t *r;
            // in-place version
            if (isIP) {
                func_ip(*t);
                r = reinterpret_cast<result_t *>(t);
            }
            else {
                r = new result_t();
                func_nip(*t, *r);
                delete t;
            }
#if defined(LOG_DIR)
            endTS = current_time_nsecs();
            endTD = current_time_nsecs();
            double elapsedTS_us = ((double) (endTS - startTS)) / 1000;
            avg_ts_us += (1.0 / rcvTuples) * (elapsedTS_us - avg_ts_us);
            double elapsedTD_us = ((double) (endTD - startTD)) / 1000;
            avg_td_us += (1.0 / rcvTuples) * (elapsedTD_us - avg_td_us);
            startTD = current_time_nsecs();
#endif
            return r;
        }

        // svc_end method (utilized by the FastFlow runtime)
        void svc_end()
        {
#if defined (LOG_DIR)
            ostringstream stream;
            stream << "************************************LOG************************************\n";
            stream << "No. of received tuples: " << rcvTuples << "\n";
            stream << "Average service time: " << avg_ts_us << " usec \n";
            stream << "Average inter-departure time: " << avg_td_us << " usec \n";
            stream << "***************************************************************************\n";
            logfile << stream.str();
            logfile.close();
#endif
        }
    };

public:
    /** 
     *  \brief Constructor I (in-place version, valid if tuple_t = result_t)
     *  
     *  \param _func a function/functor to be executed on each input tuple
     *  \param _pardegree parallelism degree of the Map pattern
     *  \param _name string with the unique name of the Map pattern
     */ 
    template <typename T=size_t>
    Map(typename enable_if<is_same<T,T>::value && is_same<tuple_t,result_t>::value, map_func_ip_t>::type _func, T _pardegree, string _name)
    {
        // check the validity of the parallelism degree
        if (_pardegree == 0) {
            cerr << RED << "WindFlow Error: parallelism degree cannot be zero" << DEFAULT << endl;
            exit(EXIT_FAILURE);
        }
        // vector of Map_Node instances
        vector<ff_node *> w;
        for (size_t i=0; i<_pardegree; i++) {
            auto *seq = new Map_Node(_func, _name);
            w.push_back(seq);
        }
        ff_farm::add_workers(w);
        // add default collector
        ff_farm::add_collector(nullptr);
        // when the Map will be destroyed we need aslo to destroy the emitter, workers and collector
        ff_farm::cleanup_all();
    }

    /** 
     *  \brief Constructor II (not in-place version, valid if tuple_t != result_t)
     *  
     *  \param _func a function/functor to be executed on each input tuple
     *  \param _pardegree parallelism degree of the Map pattern
     *  \param _name string with the unique name of the Map pattern
     */ 
    template <typename T=size_t>
    Map(typename enable_if<is_same<T,T>::value && !is_same<tuple_t,result_t>::value, map_func_nip_t>::type _func, T _pardegree, string _name)
    {
        // check the validity of the parallelism degree
        if (_pardegree == 0) {
            cerr << RED << "WindFlow Error: parallelism degree cannot be zero" << DEFAULT << endl;
            exit(EXIT_FAILURE);
        }
        // vector of Map_Node instances
        vector<ff_node *> w;
        for (size_t i=0; i<_pardegree; i++) {
            auto *seq = new Map_Node(_func, _name);
            w.push_back(seq);
        }
        ff_farm::add_workers(w);
        // add default collector
        ff_farm::add_collector(nullptr);
        // when the Map will be destroyed we need aslo to destroy the emitter, workers and collector
        ff_farm::cleanup_all();
    }

//@cond DOXY_IGNORE

    // -------------------------------------- deleted methods ----------------------------------------
    template<typename T>
    int add_emitter(T *e)                                                                    = delete;
    template<typename T>
    int add_emitter(const T &e)                                                              = delete;
    template<typename T>
    int change_emitter(T *e, bool cleanup=false)                                             = delete;
    template<typename T>
    int change_emitter(const T &e, bool cleanup=false)                                       = delete;
    void set_ordered(const size_t MemoryElements=DEF_OFARM_ONDEMAND_MEMORY)                  = delete;
    int add_workers(std::vector<ff_node *> &w)                                               = delete;
    int add_collector(ff_node *c, bool cleanup=false)                                        = delete;
    int wrap_around(bool multi_input=false)                                                  = delete;
    int remove_collector()                                                                   = delete;
    void cleanup_workers()                                                                   = delete;
    void cleanup_all()                                                                       = delete;
    bool offload(void *task, unsigned long retry=((unsigned long)-1),
        unsigned long ticks=ff_loadbalancer::TICKS2WAIT)                                     = delete;
    bool load_result(void **task, unsigned long retry=((unsigned long)-1),
        unsigned long ticks=ff_gatherer::TICKS2WAIT)                                         = delete;
    bool load_result_nb(void **task)                                                         = delete;

private:
    using ff_farm::set_scheduling_ondemand;

//@endcond

};

#endif