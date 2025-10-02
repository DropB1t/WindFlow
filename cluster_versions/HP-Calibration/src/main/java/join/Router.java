/**************************************************************************************
 *  Copyright (c) 2024- Gabriele Mencagli and Yuriy Rymarchuk
 *  
 *  This file is part of IntervalJoinBenchmarks.
 *  
 *  IntervalJoinBenchmarks is free software dual licensed under the GNU LGPL or MIT License.
 *  You can redistribute it and/or modify it under the terms of the
 *    * GNU Lesser General Public License as published by
 *      the Free Software Foundation, either version 3 of the License, or
 *      (at your option) any later version
 *    OR
 *    * MIT License: https://github.com/DropB1t/IntervalJoinBenchmarks/blob/main/LICENSE
 *  
 *  IntervalJoinBenchmarks is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Lesser General Public License for more details.
 *  You should have received a copy of the GNU Lesser General Public License and
 *  the MIT License along with WindFlow. If not, see <http://www.gnu.org/licenses/>
 *  and <http://opensource.org/licenses/MIT/>.
 **************************************************************************************
 */

package join;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.api.common.state.MapStateDescriptor;

// Router class
public class Router extends BroadcastProcessFunction<Tuple7<Integer, Integer, Long, String, Integer, Integer, Integer>, HashMap<Integer, Integer>, Tuple7<Integer, Integer, Long, String, Integer, Integer, Integer>> {
    private static final Logger LOG = LoggerFactory.getLogger(Router.class);
    private Map<Integer, List<Integer>> keyToJoiners;
    private int numJoiners;
    private final MapStateDescriptor<String, HashMap<Integer, Integer>> broadcastStateDesc;
    private boolean calibrationEnd;
    private Map<Integer, Integer> freqKeys;
    private int max_splitting_degree;
    private int routerId;
    private double threshold;

    // constructor
    public Router(int _numJoiners,
                  MapStateDescriptor<String, HashMap<Integer, Integer>> _desc,
                  int _max_splitting_degree,
                  double _threshold)
    {
    	keyToJoiners = null;
        numJoiners = _numJoiners;
        broadcastStateDesc = _desc;
        calibrationEnd = false;
        freqKeys = null;
        max_splitting_degree = _max_splitting_degree;
        threshold = _threshold;
    }

    // open method
    @Override
    public void open(Configuration parameters) throws Exception {
        routerId = getRuntimeContext().getIndexOfThisSubtask();
    }

    @Override
    public void processElement(Tuple7<Integer, Integer, Long, String, Integer, Integer, Integer> tuple, ReadOnlyContext ctx, Collector<Tuple7<Integer, Integer, Long, String, Integer, Integer, Integer>> out) throws Exception {
        if (routerId != tuple.f5) {
            throw new RuntimeException("Router receives a wrong tuple from the sources!");
        }
        if (calibrationEnd) { // steady-state phase
            if (freqKeys == null) {
                freqKeys = ctx.getBroadcastState(broadcastStateDesc).get("config");
		        ArrayList<Double> probs = new ArrayList<Double>();
		        int num_tuples_calibration = freqKeys.values().stream().mapToInt(Integer::intValue).sum();
		        MappingPolicy.init(freqKeys.size(), num_tuples_calibration);
		        MappingPolicy.computeProbabilities(freqKeys, probs);
		        keyToJoiners = new HashMap<>();
		        ArrayList<Joiner> joiners = new ArrayList<Joiner>();
		        for (int i=0; i<numJoiners; i++) {
		            joiners.add(new Joiner());
		        }
		        MappingPolicy.assignKeys(numJoiners,
		                                 threshold,
		                                 probs,
		                                 keyToJoiners,
		                                 joiners,
		                                 max_splitting_degree);
				for (Integer key: freqKeys.keySet()) {
				    freqKeys.put(key, 0);
				}
				LOG.info("Router " + routerId + " has finished the calibration phase");
            }
            if (ctx.timestamp() != -1) { // valid tuple
				freqKeys.put(tuple.f0, freqKeys.getOrDefault(tuple.f0, 0) + 1);
	            List<Integer> targets = keyToJoiners.get(tuple.f0);
	            if (targets == null) {
	                throw new RuntimeException("Router receives a key that does not exist!");
	            }
                int ownId = targets.get((freqKeys.get(tuple.f0)) % targets.size());
                for (Integer dstId: targets) {
                    out.collect(new Tuple7<>(tuple.f0, tuple.f1, (long) ownId, tuple.f3, tuple.f4, dstId, 1));
                }
            }
            else { // end-of-stream message
                for (int idx=0; idx<numJoiners; idx++) {
                    out.collect(new Tuple7<>(tuple.f0, tuple.f1, tuple.f2, tuple.f3, tuple.f4, idx, tuple.f6));
                }
            }
        }
        else { // calibration phase
            if (ctx.timestamp() != -1) { // valid tuple
                int dstId = (tuple.f0).intValue() % numJoiners;
                out.collect(new Tuple7<>(tuple.f0, tuple.f1, (long) dstId, tuple.f3, tuple.f4, dstId, 0));
            }
            else { // end-of-stream message
                for (int idx=0; idx<numJoiners; idx++) {
                    out.collect(new Tuple7<>(tuple.f0, tuple.f1, tuple.f2, tuple.f3, tuple.f4, idx, tuple.f6));
                }
            }
        }
    }

    // processBroadcastElement method
    @Override
    public void processBroadcastElement(HashMap<Integer, Integer> value, Context ctx, Collector<Tuple7<Integer, Integer, Long, String, Integer, Integer, Integer>> out) throws Exception {
        ctx.getBroadcastState(broadcastStateDesc).put("config", value);
        calibrationEnd = true;
    }
}
