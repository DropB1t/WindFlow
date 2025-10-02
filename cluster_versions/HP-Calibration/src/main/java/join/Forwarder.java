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
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.util.OutputTag;

// Forwarder class
public class Forwarder extends ProcessFunction<Tuple7<Integer, Integer, Long, String, Integer, Integer, Integer>, Tuple7<Integer, Integer, Long, String, Integer, Integer, Integer>> {
    private static final Logger LOG = LoggerFactory.getLogger(Forwarder.class);
    private transient HashMap<Integer, Integer> keyCounts;
    private long startTime;
    private long calibration_time_ms;
    private int forwarerId;
    private boolean calibrationEnd;
    public static final OutputTag<HashMap<Integer, Integer>> controlTag = new OutputTag<HashMap<Integer, Integer>>("calibration-stream"){};

    // constructor
    public Forwarder(long _calibration_time_ms)
    {
    	calibration_time_ms = _calibration_time_ms;
        calibrationEnd = false;
    }

    // open method
    @Override
    public void open(Configuration parameters) throws Exception {
    	keyCounts = new HashMap<>();
    	startTime = System.currentTimeMillis();
        forwarerId = getRuntimeContext().getIndexOfThisSubtask();
    }

    @Override
    public void processElement(Tuple7<Integer, Integer, Long, String, Integer, Integer, Integer> tuple, Context ctx, Collector<Tuple7<Integer, Integer, Long, String, Integer, Integer, Integer>> out) throws Exception {
        if (forwarerId != tuple.f5) {
            throw new RuntimeException("Forwarder receives a wrong tuple from the sources!");
        }

        long currentTime = System.currentTimeMillis();

        if (currentTime - startTime <= calibration_time_ms) { // calibration phase
            keyCounts.merge(tuple.f0, 1, Integer::sum);
        }
        else { // end of calibration
            if (!calibrationEnd) {
                ctx.output(controlTag, keyCounts);
                calibrationEnd = true;
            }
        }
        out.collect(tuple); // always forward the input tuple to the router
    }
}
