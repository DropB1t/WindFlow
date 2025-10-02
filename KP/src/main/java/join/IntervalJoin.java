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

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import util.Log;
import java.util.concurrent.ThreadLocalRandom;

// IntervalJoin class
public class IntervalJoin extends ProcessJoinFunction<Tuple3<Integer, Integer, Long>, Tuple3<Integer, Integer, Long>, Tuple3<Integer, Integer, Long>> {
    private static final Logger LOG = Log.get(IntervalJoin.class);
    private long emitted;
    private long t_start;
    private long t_end;
    final double FILTERING_RATIO  = 0.05;

    // Open method
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        t_start = System.nanoTime();
        emitted = 0;
    }

    // processElement method
    @Override
    public void processElement(Tuple3<Integer, Integer, Long> first, Tuple3<Integer, Integer, Long> second, Context ctx, Collector<Tuple3<Integer, Integer, Long>> out) throws Exception {
        int threshold = (int) ((FILTERING_RATIO * 199) + 2);
        if ((first.f1 + second.f1) < threshold) {
        // if (true) {
	        Tuple3<Integer, Integer, Long> out_t = new Tuple3<Integer, Integer, Long>();
	        out_t.f0 = first.f0;
	        out_t.f1 = Integer.valueOf(first.f1 + second.f1);
	        out_t.f2 = Long.valueOf(Math.max(first.f2, second.f2));
	        out.collect(out_t);
	        emitted++;
        }
    }

    // close method
    @Override
    public void close() {
        if (emitted == 0) {
            LOG.info("[Join] emitted tuples: " + emitted);
        }
        else {
        	t_end = System.nanoTime();
            int id = getRuntimeContext().getIndexOfThisSubtask();
            int n = getRuntimeContext().getNumberOfParallelSubtasks();
            long t_elapsed = (t_end - t_start) / 1000000; // elapsed time in milliseconds
            LOG.info("[Join] " + (id+1) + "/" + n + " execution time: " + t_elapsed +
                    " ms, emitted: " + emitted +
                    ", bandwidth: " + Math.floor(emitted / ((double)t_elapsed / 1000)) +  // tuples per second
                    " tuples/s");
        }
    }
}
