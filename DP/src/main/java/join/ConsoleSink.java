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

import util.Log;
import util.Sampler;
import util.MetricGroup;
import org.slf4j.Logger;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

// ConsoleSink class
public class ConsoleSink extends RichSinkFunction<Tuple3<Integer, Integer, Long>> {
    private static final Logger LOG = Log.get(ConsoleSink.class);
    private long received;
    private long t_start;
    private long t_end;
    private Sampler latency;
    private final long samplingRate;
    private int sinkId;

    // constructor
    public ConsoleSink(long _samplingRate) {
        samplingRate = _samplingRate;
    }

    // open method
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        t_start = System.nanoTime();
        received = 0;
        latency = new Sampler(samplingRate);
        sinkId = getRuntimeContext().getIndexOfThisSubtask();
    }

    // invoke method
    @Override
    public void invoke(Tuple3<Integer, Integer, Long> in, Context context) throws Exception {
        long timestamp = in.f2;
        // evaluate latency
        long now = System.nanoTime();
        latency.add((double)((now - timestamp)/ 1e3), System.nanoTime()); // us precision
        received++;
        // System.out.println("Sink received result [" + in.f0 + ", " + in.f1 + ", " + context.timestamp() + "]");
    }

    @Override
    public void close() {
        if (received == 0) {
            LOG.info("[Sink] received tuples: " + received);
        }
        else {
        	t_end = System.nanoTime();
            long t_elapsed = (t_end - t_start) / 1000000; // elapsed time in milliseconds
            //LOG.info("exec time " + t_elapsed + " || in seconds " + ((double)t_elapsed / 1000) );
            LOG.info("[Sink] execution time: " + t_elapsed +
                    " ms, received: " + received +
                    ", bandwidth: " + Math.floor(received / ((double)t_elapsed / 1000)) +  // tuples per second
                    " tuples/s");
            MetricGroup.add("latency", latency);
        }
    }
}
