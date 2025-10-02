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

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import join.Tuple;
import util.ThroughputCounter;
import java.util.ArrayList;

// FileSource class
public class FileSource extends RichParallelSourceFunction<Tuple5<Integer, Integer, Long, String, Integer>> {
    private static final Logger LOG = LoggerFactory.getLogger(FileSource.class);
    private long t_start;
    private long t_end;
    private ArrayList<Tuple> dataset;
    private int data_size;
    private final long runtime;
    private boolean running = true;
    private final int gen_rate;
    private long nt_execution;
    private long generated;
    private int index;
    private long ts = 1704106800000L; // January 1, 2024 12:00:00 AM in ms
    private String streamTag;
    private int sourceId;

    // constructor
    public FileSource(long _runtime,
    	              int _gen_rate,
    	              ArrayList<Tuple> _dataset,
    	              String _streamTag) {
        this.runtime = (long) (_runtime * 1e9); // ns
        this.gen_rate = _gen_rate;
        this.dataset = _dataset;
        index = 0;
        generated = 0;
        data_size = dataset.size();
        nt_execution = 0;
        streamTag = _streamTag;
    }

    // open method
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        sourceId = getRuntimeContext().getIndexOfThisSubtask();
    }

    // run method
    @Override
    public void run(final SourceContext<Tuple5<Integer, Integer, Long, String, Integer>> ctx) throws Exception {
        this.t_start = System.nanoTime();
        // generation loop
        while ((System.nanoTime() - this.t_start < runtime) && running) {
        // while(true) {
            Tuple tuple = dataset.get(index);
            ts += tuple.ts_off != 0L ? tuple.ts_off : 500L;            
            ctx.collectWithTimestamp(new Tuple5<>(tuple.key, tuple.value, System.nanoTime(), streamTag, sourceId), ts);
            generated++;
            index++;
            if (gen_rate != 0) { // limit generation rate with active delay
                long delay_nsec = (long) ((1.0d / gen_rate) * 1e9);
                active_delay(delay_nsec);
            }
            if (index >= data_size) { // check the dataset boundaries
                index = 0;
                nt_execution++;
                // break;
            }
        }

        // send end-of-stream message to the joiners
        ctx.collectWithTimestamp(new Tuple5<>(0, 0, 0L, streamTag, sourceId), -1);

        // terminate the generation
        running = false;
        ThroughputCounter.add(generated);
    }

    // active_delay method
    private void active_delay(double nsecs) {
        long t_start = System.nanoTime();
        long t_now;
        boolean end = false;
        while (!end) {
            t_now = System.nanoTime();
            end = (t_now - t_start) >= nsecs;
        }
    }

    // cancel method
    @Override
    public void cancel() {
        running = false;
    }

    // close method
    @Override
    public void close() {
        if (generated == 0) {
            LOG.info("[Source] generated tuples: " + generated);
        }
        else {
            t_end = System.nanoTime();
            long t_elapsed = (this.t_end - this.t_start) / 1000000; // elapsed time in milliseconds
            double rate = Math.floor( generated / ((double)(this.t_end - this.t_start) / 1e9) ); // per second
            LOG.info("[Source " + streamTag + " " + sourceId + "] execution time: " + t_elapsed +
                    " ms, generated: " + generated +
                    ", generations: " + nt_execution +
                    ", bandwidth: " + rate +  // tuples per second
                    " tuples/s");
        }
    }
}
