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
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.SortedMap;
import java.util.Queue;
import java.util.PriorityQueue;

// RawIntervalJoin class
public class RawIntervalJoin extends AbstractStreamOperator<Tuple3<Integer, Integer, Long>>
                             implements OneInputStreamOperator<Tuple5<Integer, Integer, Long, String, Integer>, Tuple3<Integer, Integer, Long>> {
    private static final Logger LOG = LoggerFactory.getLogger(RawIntervalJoin.class);
    private int joinerId;
    private int numJoiners;
    private long emitted;
    private long t_start;
    private long t_end;
    final double FILTERING_RATIO  = 0.05;

    // interval join variables
    private final long lowerBound;
    private final long upperBound;
    private transient Map<Integer, NavigableMap<Long, List<BufferEntry>>> orangeBuffer;
    private transient Map<Integer, NavigableMap<Long, List<BufferEntry>>> greenBuffer;
    private transient PriorityQueue<TimerEntry> timerHeap;

    // constructor
    public RawIntervalJoin(long _lowerBound, long _upperBound) {
        this.lowerBound = _lowerBound;
        this.upperBound = _upperBound;
        if (lowerBound > upperBound) {
            throw new IllegalArgumentException("RawIntervalJoin: lowerBound > upperBound");
        }
    }

    // open method
    @Override
    public void open() throws Exception {
        super.open();
        orangeBuffer = new HashMap<>();
        greenBuffer = new HashMap<>();
        joinerId = getRuntimeContext().getIndexOfThisSubtask();
        numJoiners = getRuntimeContext().getNumberOfParallelSubtasks();
        timerHeap = new PriorityQueue<>();
        t_start = System.nanoTime();
        emitted = 0;
    }

    // processElement method
    @Override
    public void processElement(StreamRecord<Tuple5<Integer, Integer, Long, String, Integer>> element) {
        final Tuple5<Integer, Integer, Long, String, Integer> tuple = element.getValue();
        final long evtTs = element.getTimestamp();
        BufferEntry bt = new BufferEntry(tuple, evtTs);
        if ("orange".equals(bt.tuple.f3)) {
            processJoinOrange(bt);
        }
        else {
            processJoinGreen(bt);
        }
    }

    // close method
    @Override
    public void close() throws Exception {
        t_end = System.nanoTime();
        if (emitted == 0) {
            LOG.info("[Join] emitted tuples: " + emitted);
        }
        else {
            long t_elapsed = (t_end - t_start) / 1000000; // elapsed time in milliseconds
            LOG.info("[Join] " + (joinerId + 1) + "/" + numJoiners + " execution time: " + t_elapsed +
                    " ms, emitted: " + emitted +
                    ", bandwidth: " + Math.floor(emitted / ((double)t_elapsed / 1000)) +  // tuples per second
                    " tuples/s");
        }
        super.close();
    }

    // processJoinOrange method
    private void processJoinOrange(BufferEntry bt) {
        final Tuple5<Integer, Integer, Long, String, Integer> t = bt.tuple;
        orangeBuffer.computeIfAbsent(t.f0, k -> new TreeMap<>());
        greenBuffer.computeIfAbsent(t.f0, k -> new TreeMap<>());
        processJoin(bt, orangeBuffer.get(t.f0), greenBuffer.get(t.f0), lowerBound, upperBound, true);
    }

    // processJoinGreen method
    private void processJoinGreen(BufferEntry bt) {
        final Tuple5<Integer, Integer, Long, String, Integer> t = bt.tuple;
        orangeBuffer.computeIfAbsent(t.f0, k -> new TreeMap<>());
        greenBuffer.computeIfAbsent(t.f0, k -> new TreeMap<>());
        processJoin(bt, greenBuffer.get(t.f0), orangeBuffer.get(t.f0), -upperBound, -lowerBound, false);
    }

    // processJoin method
    private void processJoin(
            BufferEntry bt,
            NavigableMap<Long, List<BufferEntry>> ourBuffer,
            NavigableMap<Long, List<BufferEntry>> otherBuffer,
            long relativeLowerBound,
            long relativeUpperBound,
            boolean isOrange) {
        final Tuple5<Integer, Integer, Long, String, Integer> t = bt.tuple;
        final long ts = bt.timestamp;

        // storing the input tuple in a circular manner
		addToBuffer(ourBuffer, bt);

        // probe the other side
	  	long low  = ts + relativeLowerBound;
	  	long high = ts + relativeUpperBound;
	  	SortedMap<Long, List<BufferEntry>> cand = otherBuffer.subMap(low, true, high, true);
        for (SortedMap.Entry<Long, List<BufferEntry>> e: cand.entrySet()) {
            long otherTs = e.getKey();
            for (BufferEntry o: e.getValue()) {
                if (isOrange) {
                    joinEmit(t, o.tuple, ts, otherTs);
                }
                else {
                    joinEmit(o.tuple, t, otherTs, ts);
                }
            }
        }

        long cleanupTime = (relativeUpperBound > 0L) ? ts + relativeUpperBound : ts;
        if (isOrange) {
            timerHeap.add(new TimerEntry(cleanupTime, "orange", t.f0));
        }
        else {
            timerHeap.add(new TimerEntry(cleanupTime, "green", t.f0));
        }
    }

    // addToBuffer method
    private static void addToBuffer(Map<Long, List<BufferEntry>> buffer, BufferEntry bt) {
        buffer.computeIfAbsent(bt.timestamp, k -> new ArrayList<>()).add(bt);
    }

    // processWatermark method
    @Override
    public void processWatermark(Watermark mark) throws Exception {
        long wm = mark.getTimestamp();
        while (!timerHeap.isEmpty() && timerHeap.peek().timestamp <= wm) {
            TimerEntry entry = timerHeap.poll();
            fireCleanup(entry);
        }
        super.processWatermark(mark); // important to forward watermarks
    }

    // fireCleanup method
    private void fireCleanup(TimerEntry entry) {
        if ((entry.type).equals("orange")) {
            NavigableMap<Long, List<BufferEntry>> buffer = orangeBuffer.get(entry.key);
            if (buffer != null) {
                long timestamp = (upperBound <= 0L) ? entry.timestamp : entry.timestamp - upperBound;
                buffer.remove(timestamp);
            }
        }
        else {
            NavigableMap<Long, List<BufferEntry>> buffer = greenBuffer.get(entry.key);
            if (buffer != null) {
                long timestamp = (lowerBound <= 0L) ? entry.timestamp + lowerBound : entry.timestamp;
                buffer.remove(timestamp);
            }
        }
    }

    // joinEmit method
    private void joinEmit(Tuple5<Integer, Integer, Long, String, Integer> left,
                          Tuple5<Integer, Integer, Long, String, Integer> right,
                          long leftTs,
                          long rightTs) {
        long resultTs = Math.max(leftTs, rightTs);
        int threshold = (int) ((FILTERING_RATIO * 199) + 2);
        if ((left.f1 + right.f1) < threshold) {
        // if (true) {
	        Tuple3<Integer, Integer, Long> result = new Tuple3<Integer, Integer, Long>();
        	result.f0 = left.f0;
        	result.f1 = left.f1 + right.f1;
        	result.f2 = Math.max(left.f2, right.f2);
        	output.collect(new StreamRecord<>(result, resultTs));
	        emitted++;
        }
    }

    // TimerEntry class
    public static class TimerEntry implements Comparable<TimerEntry> {
        final long timestamp;
        final String type;
        final Integer key;

        // constructor
        TimerEntry(long _timestamp, String _type, Integer _key) {
            this.timestamp = _timestamp;
            this.type = _type;
            this.key = _key;
        }

        // compareTo method
        @Override
        public int compareTo(TimerEntry other) {
            return Long.compare(this.timestamp, other.timestamp);
        }
    }
}
