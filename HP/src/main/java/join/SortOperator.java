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

import org.apache.flink.api.java.tuple.Tuple6;
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
import java.util.Queue;
import java.util.PriorityQueue;
import java.util.TreeMap;

// SortOperator class
public class SortOperator extends AbstractStreamOperator<Tuple6<Integer, Integer, Long, String, Integer, Integer>>
                          implements OneInputStreamOperator<Tuple6<Integer, Integer, Long, String, Integer, Integer>, Tuple6<Integer, Integer, Long, String, Integer, Integer>> {
    private static final Logger LOG = LoggerFactory.getLogger(SortOperator.class);
    private int sorterId;
    private int numSorter;
    private long emitted;
    private long t_start;
    private long t_end;
    private int eos_counter;

    // deterministic ordering variables
    private transient Map<Integer, Queue<BufferEntry>[]> sortBuffers;
	private transient Map<Integer, TsMultiset> keyMinTimestamps;
    private transient List<String> sequence;
    private transient Map<Integer, Integer> currentIndex;
    private transient Map<String, Integer> laneIndexMap;
    private final int parOrange;
    private final int parGreen;

    // constructor
    public SortOperator(int _parOrange, int _parGreen) {
        this.parOrange = _parOrange;
        this.parGreen = _parGreen;
    }

    // open method
    @Override
    public void open() throws Exception {
       	super.open();
        sequence = new ArrayList<>();
        laneIndexMap = new HashMap<>();
        int maxPar = Math.max(parOrange, parGreen);
        int idx = 0;
        for (int i=0; i<maxPar; i++) {
            if (i < parOrange) {
                String key = "orange-" + i;
                sequence.add(key);
                laneIndexMap.put(key, idx++);
            }
            if (i < parGreen) {
                String key = "green-" + i;
                sequence.add(key);
                laneIndexMap.put(key, idx++);
            }
        }
        sortBuffers = new HashMap<>();
        keyMinTimestamps = new HashMap<>();
        currentIndex = new HashMap<>();
        t_start = System.nanoTime();
        emitted = 0;
        eos_counter = 0;
        sorterId = getRuntimeContext().getIndexOfThisSubtask();
        numSorter = getRuntimeContext().getNumberOfParallelSubtasks();
    }

    // processElement method
    @Override
    public void processElement(StreamRecord<Tuple6<Integer, Integer, Long, String, Integer, Integer>> element) {
        final Tuple6<Integer, Integer, Long, String, Integer, Integer> tuple = element.getValue();
        final long evtTs = element.getTimestamp();

        // eos_counter logic
        if (evtTs == -1) {
        	eos_counter++;
        	if (eos_counter == parOrange + parGreen) {
        		eos_notify();
        		return;
        	}
        	else {
        		return;
        	}
        }

        int key = tuple.f0;
        // initialize if missing
        if (!sortBuffers.containsKey(key)) {
            @SuppressWarnings("unchecked")
            Queue<BufferEntry>[] queues = new Queue[sequence.size()];
            for (int i=0; i<sequence.size(); i++) {
                queues[i] = new ArrayDeque<>();
            }
            sortBuffers.put(key, queues);
            currentIndex.put(key, 0);
        }
        Queue<BufferEntry>[] queues = sortBuffers.get(key);

        // find lane index via precomputed map
        String rrKey = tuple.f3 + "-" + tuple.f4;
        Integer laneIndex = laneIndexMap.get(rrKey);
        if (laneIndex == null) {
            throw new IllegalStateException("Unknown lane key: " + rrKey);
        }

        // enqueue into the corresponding lane
        queues[laneIndex].add(new BufferEntry(tuple, evtTs));
        keyMinTimestamps.computeIfAbsent(key, k -> new TsMultiset()).add(evtTs);

        // drain round-robin
        boolean progressed = true;
        while (progressed) {
            progressed = false;
            int idx = currentIndex.get(key);
            Queue<BufferEntry> q = queues[idx];
            if (!q.isEmpty()) {
                BufferEntry bt = q.poll();
                output.collect(new StreamRecord<>(bt.tuple, bt.timestamp));
                keyMinTimestamps.get(key).remove(bt.timestamp);
                emitted++;
                idx = (idx + 1) % sequence.size();
                currentIndex.put(key, idx);
                progressed = true;
            }
        }   
    }

    // eos_notify method
    public void eos_notify() {
        for (Integer key: sortBuffers.keySet()) {
            Queue<BufferEntry>[] queues = sortBuffers.get(key);
            boolean progressed = true;
            while (progressed) {
                progressed = false;
                int idx = currentIndex.get(key);
                Queue<BufferEntry> q = queues[idx];
                if (!q.isEmpty()) {
                    BufferEntry bt = q.poll();
                    output.collect(new StreamRecord<>(bt.tuple, bt.timestamp));
                    keyMinTimestamps.get(key).remove(bt.timestamp);
                    emitted++;
                    idx = (idx + 1) % sequence.size();
                    currentIndex.put(key, idx);
                    progressed = true;
                }
                else {
                    // check if any non-empty
                    for (Queue<BufferEntry> qq: queues) {
                        if (!qq.isEmpty()) {
                            int next = (idx + 1) % sequence.size();
                            currentIndex.put(key, next);
                            progressed = true;
                            break;
                        }
                    }
                }
            }
            // ensure all queues empty
            for (Queue<BufferEntry> qq : queues) {
                assert qq.isEmpty();
            }
        }
    }

    // close method
    @Override
    public void close() throws Exception {
        t_end = System.nanoTime();
        if (emitted == 0) {
            LOG.info("[Sorter] emitted tuples: " + emitted);
        }
        else {
            long t_elapsed = (t_end - t_start) / 1000000; // elapsed time in milliseconds
            LOG.info("[Sorter] " + (sorterId + 1) + "/" + numSorter + " execution time: " + t_elapsed +
                    " ms, emitted: " + emitted +
                    ", bandwidth: " + Math.floor(emitted / ((double)t_elapsed / 1000)) +  // tuples per second
                    " tuples/s");
        }
        super.close();
    }

    // processWatermark method
    @Override
    public void processWatermark(Watermark mark) throws Exception {
        long candidateWm = mark.getTimestamp();
	    long minBuffered = Long.MAX_VALUE;
	   	for (Map.Entry<Integer, TsMultiset> entry: keyMinTimestamps.entrySet()) {
	   		TsMultiset bag = entry.getValue();
	        Long kmin = bag.peekMin();
	        if (kmin != null) {
	        	if (kmin < minBuffered) {
	        		minBuffered = kmin;
	        	}
	        	long kwm = Math.min(candidateWm, kmin - 1);
	        	Tuple6<Integer, Integer, Long, String, Integer, Integer> wm_tuple = new Tuple6<>(entry.getKey(), 0, -1L, "", 0, 0);
	            output.collect(new StreamRecord<>(wm_tuple, kwm));
	        }
	    }
	    long effectiveWm = (minBuffered == Long.MAX_VALUE) ? candidateWm : Math.min(candidateWm, minBuffered - 1);
        output.emitWatermark(new Watermark(candidateWm));
    }

/*
    // TsMultiset class
	static final class TsMultiset {
	    private final PriorityQueue<Long> heap = new PriorityQueue<>();
	    private final HashMap<Long, Integer> cnt = new HashMap<>();

	    // add method O(log u_k) if new, O(1) if present
	    void add(long ts) {
	        int newCount = cnt.merge(ts, 1, Integer::sum);
	        if (newCount == 1) {
	            heap.add(ts);
	        }
	    }

	    // remove method O(1)
	    void remove(long ts) {
	        Integer c = cnt.get(ts);
	        if (c == null) {
	            return; // it should not happen
	        }
	        if (c == 1) {
	        	cnt.remove(ts);
	        }
	        else {
	        	cnt.put(ts, c - 1);
	        }
	    }

	    // peekMin method amortized O(log u_k)
	    Long peekMin() {
	        while (!heap.isEmpty()) {
	            Long top = heap.peek();
	            if (cnt.containsKey(top)) {
	            	return top;
	            }
	            heap.poll();
	        }
	        return null;
	    }

	    // isEmpty method
	    boolean isEmpty() {
	    	return peekMin() == null;
	    }
	}
*/

	// TsMultiset class
	static final class TsMultiset {
	    private final TreeMap<Long, Integer> tree = new TreeMap<>();

	    // add method
	    void add(long ts) {
	        tree.merge(ts, 1, Integer::sum);
	    }

	    // remove method
	    void remove(long ts) {
	        tree.computeIfPresent(ts, (k, v) -> (v == 1) ? null : v - 1);
	    }

	    // peekMin method
	    Long peekMin() {
	        return tree.isEmpty() ? null : tree.firstKey();
	    }

	    // isEmpty method
	    boolean isEmpty() {
	        return tree.isEmpty();
	    }
	}
}
