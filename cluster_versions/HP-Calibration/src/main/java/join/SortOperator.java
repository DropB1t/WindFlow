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

import org.apache.flink.api.java.tuple.Tuple7;
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
import java.util.stream.IntStream;

// SortOperator class
public class SortOperator extends AbstractStreamOperator<Tuple7<Integer, Integer, Long, String, Integer, Integer, Integer>>
                          implements OneInputStreamOperator<Tuple7<Integer, Integer, Long, String, Integer, Integer, Integer>, Tuple7<Integer, Integer, Long, String, Integer, Integer, Integer>> {
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
    private boolean calibrationEnd;
    private transient boolean[] calibrationFlags;

    private int numSwitchTuples;
    private int numSwitchTuplesCopy;
    private long startSwitchPeriod;
    private long endSwitchPeriod;

    // constructor
    public SortOperator(int _parOrange, int _parGreen) {
        this.parOrange = _parOrange;
        this.parGreen = _parGreen;
        calibrationEnd = false;

        numSwitchTuples = 0;
        numSwitchTuplesCopy = 0;
        startSwitchPeriod = 0;
        endSwitchPeriod = 0;
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
        calibrationFlags = new boolean[parOrange + parGreen]; // initialize to false by default
    }

    // processElement method
    @Override
    public void processElement(StreamRecord<Tuple7<Integer, Integer, Long, String, Integer, Integer, Integer>> element) {
        final Tuple7<Integer, Integer, Long, String, Integer, Integer, Integer> tuple = element.getValue();
        final long evtTs = element.getTimestamp();
        int key = tuple.f0;

        if (sorterId != tuple.f5) {
            throw new RuntimeException("Sorted receives a wrong tuple from the sources!");
        }

        // eos_counter logic
        if (evtTs == -1) {
	        if (!calibrationEnd) {
	            throw new RuntimeException("Sorter receives and end-of-stream tuple while calibration phase is not over!");
	        }
        	eos_counter++;
        	if (eos_counter == parOrange + parGreen) {
        		eos_notify();
        		return;
        	}
        	else {
        		return;
        	}
        }

        // initialize queues and counter associated with the received key
        if (!sortBuffers.containsKey(key)) {
            @SuppressWarnings("unchecked")
            Queue<BufferEntry>[] queues = new Queue[sequence.size()];
            for (int i=0; i<sequence.size(); i++) {
                queues[i] = new ArrayDeque<>();
            }
            sortBuffers.put(key, queues);
            currentIndex.put(key, 0);
        }

        String rrKey = tuple.f3 + "-" + tuple.f4;
	    Integer laneIndex = laneIndexMap.get(rrKey);
	    if (laneIndex == null) {
	    	throw new IllegalStateException("Unknown lane key: " + rrKey);
	    }

        if (calibrationEnd) { // steady-state phase
        	if (tuple.f6 == 0) {
        		throw new RuntimeException("Calibration tuple received after the calibration phase is over!");
        	}
	        Queue<BufferEntry>[] queues = sortBuffers.get(key);
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

	                if (numSwitchTuplesCopy > 0) {
	                	numSwitchTuplesCopy--;
		                if (numSwitchTuplesCopy == 0) {
		                	endSwitchPeriod = System.nanoTime();
		                }	
	                }

	                output.collect(new StreamRecord<>(bt.tuple, bt.timestamp));
	                keyMinTimestamps.get(key).remove(bt.timestamp);
	                emitted++;
	                idx = (idx + 1) % sequence.size();
	                currentIndex.put(key, idx);
	                progressed = true;
	            }
	        }
        }
        else { // calibration phase
        	if (tuple.f6 == 0) { // calibration tuple
        		output.collect(new StreamRecord<>(tuple, evtTs)); // forward the tuple in output to the joiner
        	}
        	else { // non-calibration tuple
        		if (startSwitchPeriod == 0) {
        			startSwitchPeriod = System.nanoTime();
        		}
        		Queue<BufferEntry>[] queues = sortBuffers.get(key);
        		calibrationFlags[laneIndex] = true;
		        queues[laneIndex].add(new BufferEntry(tuple, evtTs));
		        keyMinTimestamps.computeIfAbsent(key, k -> new TsMultiset()).add(evtTs);
		        calibrationEnd = IntStream.range(0, calibrationFlags.length).allMatch(i -> calibrationFlags[i]);

		        numSwitchTuples++;
		        numSwitchTuplesCopy++;
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

            LOG.info("[Sorter] switching tuples " + numSwitchTuples);
            LOG.info("[Sorter] switching time + " + (endSwitchPeriod - startSwitchPeriod) / 1e6);
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
	        	Tuple7<Integer, Integer, Long, String, Integer, Integer, Integer> wm_tuple = new Tuple7<>(entry.getKey(), -1, -1L, "", -1, sorterId, -1);
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
