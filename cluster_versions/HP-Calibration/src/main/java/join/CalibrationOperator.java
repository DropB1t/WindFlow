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

// CalibrationOperator class
class CalibrationOperator extends ProcessFunction<HashMap<Integer, Integer>, HashMap<Integer, Integer>> {
	private static final Logger LOG = LoggerFactory.getLogger(CalibrationOperator.class);
	private int counter;
	private int howmany;
	private HashMap<Integer, Integer> state;
    private boolean isDone;

	// constructor
	public CalibrationOperator(int _howmany) {
		counter = 0;
		howmany = _howmany;
        isDone = false;
	}

    // open method
    @Override
    public void open(Configuration parameters) throws Exception {
    	state = new HashMap<>();
    }	

	// processElement method
    @Override
    public void processElement(HashMap<Integer, Integer> msg, Context ctx, Collector<HashMap<Integer, Integer>> out) {
        if (isDone) {
            return;
        }
        counter++;
        for (Map.Entry<Integer, Integer> entry: msg.entrySet()) {
            state.merge(entry.getKey(), entry.getValue(), Integer::sum);
        }
        if (counter == howmany) {
        	LOG.info("CalibrationOperator sends the frequencyMap to the Routers");
        	out.collect(state);
            isDone = true;
        }
    }
}
