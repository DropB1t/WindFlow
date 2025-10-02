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

package util;

import java.util.Stack;
import java.util.HashMap;
import java.io.IOException;

// MetricGroup class
public class MetricGroup {
    private static HashMap<String, Stack<Sampler>> map;
    static {
        map = new HashMap<>();
    }

    // add method
    public static synchronized void add(String name, Sampler sampler) {
        Stack<Sampler> samplers = map.computeIfAbsent(name, key -> new Stack<>());
        samplers.add(sampler);
    }

    // dumpAll method
    public static void dumpAll() throws IOException {
        for (String name : map.keySet()) {
            Metric metric = getMetric(name);
            metric.dump();
        }
    }

    // getMetric method
    private static Metric getMetric(String name) {
        Metric metric = new Metric(name);
        // consume all the groups
        Stack<Sampler> samplers = map.get(name);
        while (!samplers.empty()) {
            Sampler sampler = samplers.pop();
            metric.addTotal(sampler.getTotal());
            // add all the values from the sampler
            for (double value : sampler.getValues()) {
                metric.add(value);
            }
        }
        return metric;
    }
}
