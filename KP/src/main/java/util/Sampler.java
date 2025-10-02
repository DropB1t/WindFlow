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

import java.util.List;
import java.util.ArrayList;

// Sampler class
public class Sampler {
    private final long samplesPerSeconds;
    private List<Double> samples;
    private long epoch;
    private long counter;
    private long total;

    // constructor I
    public Sampler() {
        this(0);
    }

    // constructor II
    public Sampler(long samplesPerSeconds) {
        this.samplesPerSeconds = samplesPerSeconds;
        epoch = System.nanoTime();
        counter = 0;
        total = 0;
        samples = new ArrayList<>();
    }

    // add method I
    public void add(double value) {
        add(value, 0);
    }

    // add method II
    public void add(double value, long timestamp) {
        total++;
        // add samples according to the sample rate
        double seconds = (timestamp - epoch) / 1e9;
        if (samplesPerSeconds == 0 || counter <= samplesPerSeconds * seconds) {
            samples.add(value);
            counter++;
        }
    }

    // getValues method
    public List<Double> getValues() {
        return samples;
    }

    // getTotal method
    public long getTotal() {
        return total;
    }
}
