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

import java.io.IOException;
import java.io.Serializable;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.JsonNodeFactory;

// Metric class
public class Metric implements Serializable {
    //private String name;
    private String fileName;
    private DescriptiveStatistics descriptiveStatistics;
    private long total;

    // constructor
    public Metric(String name) {
        //this.name = name;
        this.total = 0;
        fileName = String.format("%s.json", name);
        descriptiveStatistics = new DescriptiveStatistics();
    }

    // add method
    public void add(double value) {
        descriptiveStatistics.addValue(value);
    }

    // setTotal method
    public void setTotal(long total) {
        this.total = total;
    }

    // addTotal method
    public void addTotal(long total) {
        this.total += total;
    }

    // dump method
    public void dump() throws IOException {
        ObjectNode objectNode = JsonNodeFactory.instance.objectNode();
        //objectNode.put("name", name);
        objectNode.put("total", total);
        objectNode.put("samples", descriptiveStatistics.getN());
        objectNode.put("mean", descriptiveStatistics.getMean());
        objectNode.put("0", descriptiveStatistics.getMin());
        objectNode.put("5", descriptiveStatistics.getPercentile(5));
        objectNode.put("25", descriptiveStatistics.getPercentile(25));
        objectNode.put("50", descriptiveStatistics.getPercentile(50));
        objectNode.put("75", descriptiveStatistics.getPercentile(75));
        objectNode.put("95", descriptiveStatistics.getPercentile(95));
        objectNode.put("100", descriptiveStatistics.getMax()); 
        Util.appendJson((JsonNode)objectNode, fileName);
    }
}
