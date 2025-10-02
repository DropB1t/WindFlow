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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import util.Log;

// KeyD class
class KeyD {
    int key;
    double prob;

    // constructor
    KeyD(int _key, double _prob) {
        this.key = _key;
        this.prob = _prob;
    }
}

// Joiner class
class Joiner {
    List<Integer> assignedKeys = new ArrayList<>();
    double load = 0.0;
}

// MappingPolicy class
public class MappingPolicy {
    static int numKeys = 0;
    static int numTuples = 0;
    private static final Logger LOG = Log.get(MappingPolicy.class);

    // init method
    public static void init(int _numKeys, int _numTuples) {
        numKeys = _numKeys;
        numTuples = _numTuples;
    }

    // balancing method
    static double balancing(List<Joiner> joiners) {
        double totalLoad = 0.0;
        double maxLoad = 0.0;
        for (Joiner j: joiners) {
            totalLoad += j.load;
            if (j.load > maxLoad) {
                maxLoad = j.load;
            }
        }
        double averageLoad = totalLoad / joiners.size();
        return maxLoad / averageLoad;
    }

    // allLoaded method
    static boolean allLoaded(List<Joiner> joiners) {
        for (Joiner j: joiners) {
            if (j.load == 0) {
                return false;
            }
        }
        return true;
    }

    // allMaxSplit method
    static boolean allMaxSplit(List<Integer> splits, int maxSplitting) {
        for (int d: splits) {
            if (d < maxSplitting) {
                return false;
            }
        }
        return true;
    }

    // computeProbabilities method
    static void computeProbabilities(List<Tuple> dataset, List<Double> probs) {
        int[] counters = new int[numKeys];
        for (Tuple t: dataset) {
            counters[t.key]++;
        }
        for (int c: counters) {
            double p = ((double) c) / numTuples;
            probs.add(p);
        }
    }

    // printAssignment method
    static void printAssignment(List<Joiner> joiners) {
        String msg = "Assignment Joiners -> Keys\n";
        int idx = 0;
        for (Joiner j: joiners) {
        	msg += "Joiner " + idx++ + " has load " + j.load + ", keys { ";
            for (int k: j.assignedKeys) {
                msg += k + ", ";
            }
            msg += "}\n";
        }
        LOG.info(msg);
    }

    // averageSplitting method
    static double averageSplitting(List<Integer> splits) {
        double res = 0;
        for (int s: splits) {
            res += s;
        }
        return res / splits.size();
    }

    // assignKeys function
    static void assignKeys(int numJoiners,
                           double threshold,
                           List<Double> probs,
                           Map<Integer, List<Integer>> keyToJoiners,
                           List<Joiner> joiners,
                           int maxSplitting) {
        List<Integer> splitDegrees = new ArrayList<>(Collections.nCopies(numKeys, 0));
        List<KeyD> sortedKeys = new ArrayList<>();
        for (int i=0; i < numKeys; i++) {
            sortedKeys.add(new KeyD(i, probs.get(i)));
        }

        // sort descending by probability
        sortedKeys.sort((k1, k2) -> Double.compare(k2.prob, k1.prob));

        // FIRST PASS
        for (KeyD keyD: sortedKeys) {
            Joiner minJoiner = Collections.min(joiners, Comparator.comparingDouble(j -> j.load));
            minJoiner.assignedKeys.add(keyD.key);
            minJoiner.load += keyD.prob;
            splitDegrees.set(keyD.key, 1);
            keyToJoiners.computeIfAbsent(keyD.key, k -> new ArrayList<>()).add(joiners.indexOf(minJoiner));
        }

        if ((balancing(joiners) <= threshold) && allLoaded(joiners)) {
            LOG.info("STOP FIRST PASS -> final balancing is: " + balancing(joiners) + ", threshold was: " + threshold + ", average splitting degree: " + averageSplitting(splitDegrees));
            return;
        }

        // SECOND PASS
        boolean balanced = false;
        while (!balanced) {
            for (KeyD keyD: sortedKeys) {
                if (splitDegrees.get(keyD.key) < maxSplitting) {
                    Joiner minJoiner = Collections.min(joiners, (j1, j2) -> {
                        List<Integer> vec = keyToJoiners.getOrDefault(keyD.key, new ArrayList<>());
                        boolean j1HasKey = vec.contains(joiners.indexOf(j1));
                        boolean j2HasKey = vec.contains(joiners.indexOf(j2));
                        if (j1HasKey && !j2HasKey) return 1;
                        if (!j1HasKey && j2HasKey) return -1;
                        return Double.compare(j1.load, j2.load);
                    });
                    if (!keyToJoiners.getOrDefault(keyD.key, new ArrayList<>()).contains(joiners.indexOf(minJoiner))) {
                        for (Joiner j: joiners) {
                            if (j.assignedKeys.contains(keyD.key)) {
                                j.load = j.load - (keyD.prob / splitDegrees.get(keyD.key)) +
                                         (keyD.prob / (splitDegrees.get(keyD.key) + 1));
                            }
                        }
                        minJoiner.assignedKeys.add(keyD.key);
                        minJoiner.load += keyD.prob / (splitDegrees.get(keyD.key) + 1);
                        splitDegrees.set(keyD.key, splitDegrees.get(keyD.key) + 1);
                        keyToJoiners.computeIfAbsent(keyD.key, k -> new ArrayList<>()).add(joiners.indexOf(minJoiner));
                        if ((balancing(joiners) <= threshold) && allLoaded(joiners)) {
                            balanced = true;
                            break;
                        }
                    }
                    else {
                        LOG.error("Error in MappingPolicy");
                        System.exit(1);
                    }
                }
            }
            if (((balancing(joiners) <= threshold) && allLoaded(joiners)) || allMaxSplit(splitDegrees, maxSplitting)) {
                balanced = true;
            }
        }
        for (Joiner j: joiners) {
            j.assignedKeys.sort(Comparator.naturalOrder());
        }

        LOG.info("STOP SECOND PASS -> final balancing is: " + balancing(joiners) +
                           ", threshold was: " + threshold +
                           ", average splitting degree: " + averageSplitting(splitDegrees));
    }
}
