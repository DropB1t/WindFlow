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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.eventtime.AscendingTimestampsWatermarks;
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.functions.Partitioner;
import org.slf4j.Logger;
import join.sources.FileSource;
import constants.IntervalJoinConstants;
import util.Log;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Comparator;
import java.util.concurrent.ThreadLocalRandom;

// IntervalJoinBench class
public class IntervalJoinBench {
    private static final Logger LOG = Log.get(IntervalJoinBench.class);
    public static enum testType {
        SYNTHETIC,
        ROVIO_TEST,
        STOCK_TEST;
    }
    private static testType type = testType.SYNTHETIC; // type of the dataset
    private static int numKeys = 0;
    private static int datasetSize = 0;

    // main method
    public static void main(String[] args) throws Exception {
        String alert = "Parameters: --parallelism <nRSource,nLSource,nJoin,nSink> --type <r_dataset_pathname> --lower <lower bound in ms> --upper <upper bound in ms> [--hybrid <max splitting degree>] [--load <threshold>] [--chaining]\n";
        if (args.length == 1 && args[0].equals(IntervalJoinConstants.HELP)) {
            LOG.error(alert);
            System.exit(0);
        }
        int runtime = 60; // execution time in seconds
        ParameterTool argsTool = ParameterTool.fromArgs(args);
        if (!argsTool.has("parallelism") || !argsTool.has("type") || !argsTool.has("lower") || !argsTool.has("upper")) {
            LOG.error("Error in parsing the input arguments");
            LOG.error(alert);
            System.exit(1);
        }
        int rate = 0;
        long lower_bound = argsTool.getLong("lower", 0);
        long upper_bound = argsTool.getLong("upper", 0);
        String rpath = argsTool.get("type", "");
        String lpath = rpath;
        String toReplace = "r_";
        String replacement = "l_";
        int pos = lpath.indexOf(toReplace);
        if (pos != -1) {
            lpath = lpath.substring(0, pos) + replacement + lpath.substring(pos + toReplace.length());
        }
        else {
            LOG.error("Error in parsing the input dataset files");
            System.exit(1);         
        }
        int[] parallelism_degs = ToIntArray(argsTool.get("parallelism").split(","));
        if (parallelism_degs.length != 4) {
            LOG.error("Please provide 4 parallelism degrees");
            System.exit(1);
        }
        int source1_deg = parallelism_degs[0];
        int source2_deg = parallelism_degs[1];
        int join_deg = parallelism_degs[2];
        int sink_deg = parallelism_degs[3];
        int max_splitting_degree = argsTool.getInt("hybrid", join_deg);
        if ((max_splitting_degree == 0) || (max_splitting_degree > join_deg)) {
            LOG.error("Max splitting degree is invalid");
            System.exit(1);
        }
        double threshold = argsTool.getDouble("load", 1.2);
        boolean chaining = argsTool.has("chaining");

        // parse input datasets 
        ArrayList<Tuple> ldataset, rdataset;
        RichParallelSourceFunction<Tuple6<Integer, Integer, Long, String, Integer, Integer>> orangeSource, greenSource;
        ldataset = parseDataset(lpath, IntervalJoinConstants.DEFAULT_SEPARATOR);
        rdataset = parseDataset(rpath, IntervalJoinConstants.DEFAULT_SEPARATOR);    

        // create the keys to joiners map
        ArrayList<Tuple> mergedDataset = new ArrayList<Tuple>(ldataset);
        mergedDataset.addAll(rdataset);
        ArrayList<Double> probs = new ArrayList<Double>();
        MappingPolicy.init(numKeys, datasetSize);
        MappingPolicy.computeProbabilities(mergedDataset, probs);
        Map<Integer, List<Integer>> keyToJoiners = new HashMap<>();
        ArrayList<Joiner> joiners = new ArrayList<Joiner>();
        for (int i=0; i<join_deg; i++) {
            joiners.add(new Joiner());
        }
        MappingPolicy.assignKeys(join_deg,
                                 threshold,
                                 probs,
                                 keyToJoiners,
                                 joiners,
                                 max_splitting_degree);

        // creation of the sources
        orangeSource = new FileSource(runtime, rate, ldataset, "orange", join_deg, keyToJoiners);
        greenSource = new FileSource(runtime, rate, rdataset, "green", join_deg, keyToJoiners);
        MappingPolicy.printAssignment(joiners);

        // set up the streaming execution Environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().disableGenericTypes();
        // env.getConfig().enableObjectReuse();

        // creation of the data-flow graph
        DataStream<Tuple6<Integer, Integer, Long, String, Integer, Integer>> orangeStream = env.addSource(orangeSource)
                                                    .setParallelism(source1_deg)
                                                    .assignTimestampsAndWatermarks(IngestionTimeWatermarkStrategy.create());

        DataStream<Tuple6<Integer, Integer, Long, String, Integer, Integer>> greenStream = env.addSource(greenSource)
                                                    .setParallelism(source2_deg)
                                                    .assignTimestampsAndWatermarks(IngestionTimeWatermarkStrategy.create());

        DataStream<Tuple6<Integer, Integer, Long, String, Integer, Integer>> unionedStream = orangeStream.union(greenStream);

        DataStream<Tuple6<Integer, Integer, Long, String, Integer, Integer>> multicastStream = unionedStream.partitionCustom(
                new Partitioner<Integer>() {
                    @Override
                    public int partition(Integer key, int numPartitions) {
                        return key;
                    }
                },
                tuple -> tuple.f5
        );

        TypeInformation<Tuple6<Integer, Integer, Long, String, Integer, Integer>> outType1 = TypeInformation.of(new TypeHint<Tuple6<Integer, Integer, Long, String, Integer, Integer>>() {});

        DataStream<Tuple6<Integer, Integer, Long, String, Integer, Integer>> sortedStream =
                multicastStream.transform(
                        "SortOperator",
                        outType1,
                        new SortOperator(source1_deg, source2_deg)).setParallelism(join_deg);

        TypeInformation<Tuple3<Integer, Integer, Long>> outType2 = TypeInformation.of(new TypeHint<Tuple3<Integer, Integer, Long>>() {});

        DataStream<Tuple3<Integer, Integer, Long>> joinedStream =
                sortedStream.forward().transform(
                        "RawIntervalJoin",
                        outType2,
                        new RawIntervalJoin(lower_bound, upper_bound, keyToJoiners)).setParallelism(join_deg);

        joinedStream.addSink(new ConsoleSink()).setParallelism(sink_deg);

        LOG.info("Submiting " + IntervalJoinConstants.DEFAULT_TOPO_NAME + " with parameters:\n" +
            "  * rate: " + ((rate == 0) ? "full_speed" : rate) + " tuples/second\n" +
            "  * source1: " + source1_deg + "\n" +
            "  * source2: " + source2_deg + "\n" +
            "  * join: " + join_deg + "\n" +
            "  * sink: " + sink_deg + "\n" +
            "  * lower_bound: " + lower_bound + " ms\n" +
            "  * upper_bound: " + upper_bound + " ms\n" +
            "  * TOPOLOGY\n" +
            "  * ==============================\n" +
            "  * source1 +--+ \n" +
            "  *            +--> join --> sink \n" + 
            "  * source2 +--+ \n" +
            "  * ==============================\n" +
            ((chaining) ? "  * chaining enabled" : "  * chaining disabled"));
        try {
            if (!chaining) {
                env.disableOperatorChaining();
            }
            // run the topology
            LOG.info("Executing " + IntervalJoinConstants.DEFAULT_TOPO_NAME + " topology");
            JobExecutionResult result = env.execute();
            LOG.info("Exiting");
        }
        catch (Exception e) {
            e.printStackTrace(); // Prints full cause chain
            LOG.error(e.toString());
        }
    }

    // ToIntArray method
    private static int[] ToIntArray(String[] stringArray) {
        return Stream.of(stringArray).mapToInt(Integer::parseInt).toArray();
    }

    // parseDataset method
    private static ArrayList<Tuple> parseDataset(String _file_path, String splitter) {
        ArrayList<Tuple> ds = new ArrayList<>();
        try {
            Scanner scan = new Scanner(new File(_file_path));
            if (type == testType.SYNTHETIC) {
                if (scan.hasNextLine()) {
                    String par_line = scan.nextLine();
                    String[] params = par_line.split(splitter);
                    if (params.length != 2) {
                        LOG.error("Error in parsing Syntethic parameters");
                        System.exit(1);
                    }
                    if (numKeys == 0) {
                        numKeys = Integer.valueOf(params[0]);
                    }
                    else {
                        if (numKeys != Integer.valueOf(params[0])) {
                            LOG.error("Mismatch between number of keys in the two dataset files");
                            System.exit(1);
                        }
                    }
                    datasetSize += Integer.valueOf(params[1]);
                }
            }
            while (scan.hasNextLine()) {
                String line = scan.nextLine();
                if (line.isBlank()) { continue; }
                Tuple tuple;
                String[] fields = line.split(splitter); // regex quantifier (matches one or many split char)
                switch (type) {
                    case ROVIO_TEST:
                        LOG.error("ROVIO non supported at the moment, aborting program...");
                        System.exit(1);
                        if (fields.length != 4) {
                            LOG.error("Error in parsing tuple");
                            System.exit(1);
                        }
                        tuple = new Tuple(Integer.valueOf(fields[0]), Integer.valueOf(fields[2]), 0L); // Key - Value
                    break;
                    case STOCK_TEST:
                        LOG.error("STOCK non supported at the moment, aborting program...");
                        System.exit(1);
                        if (fields.length != 2) {
                            LOG.error("Error in parsing tuple");
                            System.exit(1);
                        }
                        tuple = new Tuple(Integer.valueOf(fields[0]), Integer.valueOf(fields[1]), 0L); // Key - Value
                    break;
                    case SYNTHETIC:
                    default:
                        if (fields.length != 2) {
                            LOG.error("Error in parsing syntethic tuple");
                            System.exit(1);
                        }
                        int value = ThreadLocalRandom.current().nextInt(1, 101);
                        tuple = new Tuple(Integer.valueOf(fields[0]) - 1, value, Long.valueOf(fields[1])); // Key - Value - Timestamp
                    break;
                }
                ds.add(tuple);
            }
            scan.close();
            scan = null;
        }
        catch (FileNotFoundException | NullPointerException e) {
            LOG.error("The file {} does not exists", _file_path);
            throw new RuntimeException("The file '"  + _file_path + "' does not exists");
        }
        return ds;
    }

    // IngestionTimeWatermarkStrategy class
    private static class IngestionTimeWatermarkStrategy implements WatermarkStrategy<Tuple6<Integer, Integer, Long, String, Integer, Integer>> {

        // constructor
        private IngestionTimeWatermarkStrategy() {}

        // create method
        public static IngestionTimeWatermarkStrategy create() {
            return new IngestionTimeWatermarkStrategy();
        }

        // createWatermarkGenerator method
        @Override
        public WatermarkGenerator<Tuple6<Integer, Integer, Long, String, Integer, Integer>> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
            return new AscendingTimestampsWatermarks<>();
        }

        // createTimestampAssigner method
        @Override
        public TimestampAssigner<Tuple6<Integer, Integer, Long, String, Integer, Integer>> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
            return (event, timestamp) -> timestamp;
        }
    }
}
