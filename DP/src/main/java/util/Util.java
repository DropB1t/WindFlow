/**************************************************************************************
 *  Copyright (c) 2019- Gabriele Mencagli and Yuriy Rymarchuk
 *  
 *  This file is part of StreamBenchmarks.
 *  
 *  StreamBenchmarks is free software dual licensed under the GNU LGPL or MIT License.
 *  You can redistribute it and/or modify it under the terms of the
 *    * GNU Lesser General Public License as published by
 *      the Free Software Foundation, either version 3 of the License, or
 *      (at your option) any later version
 *    OR
 *    * MIT License: https://github.com/ParaGroup/StreamBenchmarks/blob/master/LICENSE.MIT
 *  
 *  StreamBenchmarks is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Lesser General Public License for more details.
 *  You should have received a copy of the GNU Lesser General Public License and
 *  the MIT License along with WindFlow. If not, see <http://www.gnu.org/licenses/>
 *  and <http://opensource.org/licenses/MIT/>.
 **************************************************************************************
 */

package util;

import java.io.File;
import java.io.IOException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectWriter;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.JsonNodeType;

// Util class
public class Util {
    // appendJson method
    public static void appendJson(JsonNode jsonNode, String fileName) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        ObjectWriter objectWriter = objectMapper.writer(new DefaultPrettyPrinter());
        // read existing JSON file
        File file = new File(fileName);
        ArrayNode jsonArray;
        if (file.exists()) {
            JsonNode rootNode = objectMapper.readTree(file);
            if (rootNode.getNodeType() == JsonNodeType.ARRAY) {
                jsonArray = (ArrayNode) rootNode;
            } else {
                jsonArray = objectMapper.createArrayNode();
            }
        } else {
            jsonArray = objectMapper.createArrayNode();
        }
        // append new JSON object
        jsonArray.add(jsonNode);
        // write JSON file
        objectWriter.writeValue(file, jsonArray);
    }
}
