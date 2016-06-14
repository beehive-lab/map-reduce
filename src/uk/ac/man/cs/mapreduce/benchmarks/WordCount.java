/*
 * Copyright 2016 University of Manchester
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package uk.ac.man.cs.mapreduce.benchmarks;

import java.util.Comparator;
import java.util.List;
import uk.ac.man.cs.mapreduce.*;

public class WordCount {
    
    public final MapReduce<String, String, Integer> mrj;
    
    private final Integer one = 1;

    public WordCount() {
        mrj = new MapReduce<>(mapper, reducer);
    }
    
    private Mapper<String, String, Integer> mapper = new Mapper<String, String, Integer>() {
        @Override
        public void map(String input, Emitter<String, Integer> emitter) {
            String data = input.toUpperCase();
            int i = 0, start;
            int length = data.length();
            while (i < length) {
                while (i < length && (data.charAt(i) < 'A' || data.charAt(i) > 'Z')) {
                    i++;
                }
                start = i;
                while (i < length && ((data.charAt(i) >= 'A' && data.charAt(i) <= 'Z') || data.charAt(i) == '\'')) {
                    i++;
                }
                if (i > start) {
                    emitter.emit(data.substring(start, i), one);
                }
            }
        }
    };
    
    private Reducer<String, Integer> reducer = new Reducer<String, Integer>() {
        @Override
        public void reduce(String key, List<Integer> values, Emitter<String, Integer> emitter) {
            emitter.emit(key, values.size());
        }
    };
    
    private Comparator<KeyValue<String, Integer>> sorter = (kvp1, kvp2) -> {
        int diff = kvp2.getValue().compareTo(kvp1.getValue());
        if (diff == 0) {
            diff = kvp1.getKey().compareTo(kvp2.getKey());
        }
        return diff;
    };

    public long run(List<String> input, int parallelism) throws Exception {
        return run(input, parallelism, false);
    }
    
    public long run(List<String> input, int parallelism, boolean verbose) throws Exception {
        long startTime = System.currentTimeMillis();

        List<KeyValue<String, Integer>> results = mrj.run(input, parallelism);

        int wordCount = 0;

        for (KeyValue<String, Integer> kvp : results) {
            wordCount += kvp.getValue();
        }

        long stopTime = System.currentTimeMillis();

        if (verbose) {
            System.out.println("WORD COUNT - DISPLAYING TOP 10");

            for (int i = 0; i < Math.min(10, results.size()); i++) {
                System.out.printf("%12d %s\n", results.get(i).getValue(), results.get(i).getKey());
            }

            System.out.printf("%12d TOTAL\n", wordCount);
            System.out.printf("%12d UNIQUE WORDS\n", results.size());
            System.out.println("          in " + (stopTime - startTime));
        }

        return stopTime - startTime;
    }

    public static void main(String[] args) {
        try {
            int parallelism = Integer.decode(args[0]);

            String inputFile = args[1];

            int bufferSize = Integer.decode(args[2]);

            boolean verbose = args.length > 3;
            
            // ----- MAP REDUCE EXECUTION -----
            
            WordCount wc = new WordCount();
            
            wc.run(Splitter.fileToStringBuffers(inputFile, bufferSize), parallelism, verbose);
            
            //-------------- END --------------
            
        } catch (Exception ignore) {
            System.out.println("USEAGE: <threads> <input file> <buffer size> [<verbose>]");
        }
    }
}
