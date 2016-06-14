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

import java.util.List;
import uk.ac.man.cs.mapreduce.*;

public class StringMatch {

    private final String key1 = "Helloworld";
    private final String key2 = "howareyou";
    private final String key3 = "ferrari";
    private final String key4 = "whotheman";
    private char[] testKey1, testKey2, testKey3, testKey4;
    private final char offset = 5;
    
    private final MapReduce<String, String, Integer> mrj;

    public StringMatch() {
        mrj = new MapReduce<>(mapper, reducer);

        testKey1 = hash(key1);
        testKey2 = hash(key2);
        testKey3 = hash(key3);
        testKey4 = hash(key4);
    }
    
    private Mapper<String, String, Integer> mapper = new Mapper<String, String, Integer>() {
        @Override
        public void map(String input, Emitter<String, Integer> emitter) {
            
            char[] keys = input.toCharArray();
            char[] buffer = new char[32];
            
            int length = keys.length;
            int i = 0;
            int j;
            
            while (i < length) {
                j = 0;
            
                while((i < length) && keys[i] != '\r' && keys[i] != '\n') {
                    buffer[j++] = keys[i++];
                }

                hash(buffer, j);

                if(isEqual(testKey1, buffer))
                {
                    emitter.emit(key1, 1);
                }

                if(isEqual(testKey2, buffer))
                {
                    emitter.emit(key2, 1);
                }

                if(isEqual(testKey3, buffer))
                {
                    emitter.emit(key3, 1);
                }

                if(isEqual(testKey4, buffer))
                {
                    emitter.emit(key4, 1);
                }

                while((i < length) && (keys[i] == '\r' || keys[i] == '\n')) {
                    i++;
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

    public long run(List<String> input, int parallelism) throws Exception {
        return run( input, parallelism, false);
    }

    public long run(List<String> input, int parallelism, boolean verbose) throws Exception {
        long startTime = System.currentTimeMillis();

        List<KeyValue<String, Integer>> result = mrj.run(input, parallelism);

        long stopTime = System.currentTimeMillis();

        if (verbose) {
            System.out.println("STRING MATCH");
            for (KeyValue<String, Integer> kv : result) {
                System.out.printf("%12s %d%n", kv.getKey(), kv.getValue());
            }
            System.out.println("          in " + (stopTime - startTime));
        }

        return stopTime - startTime;
    }

    private char[] hash(String input) {
        char[] array = input.toCharArray();
        hash(array, array.length);
        return array;
    }
    
    private void hash(char[] input, int length) {
        for (int i = 0; i < length; i++) {
            input[i] += offset;
        }
    }
    
    private boolean isEqual(char[] key, char[] buffer) {
        for (int i = 0; i < key.length; i++) {
            if (key[i] != buffer[i]) {
                return false;
            }
        }
        return true;
    }

    public static void main(String[] args) {
        try {
            int parallelism = Integer.parseInt(args[0]);

            int bufferSize = Integer.parseInt(args[1]);

            String inputFile = args[2];

            boolean verbose = args.length > 3;

            // ----- MAP REDUCE EXECUTION -----
            
            StringMatch sm = new StringMatch();

            sm.run(Splitter.fileToStringBuffers(inputFile, bufferSize), parallelism, verbose);
            
            //-------------- END --------------
            
        } catch (Exception e) {
            System.out.println("USAGE: <threads> <input file> [<verbose>]");
        }
    }
}
