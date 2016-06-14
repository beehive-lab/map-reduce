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
import java.util.concurrent.atomic.AtomicInteger;
import uk.ac.man.cs.mapreduce.*;

public class LinearRegression {

    private AtomicInteger sizeOfInput = new AtomicInteger(0);
    
    public static enum Key {

        X, Y, XX, YY, XY, NUM
    };

    public final MapReduce<byte[], Key, Long> mrj;

    public LinearRegression() {
        mrj = new MapReduce<>(mapper, reducer);
    }
    
    private Mapper<byte[], Key, Long> mapper = new Mapper<byte[], Key, Long>() {
        
        @Override
        public void map(byte[] input, Emitter<Key, Long> emitter) {
            
            sizeOfInput.addAndGet(input.length);
            
            long sx = 0;
            long sy = 0;
            long sxx = 0;
            long syy = 0;
            long sxy = 0;

            int length = input.length & 0xFFFFFFFE;

            for (int i = 0; i < length; i += 2) {
                long x = input[i];
                long y = input[i + 1];

                sx += x;
                sxx += x * x;
                sy += y;
                syy += y * y;
                sxy += x * y;
            }

            emitter.emit(Key.X, sx);
            emitter.emit(Key.Y, sy);
            emitter.emit(Key.XX, sxx);
            emitter.emit(Key.YY, syy);
            emitter.emit(Key.XY, sxy);
            emitter.emit(Key.NUM, new Long(length / 2));
        }
    };
    
    private Reducer<Key, Long> reducer = new Reducer<Key, Long>() {
        
        @Override
        public void reduce(Key key, List<Long> values, Emitter<Key, Long> emitter) {
            long sum = 0;

            for (Long l : values) {
                sum += l;
            }

            emitter.emit(key, sum);
        }
    };
    
    public long run(List<byte[]> input, int parallelism) throws Exception {
        return run(input, parallelism, false);
    }

    public long run(List<byte[]> input, int parallelism, boolean verbose) throws Exception {
        long startTime = System.currentTimeMillis();

        double sumX = 0, sumY = 0, sumXX = 0, sumYY = 0, sumXY = 0, n = 0;

        for (KeyValue<Key, Long> kvp : mrj.run(input, parallelism)) {
            switch (kvp.getKey()) {
                case X:
                    sumX = kvp.getValue();
                    break;
                case Y:
                    sumY = kvp.getValue();
                    break;
                case XX:
                    sumXX = kvp.getValue();
                    break;
                case YY:
                    sumYY = kvp.getValue();
                    break;
                case XY:
                    sumXY = kvp.getValue();
                    break;
                case NUM:
                    n = kvp.getValue();
                default:
                    break;
            }
        }

        double b = ((n * sumXY) - (sumX * sumY))
                / ((n * sumXX) - (sumX * sumX));

        double a = (sumY - (b * sumX)) / n;

        double xBar = sumX / n;

        double yBar = sumY / n;

        double r2 = ((n * sumXY) - (sumX * sumY)) * ((n * sumXY) - (sumX * sumY))
                / (((n * sumXX) - (sumX * sumX)) * ((n * sumYY) - (sumY * sumY)));

        long stopTime = System.currentTimeMillis();

        System.out.println("Size = " + sizeOfInput.get());
        
        if (verbose) {
            System.out.printf("LINEAR RECURCION\n");
            System.out.printf("     RESULTS\n");
            System.out.printf("         a = %.3f\n", a);
            System.out.printf("         b = %.3f\n", b);
            System.out.printf("      xBar = %.3f\n", xBar);
            System.out.printf("      yBar = %.3f\n", yBar);
            System.out.printf("        r2 = %.3f\n", r2);
            System.out.printf("      sumX = %.0f\n", sumX);
            System.out.printf("      sumY = %.0f\n", sumY);
            System.out.printf("     sumXX = %.0f\n", sumXX);
            System.out.printf("     sumYY = %.0f\n", sumYY);
            System.out.printf("     sumXY = %.0f\n", sumXY);
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

            LinearRegression lr = new LinearRegression();

            lr.run(Splitter.fileToByteBuffers(inputFile, bufferSize), parallelism, verbose);

            //-------------- END --------------

        } catch (Exception e) {
            System.out.println("USEAGE: <threads> <input file> [<verbose>]");
            e.printStackTrace();
        }
    }
}
