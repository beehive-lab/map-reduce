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

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import uk.ac.man.cs.mapreduce.*;

public class PCA {

    private abstract class Data {
    }

    private class MeanData extends Data {

        protected int rowNumber;
        protected int[] row;

        protected MeanData(int rowNumber, int[] row) {
            this.rowNumber = rowNumber;
            this.row = row;
        }
    }

    private class CovarianceData extends Data {

        public int rowNumber, covNumber;
        public int[] means;
        public int[][] matrix;

        protected CovarianceData(int rowNumber, int covNumber, int[] means, int[][] matrix) {
            this.rowNumber = rowNumber;
            this.covNumber = covNumber;
            this.means = means;
            this.matrix = matrix;
        }
    };
    
    private MapReduce<Data, Integer, Integer> mrj;

    public PCA() {
    }
    
    private Mapper<Data, Integer, Integer> meanMapper = new Mapper<Data, Integer, Integer>() {
        
        @Override
        public void map(Data input, Emitter<Integer, Integer> emitter) {
            if (input instanceof MeanData) {
                MeanData meanInput = (MeanData) input;

                long sum = 0;

                for (int i : meanInput.row) {
                    sum += i;
                }

                emitter.emit(meanInput.rowNumber, (int) (sum / (long) meanInput.row.length));
            }
        }
    };
    
    private Mapper<Data, Integer, Integer> covarianceMapper = new Mapper<Data, Integer, Integer>() {
        
        @Override
        public void map(Data input, Emitter<Integer, Integer> emitter) {
            if (input instanceof CovarianceData) {
                CovarianceData covInput = (CovarianceData) input;

                int sum = 0;
                int[] row = covInput.matrix[covInput.rowNumber];
                int[] cov = covInput.matrix[covInput.covNumber];
                int rowMean = covInput.means[covInput.rowNumber];
                int covMean = covInput.means[covInput.covNumber];

                for (int j = 0; j < row.length; j++) {
                    sum += (row[j] - rowMean) * (cov[j] - covMean);
                }

                sum /= covInput.matrix[covInput.rowNumber].length - 1;

                emitter.emit(covInput.rowNumber * row.length + covInput.covNumber, sum);
            }
        }
    };
    
    private Reducer<Integer, Integer> reducer = new Reducer<Integer, Integer>() {
        
        @Override
        public void reduce(Integer key, List<Integer> values, Emitter<Integer, Integer> emitter) {
            emitter.emit(key, values.get(0));
        }
    };

    public long run(int rows, int columns, int gridSize, int parallelism) throws Exception {
        return run(rows, columns, gridSize, parallelism, false);
    }
    
    public long run(int rows, int columns, int gridSize, int parallelism, boolean verbose) throws Exception {
        Random random = new Random();

        int[][] matrix = new int[rows][columns];
        int[] means = new int[rows];

        for (int i = 0; i < rows; i++) {
            for (int j = 0; j < columns; j++) {
                matrix[i][j] = random.nextInt(gridSize);
            }
        }

        List<Data> input;

        List<KeyValue<Integer, Integer>> output;

        long startTime = System.currentTimeMillis();

        /* Split to calculate means */

        input = new ArrayList<>(rows);

        for (int i = 0; i < rows; i++) {
            input.add(new MeanData(i, matrix[i]));
        }

        mrj = new MapReduce<>(meanMapper, reducer);

        output = mrj.run(input, parallelism);

        for (KeyValue<Integer, Integer> kvp : output) {
            means[kvp.getKey()] = kvp.getValue();
        }

        /* Split to calculate covariance */

        int k = 0;

        input = new ArrayList<>((((rows * rows) - rows) / 2) + rows);

        for (int i = 0; i < rows; i++) {
            for (int j = i; j < columns; j++) {
                input.add(new CovarianceData(i, j, means, matrix));
            }
        }

        mrj = new MapReduce<>(covarianceMapper, reducer);

        output = mrj.run(input, parallelism);

        long sum = 0;

        for (KeyValue<Integer, Integer> kvp : output) {
            sum += kvp.getValue();
        }

        long stopTime = System.currentTimeMillis();

        if (verbose) {
            System.out.println("PRINCIPLE COMPONENT ANALYSIS - " + rows + "x" + columns);
            System.out.println("         SUM " + sum);
            System.out.println("          in " + (stopTime - startTime));
        }

        return stopTime - startTime;
    }

    public static void main(String[] args) {
        try {
            int parallelism = Integer.decode(args[0]);

            int rows = Integer.decode(args[1]);

            int columns = Integer.decode(args[2]);

            int gridSize = Integer.decode(args[3]);

            boolean verbose = args.length > 4;

            // ----- MAP REDUCE EXECUTION -----            

            PCA pca = new PCA();

            pca.run(rows, columns, gridSize, parallelism, verbose);

            //-------------- END --------------

        } catch (Exception ignore) {
            System.out.println("USEAGE: <threads> <rows> <columns> <grid size> [<verbose>]");
        }
    }
}
