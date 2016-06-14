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

public class MatrixMultiply {

    private static class Task {

        private final int[] aRow, cRow;
        private final int[][] b;

        protected Task(int[] aRow, int[][] b, int[] cRow) {
            this.aRow = aRow;
            this.b = b;
            this.cRow = cRow;
        }
    }    
    
    private final MapReduce<Task, Object, Integer> mrj;
    private static final Object SUM = new Object();

    public MatrixMultiply() {
        mrj = new MapReduce<>(mapper, reducer);
    }
    
    private final Mapper<Task, Object, Integer> mapper = new Mapper<Task, Object, Integer>() {

        @Override
        public void map(Task input, Emitter<Object, Integer> emitter) {
            int[] aRow = input.aRow;
            int[][] b = input.b;
            int[] cRow = input.cRow;
            int size = aRow.length;

            for (int i = 0; i < size; i++) {
                int aI = aRow[i];
                int[] bRow = b[i];
                for (int j = 0; j < size; j++) {
                    cRow[j] += aI * bRow[j];
                }
            }

            int sum = 0;
            for (int i = 0; i < size; i++) {
                sum += cRow[i];
            }
            emitter.emit(SUM, sum);
        }
    };
    
    private final Reducer<Object, Integer> reducer = new Reducer<Object, Integer>() {

        @Override
        public void reduce(Object key, List<Integer> values, Emitter<Object, Integer> emitter) {
            int sum = 0;
            for (Integer s : values) {
                sum += s;
            }
            emitter.emit(key, sum);
        }
    };

    private int[][] generateMatrix(int matrixSize) {
        Random random = new Random();
        int[][] matrix = new int[matrixSize][];
        for (int i = 0; i < matrixSize; i++) {
            matrix[i] = new int[matrixSize];
            for (int j = 0; j < matrixSize; j++) {
                matrix[i][j] = random.nextInt(10);
            }
        }
        return matrix;
    }
    
    public long run(int matrixSize, int parallelism) throws Exception {
        return run(matrixSize, parallelism, false);
    }
    
    public long run(int matrixSize, int parallelism, boolean verbose) throws Exception {
        int[][] A = generateMatrix(matrixSize);
        int[][] B = generateMatrix(matrixSize);
        int[][] C = new int[matrixSize][matrixSize];

        List<Task> input = new ArrayList<>(matrixSize);

        for (int i = 0; i < matrixSize; i++) {
            input.add(new Task(A[i], B, C[i]));
        }

        long startTime = System.currentTimeMillis();

        List<KeyValue<Object, Integer>> result = mrj.run(input, parallelism);

        long stopTime = System.currentTimeMillis();

        if (verbose) {
            System.out.println("MATRIX MULTIPLICATION - " + matrixSize + "x" + matrixSize);
            if (result.size() == 1 && result.get(0).getKey() == SUM) {
                System.out.println("         sum " + result.get(0).getValue());
            }
            System.out.println("          in " + (stopTime - startTime));
        }

        return stopTime - startTime;
    }

    public static void main(String[] args) {
        try {
            int parallelism = Integer.decode(args[0]);

            int matrixSize = Integer.decode(args[1]);

            boolean verbose = args.length > 2;

            // ----- MAP REDUCE EXECUTION -----            

            MatrixMultiply mm = new MatrixMultiply();

            mm.run(matrixSize, parallelism, verbose);

            //-------------- END --------------

        } catch (Exception ignore) {
            System.out.println("USEAGE: <threads> <matrix size> [<verbose>]");
        }
    }
}
