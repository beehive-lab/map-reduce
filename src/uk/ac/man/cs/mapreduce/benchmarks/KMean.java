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
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import uk.ac.man.cs.mapreduce.*;

public class KMean {

    private Point[] means;

    private final MapReduce<Point, Integer, Point> mrj;

    private Integer[] indices;

    public KMean() {
        mrj = new MapReduce<>(mapper, reducer);
    }

    private Mapper<Point, Integer, Point> mapper = new Mapper<Point, Integer, Point>() {

        @Override
        public void map(Point input, Emitter<Integer, Point> emitter) {
            int closestMean = 0, closestDistance = Integer.MAX_VALUE;

            for (int i = 0; i < means.length; i++) {
                int distance = input.getDistance(means[i]);

                if (distance < closestDistance) {
                    closestMean = i;
                    closestDistance = distance;
                }
            }

            emitter.emit(indices[closestMean], input);
        }
    };

    private Reducer<Integer, Point> reducer = new Reducer<Integer, Point>() {

        @Override
        public void reduce(Integer key, List<Point> values, Emitter<Integer, Point> emitter) {
            AveragingPoint intermediate = new AveragingPoint();

            for (Point p : values) {
                intermediate.addPoint(p);
            }
            
            emitter.emit(key, intermediate.getAverage());
        }
    };

    public long run(int dimensions, int clusters, int gridSize, int points, int parallelism) throws Exception {
        return run(dimensions, clusters, gridSize, points, parallelism, false);
    }

    public long run(int dimensions, int clusters, int gridSize, int points, int parallelism, boolean verbose) throws Exception {
        means = new Point[clusters];
        indices = new Integer[clusters];
        
        for (int i = 0; i < clusters; i++) {
            means[i] = new Point(dimensions, gridSize);
            indices[i] = i;
        }

        List<Point> input = new ArrayList<>(points);
        for (int i = 0; i < points; i++) {
            input.add(new Point(dimensions, gridSize));
        }

        boolean modified = true;

        int iterations = 0;

        long startTime = System.currentTimeMillis();

        while (modified) {
            iterations++;

            modified = false;

            for (KeyValue<Integer, Point> kvp : mrj.run(input, parallelism)) {
                int i = kvp.getKey();
                Point p = kvp.getValue();

                if (!p.equals(means[i])) {
                    modified = true;
                    means[i] = p;
                }
            }
        }

        long stopTime = System.currentTimeMillis();

        if (verbose) {
            System.out.println("   KMEANS ON");
            System.out.println("    points = " + points);
            System.out.println("      grid = " + gridSize);
            System.out.println("dimensions = " + dimensions);
            System.out.println("  clusters = " + clusters);
            System.out.println("ITERATIONS = " + iterations);
            System.out.printf("          in %-5d (%3d)\n", (stopTime - startTime), (stopTime - startTime) / iterations);
        }

        return (stopTime - startTime) / iterations;
    }

    private class AveragingPoint {
        private long[] position = null;
        private int points = 0;
        
        public AveragingPoint() {
        }
        
        public void addPoint(Point p) {
            if (position == null) {
                position = new long[p.getDimensions()];
            }
            int[] pPosition = p.getPosition();
            for (int i = 0; i < pPosition.length; i++) {
                position[i] += pPosition[i];
            }
            points++;
        }
        
        public Point getAverage() {
            if (position == null) {
                return null;
            }
            int[] pPosition = new int[position.length];
            for (int i = 0; i < position.length; i++) {
                pPosition[i] += position[i] / points;
            }
            return new Point(pPosition);
        }
    }
    
    private class Point {

        private int[] position;

        protected Point(int dimensions, int gridSize) {
            Random r = new Random();
            position = new int[dimensions];
            for (int i = 0; i < position.length; i++) {
                position[i] = r.nextInt(gridSize);
            }
        }

        protected Point(int[] position) {
            this.position = position;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || obj.getClass() != this.getClass()) {
                return false;
            }
            Point point = (Point) obj;
            return Arrays.equals(this.position, point.position);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(position);
        }

        protected int getDimensions() {
            return position.length;
        }

        protected int[] getPosition() {
            return position;
        }

        protected void setPosition(int[] position) {
            this.position = position;
        }

        protected int getDistance(Point point) {
            int distance = 0;

            for (int i = 0; i < getDimensions(); i++) {
                int p1 = this.position[i];
                int p2 = point.position[i];

                distance += (p1 - p2) * (p1 - p2);
            }

            return distance;
        }
    }

    public static void main(String[] args) {
        try {
            int parallelism = Integer.decode(args[0]);

            int dimensions = Integer.decode(args[1]);

            int clusters = Integer.decode(args[2]);

            int gridSize = Integer.decode(args[3]);

            int points = Integer.decode(args[4]);

            boolean verbose = args.length > 5;

            // ----- MAP REDUCE EXECUTION -----
            KMean km = new KMean();

            km.run(dimensions, clusters, gridSize, points, parallelism, verbose);

            //-------------- END --------------
        } catch (Exception ignore) {
            System.out.println("USEAGE:  <threads> <dimensions> <clusters> <grid size> <points> [<verbose>]");
        }
    }
}
