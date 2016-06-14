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

import java.io.File;
import java.io.FileInputStream;
import java.util.Comparator;
import java.util.List;
import uk.ac.man.cs.mapreduce.*;

public class Histogram {

    public enum Colour {

        RED, GREEN, BLUE
    };
    private static final int FILE_TYPE_OFFSET = 0;
    private static final int DATA_POSITION_OFFSET = 10;
    private static final int BITS_PER_PIXEL_OFFSET = 28;
    
    private final MapReduce<byte[], Pixel, Integer> mrj;
    
    private Pixel[] redPixels = new Pixel[256];
    private Pixel[] greenPixels = new Pixel[256];
    private Pixel[] bluePixels = new Pixel[256];

    public Histogram() {
        mrj = new MapReduce<>(mapper, reducer);

        for (int i = 0; i < 256; i++) {
            redPixels[i] = new Pixel(Colour.RED, i);
            greenPixels[i] = new Pixel(Colour.GREEN, i);
            bluePixels[i] = new Pixel(Colour.BLUE, i);
        }
    }
    
    private Mapper<byte[], Pixel, Integer> mapper = new Mapper<byte[], Pixel, Integer>() {
        @Override
        public void map(byte[] input, Emitter<Pixel, Integer> emitter) {
            int[] redValues = new int[256];
            int[] greenValues = new int[256];
            int[] blueValues = new int[256];

            int length = input.length - (input.length % 3);

            for (int i = 0; i < length; i += 3) {
                blueValues[(int) input[i] & 0xFF]++;
                greenValues[(int) input[i + 1] & 0xFF]++;
                redValues[(int) input[i + 2] & 0xFF]++;
            }

            for (int i = 0; i < 256; i++) {
                emitter.emit(bluePixels[i], blueValues[i]);
                emitter.emit(greenPixels[i], greenValues[i]);
                emitter.emit(redPixels[i], redValues[i]);
            }
        }
    };
    
    private Reducer<Pixel, Integer> reducer = new Reducer<Pixel, Integer>() {
        @Override
        public void reduce(Pixel key, List<Integer> values, Emitter<Pixel, Integer> emitter) {
            int sum = 0;

            for (Integer i : values) {
                sum += i;
            }

            emitter.emit(key, sum);
        }
    };
    
    private final Comparator<KeyValue<Pixel, Integer>> sorter = (kvp1, kvp2) ->
            kvp2.getValue().compareTo(kvp1.getValue());

    public long run(List<byte[]> input, int parallelism) throws Exception {
        return run(input, parallelism, false);
    }
    
    public long run(List<byte[]> input, int parallelism, boolean verbose) throws Exception {
        long startTime = System.currentTimeMillis();

        List<KeyValue<Pixel, Integer>> results = mrj.run(input, parallelism);

        long stopTime = System.currentTimeMillis();

        if (verbose) {
            System.out.println("HISTOGRAM");

            for (int i = 0; i < Math.min(10, results.size()); i++) {
                System.out.printf("%12d %s\n", results.get(i).getValue(), results.get(i).getKey());
            }

            System.out.println("          in " + (stopTime - startTime));
        }

        return (stopTime - startTime);
    }

    public static int getBitmapDataOffset(String filename) throws Exception {
        File image = new File(filename);

        if (!image.exists() || !image.isFile() || !image.canRead()) {
            throw new Exception("File does not exist or cannot be read");
        }

        long imageLength = image.length();

        byte[] header = new byte[BITS_PER_PIXEL_OFFSET + 2];

        FileInputStream reader = new FileInputStream(image);
        int bytesRead = reader.read(header);

        if (bytesRead != header.length) {
            throw new Exception("Unable to read file");
        }

        int fileType = (((int) header[FILE_TYPE_OFFSET + 1] & 0xFF) << 8) + ((int) header[FILE_TYPE_OFFSET] & 0xFF);

        if (fileType != 0x4D42) { /*
             * BM
             */
            throw new Exception("Invalid file format");
        }

        int bitsPerPixel = (((int) header[BITS_PER_PIXEL_OFFSET + 1] & 0xFF) << 8) + ((int) header[BITS_PER_PIXEL_OFFSET] & 0xFF);

        if (bitsPerPixel != 24) {
            throw new Exception("Invalid pixel size for benchmark " + bitsPerPixel);
        }

        int dataOffset = (((int) header[DATA_POSITION_OFFSET + 1] & 0xFF) << 8) + ((int) header[DATA_POSITION_OFFSET] & 0xFF);

        return dataOffset;
    }

    public static void main(String[] args) throws Exception {
        try {
            int parallelism = Integer.decode(args[0]);

            String inputFile = args[1];

            int bufferSize = Integer.decode(args[2]);

            while ((bufferSize % 3) > 0) {
                bufferSize++;
            }

            boolean verbose = args.length > 3;

            int dataOffset = getBitmapDataOffset(inputFile);

            // ----- MAP REDUCE EXECUTION -----

            Histogram hist = new Histogram();

            hist.run(Splitter.fileToByteBuffers(inputFile, bufferSize, dataOffset), parallelism, verbose);

            //-------------- END --------------

        } catch (Exception ignore) {
            System.out.println("USEAGE: <threads> <input file> <buffer size> [<verbose>]");
        }
    }

    private class Pixel {

        private final Colour colour;
        private final int value;

        protected Pixel(Colour colour, int value) {
            this.colour = colour;
            this.value = value;
        }

        protected Colour getColour() {
            return colour;
        }

        protected int getValue() {
            return value;
        }

        @Override
        public String toString() {
            return String.format("%s:%d", colour.name(), value);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) {
                return true;
            }
            if ((obj == null) || (obj.getClass() != this.getClass())) {
                return false;
            }
            Pixel pixel = (Pixel) obj;
            return (pixel.colour == this.colour) & (pixel.value == this.value);
        }

        @Override
        public int hashCode() {
            return colour.hashCode() ^ value;
        }
    }
}
