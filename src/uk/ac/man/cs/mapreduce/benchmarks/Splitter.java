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

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class Splitter {

    public static List<String> fileToStringBuffers(String filename, int bufferSize)
            throws FileNotFoundException, IOException {
        List<String> strings = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(filename))) {
            StringBuilder str = new StringBuilder();
            String line;
            int lines = 0;
            while ((line = reader.readLine()) != null) {
                lines++;
                if ((str.length() + line.length()) > bufferSize) {
                    strings.add(str.toString());
                    str.delete(0, str.length());
                }

                str.append(line).append('\n');
            }
            if (str.length() > 0) {
                strings.add(str.toString());
            }
        }
        return strings;
    }

    public static List<String> fileToStrings(String filename)
            throws FileNotFoundException, IOException {
        List<String> strings = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(filename))) {
            String line;
            while ((line = reader.readLine()) != null) {
                strings.add(line);
            }
        }
        return strings;
    }

    public static List<String[]> fileToLineBuffer(String filename, int bufferSize)
            throws FileNotFoundException, IOException {
        ArrayList<String[]> buffer = new ArrayList<>();;
        try (BufferedReader reader = new BufferedReader(new FileReader(filename))) {
            ArrayList<String> lines = new ArrayList<>();
            int size = 0;
            String line;
            while ((line = reader.readLine()) != null) {
                if (size + line.length() > bufferSize) {
                    buffer.add(lines.toArray(new String[0]));
                    lines.clear();
                    size = 0;
                } else {
                    lines.add(line);
                }
            }
            if (lines.size() > 0) {
                buffer.add(lines.toArray(new String[0]));
            }
        }
        return buffer;
    }

    public static List<byte[]> fileToByteBuffers(String filename, int bufferSize)
            throws FileNotFoundException, IOException {
        return fileToByteBuffers(filename, bufferSize, 0);
    }

    public static List<byte[]> fileToByteBuffers(String filename, int bufferSize, int offset)
            throws FileNotFoundException, IOException {
        ArrayList<byte[]> buffers = new ArrayList<>();
        try (FileInputStream reader = new FileInputStream(filename)) {
            byte[] buffer;
            reader.skip(offset);
            for (;;) {
                buffer = new byte[bufferSize];

                int count = reader.read(buffer);

                if (count != bufferSize) {
                    byte[] reducedBuffer = new byte[count];

                    System.arraycopy(buffer, 0, reducedBuffer, 0, count);
                    buffers.add(reducedBuffer);

                    break;
                } else {
                    buffers.add(buffer);
                }
            }
        }
        return buffers;
    }
}

