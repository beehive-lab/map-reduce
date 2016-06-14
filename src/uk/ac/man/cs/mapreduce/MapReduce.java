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
package uk.ac.man.cs.mapreduce;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveAction;
import java.util.concurrent.RecursiveTask;

public class MapReduce<I, K, V> {

    private final Mapper<I, K, V> mapper;
    
    private final Reducer<K, V> reducer;
    
    public MapReduce(Mapper<I, K, V> mapper, Reducer<K, V> reducer) {
        this.mapper = mapper;
        this.reducer = reducer;
    }

    public List<KeyValue<K, V>> run(List<I> inputs, int parallelism) {
        if (reducer.isCombinable()) {
            return runWithCombiner(inputs, parallelism);
        } else {
            return runWithReducer(inputs, parallelism);
        }
    }

    public List<KeyValue<K, V>> run(List<I> inputs, int parallelism, Comparator<KeyValue<K, V>> comparator) {
        return run(inputs, parallelism);
    }
    
    private List<KeyValue<K, V>> runWithReducer(List<I> inputs, int parallelism) {
        final ForkJoinPool fjp = new ForkJoinPool(parallelism);
        
        final ConcurrentMap<K, List<V>> collector = new ConcurrentHashMap<>();
             
        final Emitter<K, V> mapEmitter = (key, value) -> {
            List<V> values = collector.get(key);

            if (values == null) {
                List<V> list = new ArrayList<>();
                values = collector.putIfAbsent(key, list);
                if (values == null) {
                    values = list;
                }
            }
            
            synchronized (values) {
                values.add(value);
            }
        };
        
        int mapGranularity = Math.max(1, inputs.size() / (parallelism << 4));

        fjp.invoke(new MapRunner(inputs, mapEmitter, mapGranularity, 0, inputs.size()));

        Entry<K, List<V>>[] intermediates = (Entry<K, List<V>>[]) collector.entrySet().toArray(new Entry[0]);
        
        int reduceGranularity = Math.max(1, intermediates.length / (parallelism << 4));

        List<KeyValue<K, V>> results = (List<KeyValue<K, V>>) fjp.invoke(new ReduceRunner(intermediates, reduceGranularity, 0, intermediates.length));
        
        return results;
    }
    
    private List<KeyValue<K, V>> runWithCombiner(List<I> inputs, int parallelism) {
        final ForkJoinPool fjp = new ForkJoinPool(parallelism);
        
        final ConcurrentMap<K, Holder> collector = new ConcurrentHashMap<>();
             
        final Emitter<K, V> mapEmitter = (key, value) -> {
            Holder intermediate = collector.get(key);

            if (intermediate == null) {
                Holder temp = reducer.initialise();
                intermediate = collector.putIfAbsent(key, temp);
                if (intermediate == null) {
                    intermediate = temp;
                }
            }
            
            synchronized (intermediate) {
                reducer.combine(intermediate, value);
            }
        };
        
        int mapGranularity = Math.max(1, inputs.size() / (parallelism << 4));

        fjp.invoke(new MapRunner(inputs, mapEmitter, mapGranularity, 0, inputs.size()));
        
        List<KeyValue<K, V>> results = new ArrayList<>(collector.size());
        
        // Is it worth while parallelising this?
        for (Entry<K, Holder> kv : collector.entrySet()) {
            results.add(new KeyValue<>(kv.getKey(), reducer.getResult(kv.getValue())));
        }
        
        return results;
    }

    private class MapRunner extends RecursiveAction {

        private final int granularity;

        private final List<I> input;

        private final Emitter<K, V> emitter;

        private final int lo, hi;

        private MapRunner(
                List<I> input, Emitter<K, V> emitter, 
                int granularity, int lo, int hi) {
            this.input = input;
            this.emitter = emitter;
            this.lo = lo;
            this.hi = hi;
            this.granularity = granularity;
        }        

        @Override
        protected void compute() {
            int inputCount = hi - lo;

            if (inputCount > granularity) {
                int mi = lo + (inputCount >> 1);
                invokeAll(
                        new MapRunner(input, emitter, granularity, lo, mi),
                        new MapRunner(input, emitter, granularity, mi, hi));
            } else {
                List<I> sublist = input.subList(lo, hi);
                for (I i : sublist) {
                    mapper.map(i, emitter);
                }
            }
        }
    }

    private class ReduceRunner extends RecursiveTask<List<KeyValue<K, V>>> {

        private final int granularity;

        private Entry<K, List<V>>[] intermediates;

        private int lo, hi;

        public ReduceRunner(
                Entry<K, List<V>>[] intermediates,
                int granularity, int lo, int hi) {
            this.intermediates = intermediates;
            this.granularity = granularity;
            this.lo = lo;
            this.hi = hi;
        }

        @Override
        public List<KeyValue<K, V>> compute() {
            int inputCount = hi - lo;
            
            final List<KeyValue<K, V>> results
                    = new ArrayList<>(inputCount);
            
            if (inputCount > granularity) {
                int mi = lo + (inputCount >> 1);
                ReduceRunner runner1 =
                        new ReduceRunner(intermediates, granularity, lo, mi);
                runner1.fork();
                ReduceRunner runner2 =
                        new ReduceRunner(intermediates, granularity, mi, hi);
                results.addAll(runner2.compute());
                results.addAll(runner1.join());
            } else {
                Emitter<K, V> emitter = new Emitter<K, V>() {
                    @Override
                    public void emit(K key, V value) {
                        results.add(new KeyValue<>(key, value));
                    }
                };

                for (int i = lo; i < hi; i++) {
                    reducer.reduce(
                            intermediates[i].getKey(),
                            intermediates[i].getValue(),
                            emitter);
                }
            }

            return results;
        }
    }
}

