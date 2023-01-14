///*
// * Copyright 2020 LINE Corporation
// *
// * LINE Corporation licenses this file to you under the Apache License,
// * version 2.0 (the "License"); you may not use this file except in compliance
// * with the License. You may obtain a copy of the License at:
// *
// *   https://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// * License for the specific language governing permissions and limitations
// * under the License.
// */
//
//package com.linecorp.decaton.processor.runtime;
//
//import static java.util.concurrent.TimeUnit.SECONDS;
//
//import java.io.RandomAccessFile;
//import java.nio.MappedByteBuffer;
//import java.nio.channels.FileChannel.MapMode;
//import java.nio.file.Files;
//import java.nio.file.Path;
//
//import org.openjdk.jmh.annotations.Benchmark;
//import org.openjdk.jmh.annotations.BenchmarkMode;
//import org.openjdk.jmh.annotations.Fork;
//import org.openjdk.jmh.annotations.Level;
//import org.openjdk.jmh.annotations.Measurement;
//import org.openjdk.jmh.annotations.Mode;
//import org.openjdk.jmh.annotations.OperationsPerInvocation;
//import org.openjdk.jmh.annotations.OutputTimeUnit;
//import org.openjdk.jmh.annotations.Scope;
//import org.openjdk.jmh.annotations.Setup;
//import org.openjdk.jmh.annotations.State;
//import org.openjdk.jmh.annotations.TearDown;
//import org.openjdk.jmh.annotations.Threads;
//import org.openjdk.jmh.annotations.Warmup;
//
//@BenchmarkMode(Mode.Throughput)
//@OutputTimeUnit(SECONDS)
//@Threads(1)
//@Fork(1)
//@Warmup(iterations = 5, time = 1, timeUnit = SECONDS)
//@Measurement(iterations = 10, time = 1, timeUnit = SECONDS)
//public class IndexBenchmark {
//    private static final int SIZE = 1024 * 1024 * 10;
//    private static final int ENTRY_SIZE = 8 + 4;
//    private static final int ENTRIES = SIZE / ENTRY_SIZE;
//
//    @State(Scope.Thread)
//    public static class FbbfState {
//        Path path;
//        FileByteBuffer buffer;
//
//        @Setup(Level.Trial)
//        public void setup() throws Exception {
//            path = Files.createTempFile("index-", ".bin");
//        }
//
//        @Setup(Level.Invocation)
//        public void initialize() throws Exception {
//            RandomAccessFile raf = new RandomAccessFile(path.toFile(), "rw");
//            buffer = new FileByteBuffer(raf);
//
//            raf.setLength(SIZE);
//            buffer.updateLimit();
//        }
//
//        @TearDown(Level.Trial)
//        public void tearDown() throws Exception {
//            buffer.close();
//            Files.deleteIfExists(path);
//        }
//    }
//
//    @State(Scope.Thread)
//    public static class MmapState {
//        Path path;
//        MappedByteBuffer buffer;
//
//        @Setup(Level.Trial)
//        public void setup() throws Exception {
//            path = Files.createTempFile("index-", ".bin");
//        }
//
//        @Setup(Level.Invocation)
//        public void initialize() throws Exception {
//            try (RandomAccessFile raf = new RandomAccessFile(path.toFile(), "rw")) {
//                raf.setLength(SIZE);
//                buffer = raf.getChannel().map(MapMode.READ_WRITE, 0, SIZE);
//            }
//        }
//
//        @TearDown(Level.Trial)
//        public void tearDown() throws Exception {
//            Files.deleteIfExists(path);
//        }
//    }
//
//    @Benchmark
//    @OperationsPerInvocation(ENTRIES)
//    public void fbbf(FbbfState state) throws Exception {
//        for (int i = 0; i < ENTRIES; i++) {
//            state.buffer.putLong(42L);
//            state.buffer.putInt(i);
//        }
//    }
//
//    @Benchmark
//    @OperationsPerInvocation(ENTRIES)
//    public void mmap(MmapState state) throws Exception {
//        for (int i = 0; i < ENTRIES; i++) {
//            state.buffer.putLong(42L);
//            state.buffer.putInt(i);
//        }
//    }
//}
