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
//import java.util.ArrayDeque;
//import java.util.Deque;
//
//import org.apache.kafka.common.TopicPartition;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import com.linecorp.decaton.processor.DeferredCompletion;
//
///**
// * Represents consumption processing progress of records consumed from a single partition.
// * This class manages sequence of offsets and a flag which represents if each of them was completed or not.
// */
//public class OutOfOrderCommitControl {
//    private static final Logger logger = LoggerFactory.getLogger(OutOfOrderCommitControl.class);
//
//    static class OffsetState {
//        private final long offset;
//        private volatile boolean committed;
//
//        OffsetState(long offset, boolean committed) {
//            this.offset = offset;
//            this.committed = committed;
//        }
//
//        @Override
//        public String toString() {
//            return "OffsetState{" +
//                   "offset=" + offset +
//                   ", committed=" + committed +
//                   '}';
//        }
//    }
//
//    private final TopicPartition topicPartition;
//    private final int capacity;
//    private final Deque<OffsetState> states;
//
//    /**
//     * The current smallest offset that has been reported but not completed.
//     */
//    private volatile long earliest;
//    /**
//     * The current largest offset that has been reported.
//     */
//    private volatile long latest;
//    /**
//     * The current maximum offset which it and all it's previous offsets were committed.
//     */
//    private volatile long highWatermark;
//
//    public OutOfOrderCommitControl(TopicPartition topicPartition, int capacity) {
//        this.topicPartition = topicPartition;
//        states = new ArrayDeque<>(capacity);
//        this.capacity = capacity;
//        earliest = latest = 0;
//        highWatermark = -1;
//    }
//
//    public TopicPartition topicPartition() {
//        return topicPartition;
//    }
//
//    public synchronized DeferredCompletion reportFetchedOffset(long offset) {
//        if (isRegressing(offset)) {
//            throw new IllegalArgumentException(String.format(
//                    "offset regression %s: %d > %d", topicPartition, offset, latest));
//        }
//
//        if (states.size() == capacity) {
//            throw new IllegalArgumentException(
//                    String.format("offsets count overflow: cap=%d, offset=%d", capacity, offset));
//        }
//
//        OffsetState state = new OffsetState(offset, false);
//        states.addLast(state);
//        latest = state.offset;
//
//        return () -> complete(state);
//    }
//
//    /**
//     * Mark given offset stored in given {@link OffsetState} as commit-ready.
//     *
//     * To maximize performance of offset management represented by this class, we optimized implementation to
//     * be lock-free for managing offset states among multiple threads.
//     *
//     * To make this happen, the core data store {@link #states} has made to prohibit concurrent access, by
//     * making sure that all places that touches {@link #states} to be accessed only by the single thread -
//     * the thread handling consumer pool.
//     *
//     * Only this method is called concurrently by many threads, through {@link DeferredCompletion} interface,
//     * so DO NOT TOUCH {@link #states} IN THIS METHOD.
//     *
//     * @param state the {@link OffsetState} to complete.
//     */
//    void complete(OffsetState state) {
//        if (state.committed) {
//            // Suppose this offset has already been completed once, so no need to do anything.
//            return;
//        }
//        if (state.offset > latest) {
//            throw new IllegalArgumentException(String.format(
//                    "complete attempt on %d which is larger than current latest %d", state.offset, latest));
//        }
//
//        state.committed = true;
//
//        if (logger.isDebugEnabled()) {
//            logger.debug("Offset complete({}) earliest={} latest={} hw={}",
//                         state.offset, earliest, latest, highWatermark);
//        }
//    }
//
//    public synchronized void updateHighWatermark() {
//        if (logger.isTraceEnabled()) {
//            StringBuilder sb = new StringBuilder("[");
//
//            boolean first = true;
//            for (OffsetState st : states) {
//                if (first) {
//                    first = false;
//                } else {
//                    sb.append(", ");
//                }
//                sb.append(String.valueOf(st.offset) + ':' + (st.committed ? 'c' : 'n'));
//            }
//            sb.append(']');
//            logger.trace("Begin updateHighWatermark earliest={} latest={} hw={} states={}",
//                         earliest, latest, highWatermark, sb);
//        }
//
//        long lastHighWatermark = highWatermark;
//
//        OffsetState state;
//        while ((state = states.peekFirst()) != null) {
//            earliest = state.offset;
//            if (state.committed) {
//                highWatermark = state.offset;
//                states.pollFirst();
//            } else {
//                break;
//            }
//        }
//
//        if (highWatermark != lastHighWatermark) {
//            logger.debug("High watermark updated for {}: {} => {}",
//                         topicPartition, lastHighWatermark, highWatermark);
//        }
//    }
//
//    public synchronized int pendingOffsetsCount() {
//        return states.size();
//    }
//
//    public long commitReadyOffset() {
//        return highWatermark;
//    }
//
//    public boolean isRegressing(long offset) {
//        return offset < latest;
//    }
//
//    @Override
//    public String toString() {
//        return "OutOfOrderCommitControl{" +
//               "topicPartition=" + topicPartition +
//               ", earliest=" + earliest +
//               ", latest=" + latest +
//               ", highWatermark=" + highWatermark +
//               '}';
//    }
//}

/*
 * Copyright 2020 LINE Corporation
 *
 * LINE Corporation licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package com.linecorp.decaton.processor.runtime;

import java.util.Deque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents consumption processing progress of records consumed from a single partition.
 * This class manages below states:
 * - The list of record offsets which are currently being processed(either sequentially or concurrently).
 * - The list of record offsets which are completed processing and marked ready to be committed.
 * - The maximum offset of records which tells high watermark of records which has been processed and completed.
 */
public class OutOfOrderCommitControl {
    private static final Logger logger = LoggerFactory.getLogger(OutOfOrderCommitControl.class);

    private final Object offsetsHeadEntryLock = new Object();

    private final TopicPartition topicPartition;
    private final Deque<Long> onBoardOffsets;
    private final PriorityBlockingQueue<Long> committedOffsets;
    private final AtomicLong committedHighWatermark;

    public OutOfOrderCommitControl(TopicPartition topicPartition, int notUsed) {
        this.topicPartition = topicPartition;
        onBoardOffsets = new LinkedBlockingDeque<>();
        committedOffsets = new PriorityBlockingQueue<>();
        committedHighWatermark = new AtomicLong();
    }

    public TopicPartition topicPartition() {
        return topicPartition;
    }

    public void reportFetchedOffset(long offset) {
        final Long lastOffset = onBoardOffsets.peekLast();
        if (lastOffset != null) {
            if (offset < lastOffset) {
                throw new IllegalArgumentException(
                        "offset regression: offset=" + offset + ", lastOffset=" + lastOffset);
            }
        }
        onBoardOffsets.addLast(offset);
    }

    // visible for testing
    boolean isValidOffsetToComplete(long offset, Long firstOnBoardOffset) {
        return firstOnBoardOffset != null && firstOnBoardOffset <= offset;
    }

    // visible for testing
    // this method is made for hooking in unit test to validate timing issue which can happen under concurrent
    // access.
    Long peekFirstOnBoardOffset() {
        return onBoardOffsets.peekFirst();
    }

    public void complete(long offset) {
        synchronized (offsetsHeadEntryLock) {
            if (!isValidOffsetToComplete(offset, onBoardOffsets.peekFirst()) ||
                committedOffsets.contains(offset)) {
                // This should happens only when processor misused DeferredCompletion
                throw new IllegalArgumentException(
                        "offset " + offset + " of partition " + topicPartition + " completed already");
            }
            committedOffsets.add(offset);
        }

        if (logger.isTraceEnabled()) {
            logger.trace("offset complete({}), onBoard = {}, committed = {}",
                         offset, onBoardOffsets, committedOffsets);
        } else if (logger.isDebugEnabled()) {
            final Long peekFirst = onBoardOffsets.peekFirst();
            final Long peekLast = onBoardOffsets.peekLast();
            final long onBoardApproximateSize = peekFirst == null || peekLast == null
                                                ? onBoardOffsets.size() : peekLast - peekFirst + 1;
            logger.debug("offset complete({}) onBoard = [{}:{},{}], committed = [{},{}]", offset,
                         peekFirst, peekLast, onBoardApproximateSize,
                         committedOffsets.peek(), committedOffsets.size());
        }

        Long firstOnBoardOffset = peekFirstOnBoardOffset();
        if (firstOnBoardOffset != null && offset == firstOnBoardOffset) {
            shiftHighWatermark();
        }
    }

    private synchronized void shiftHighWatermark() {
        // whenever this is true, !onBoardOffsets.isEmpty() should also be true
        while (!committedOffsets.isEmpty()) {
            long firstOnBoardOffset = onBoardOffsets.getFirst();
            long firstCommittedOffset = committedOffsets.peek();
            if (firstOnBoardOffset != firstCommittedOffset) {
                break;
            }
            synchronized (offsetsHeadEntryLock) {
                onBoardOffsets.removeFirst();
                committedHighWatermark.set(committedOffsets.remove());
            }
        }
        logger.debug("new high watermark for partition {} = {}", topicPartition, committedHighWatermark.get());
    }

    public long commitReadyOffset() {
        return committedHighWatermark.get();
    }

    public int pendingRecordsCount() {
        return onBoardOffsets.size();
    }
}
