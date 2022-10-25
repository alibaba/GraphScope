/*
 *  Copyright 2016-2019 Netflix, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */
package com.alibaba.graphscope.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.BitSet;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.AtomicReference;

/**
 * This is a lock-free, thread-safe version of a {@link java.util.BitSet}.<p>
 *
 * Instead of a long array to hold the bits, this implementation uses an AtomicLongArray, then
 * does the appropriate compare-and-swap operations when setting the bits.
 *
 * @author dkoszewnik
 *
 */
public class ThreadSafeBitSet {
    private static Logger logger = LoggerFactory.getLogger(ThreadSafeBitSet.class.getName());

    public static final int DEFAULT_LOG2_SEGMENT_SIZE_IN_BITS = 14;

    private final int numLongsPerSegment;
    private final int log2SegmentSize;
    private final int segmentMask;
    private final AtomicReference<ThreadSafeBitSetSegments> segments;

    public ThreadSafeBitSet() {
        this(DEFAULT_LOG2_SEGMENT_SIZE_IN_BITS); // / 16384 bits, 2048 bytes, 256 longs per segment
    }

    public ThreadSafeBitSet(int log2SegmentSizeInBits) {
        this(log2SegmentSizeInBits, 0);
    }

    public ThreadSafeBitSet(int log2SegmentSizeInBits, int numBitsToPreallocate) {
        if (log2SegmentSizeInBits < 6)
            throw new IllegalArgumentException(
                    "Cannot specify fewer than 64 bits in each segment!");

        this.log2SegmentSize = log2SegmentSizeInBits;
        this.numLongsPerSegment = (1 << (log2SegmentSizeInBits - 6));
        this.segmentMask = numLongsPerSegment - 1;

        long numBitsPerSegment = numLongsPerSegment * 64;
        int numSegmentsToPreallocate =
                numBitsToPreallocate == 0
                        ? 1
                        : (int) (((numBitsToPreallocate - 1) / numBitsPerSegment) + 1);

        segments = new AtomicReference<ThreadSafeBitSetSegments>();
        segments.set(new ThreadSafeBitSetSegments(numSegmentsToPreallocate, numLongsPerSegment));
    }

    public void set(int position) {
        int segmentPosition =
                position >>> log2SegmentSize; // / which segment -- div by num bits per segment
        int longPosition =
                (position >>> 6)
                        & segmentMask; /// which long in the segment -- remainder of div by num bits
        // per segment
        int bitPosition =
                position & 0x3F; // / which bit in the long -- remainder of div by num bits in long
        // (64)

        AtomicLongArray segment = getSegment(segmentPosition);

        long mask = 1L << bitPosition;

        // Thread safety: we need to loop until we win the race to set the long value.
        while (true) {
            // determine what the new long value will be after we set the appropriate bit.
            long currentLongValue = segment.get(longPosition);
            long newLongValue = currentLongValue | mask;

            // if no other thread has modified the value since we read it, we won the race and we
            // are done.
            if (segment.compareAndSet(longPosition, currentLongValue, newLongValue)) break;
        }
    }

    public void setUntil(int position) {
        //        int segmentPosition = position >>> log2SegmentSize; /// which segment -- div by
        // num bits per segment
        //        for (int i = 0; i < segmentPosition; ++i){
        //            AtomicLongArray segment = getSegment(segmentPosition);
        //            for (int j = 0; j < segment.length(); ++j){
        //                segment.set(j, -1);
        //            }
        //        }
        //        int start = (segmentPosition << log2SegmentSize);
        //        for (int i = start; i < position; ++i){
        //            set(i);
        //        }
        long time0 = System.nanoTime();
        for (int i = 0; i < position; ++i) {
            set(i);
        }
        long time1 = System.nanoTime();
        logger.info("Set unitl {} cost {} ms", position, (time1 - time0) / 1000000);
    }

    public void clear(int position) {
        int segmentPosition =
                position >>> log2SegmentSize; // / which segment -- div by num bits per segment
        int longPosition =
                (position >>> 6)
                        & segmentMask; /// which long in the segment -- remainder of div by num bits
        // per segment
        int bitPosition =
                position & 0x3F; // / which bit in the long -- remainder of div by num bits in long
        // (64)

        AtomicLongArray segment = getSegment(segmentPosition);

        long mask = ~(1L << bitPosition);

        // Thread safety: we need to loop until we win the race to set the long value.
        while (true) {
            // determine what the new long value will be after we set the appropriate bit.
            long currentLongValue = segment.get(longPosition);
            long newLongValue = currentLongValue & mask;

            // if no other thread has modified the value since we read it, we won the race and we
            // are done.
            if (segment.compareAndSet(longPosition, currentLongValue, newLongValue)) break;
        }
    }

    public boolean get(int position) {
        int segmentPosition =
                position >>> log2SegmentSize; // / which segment -- div by num bits per segment
        int longPosition =
                (position >>> 6)
                        & segmentMask; /// which long in the segment -- remainder of div by num bits
        // per segment
        int bitPosition =
                position & 0x3F; // / which bit in the long -- remainder of div by num bits in long
        // (64)

        AtomicLongArray segment = getSegment(segmentPosition);

        long mask = 1L << bitPosition;

        return ((segment.get(longPosition) & mask) != 0);
    }

    public long maxSetBit() {
        ThreadSafeBitSetSegments segments = this.segments.get();

        int segmentIdx = segments.numSegments() - 1;

        for (; segmentIdx >= 0; segmentIdx--) {
            AtomicLongArray segment = segments.getSegment(segmentIdx);
            for (int longIdx = segment.length() - 1; longIdx >= 0; longIdx--) {
                long l = segment.get(longIdx);
                if (l != 0)
                    return (segmentIdx << log2SegmentSize)
                            + (longIdx * 64)
                            + (63 - Long.numberOfLeadingZeros(l));
            }
        }

        return -1;
    }

    public int nextSetBit(int fromIndex) {
        if (fromIndex < 0) throw new IndexOutOfBoundsException("fromIndex < 0: " + fromIndex);

        int segmentPosition = fromIndex >>> log2SegmentSize;

        ThreadSafeBitSetSegments segments = this.segments.get();

        if (segmentPosition >= segments.numSegments()) return -1;

        int longPosition = (fromIndex >>> 6) & segmentMask;
        int bitPosition = fromIndex & 0x3F;
        AtomicLongArray segment = segments.getSegment(segmentPosition);

        long word = segment.get(longPosition) & (0xffffffffffffffffL << bitPosition);

        while (true) {
            if (word != 0)
                return (segmentPosition << (log2SegmentSize))
                        + (longPosition << 6)
                        + Long.numberOfTrailingZeros(word);
            if (++longPosition > segmentMask) {
                segmentPosition++;
                if (segmentPosition >= segments.numSegments()) return -1;
                segment = segments.getSegment(segmentPosition);
                longPosition = 0;
            }

            word = segment.get(longPosition);
        }
    }

    /**
     * @return the number of bits which are set in this bit set.
     */
    public int cardinality() {
        ThreadSafeBitSetSegments segments = this.segments.get();

        int numSetBits = 0;

        for (int i = 0; i < segments.numSegments(); i++) {
            AtomicLongArray segment = segments.getSegment(i);
            for (int j = 0; j < segment.length(); j++) {
                numSetBits += Long.bitCount(segment.get(j));
            }
        }

        return numSetBits;
    }

    public long[] getWords() {
        int len = 0;
        ThreadSafeBitSetSegments segments = this.segments.get();
        for (int i = 0; i < segments.numSegments(); ++i) {
            AtomicLongArray segment = segments.getSegment(i);
            len += segment.length();
        }
        long[] res = new long[len];
        int ind = 0;
        for (int i = 0; i < segments.numSegments(); ++i) {
            AtomicLongArray segment = segments.getSegment(i);
            for (int j = 0; j < segment.length(); ++j) {
                res[ind] = segment.get(j);
                ind += 1;
            }
        }
        return res;
    }

    /**
     * @return the number of bits which are current specified by this bit set.  This is the maximum value
     * to which you might need to iterate, if you were to iterate over all bits in this set.
     */
    public int currentCapacity() {
        return segments.get().numSegments() * (1 << log2SegmentSize);
    }

    /**
     * Clear all bits to 0.
     */
    public void clearAll() {
        ThreadSafeBitSetSegments segments = this.segments.get();

        for (int i = 0; i < segments.numSegments(); i++) {
            AtomicLongArray segment = segments.getSegment(i);

            for (int j = 0; j < segment.length(); j++) {
                segment.set(j, 0L);
            }
        }
    }

    /**
     * Return a new bit set which contains all bits which are contained in this bit set, and which are NOT contained in the <code>other</code> bit set.<p>
     *
     * In other words, return a new bit set, which is a bitwise and with the bitwise not of the other bit set.
     *
     * @param other the other bit set
     * @return the resulting bit set
     */
    public ThreadSafeBitSet andNot(ThreadSafeBitSet other) {
        if (other.log2SegmentSize != log2SegmentSize)
            throw new IllegalArgumentException("Segment sizes must be the same");

        ThreadSafeBitSetSegments thisSegments = this.segments.get();
        ThreadSafeBitSetSegments otherSegments = other.segments.get();
        ThreadSafeBitSetSegments newSegments =
                new ThreadSafeBitSetSegments(thisSegments.numSegments(), numLongsPerSegment);

        for (int i = 0; i < thisSegments.numSegments(); i++) {
            AtomicLongArray thisArray = thisSegments.getSegment(i);
            AtomicLongArray otherArray =
                    (i < otherSegments.numSegments()) ? otherSegments.getSegment(i) : null;
            AtomicLongArray newArray = newSegments.getSegment(i);

            for (int j = 0; j < thisArray.length(); j++) {
                long thisLong = thisArray.get(j);
                long otherLong = (otherArray == null) ? 0 : otherArray.get(j);

                newArray.set(j, thisLong & ~otherLong);
            }
        }

        ThreadSafeBitSet andNot = new ThreadSafeBitSet(log2SegmentSize);
        andNot.segments.set(newSegments);
        return andNot;
    }

    /**
     * Return a new bit set which contains all bits which are contained in *any* of the specified bit sets.
     *
     * @param bitSets the other bit sets
     * @return the resulting bit set
     */
    public static ThreadSafeBitSet orAll(ThreadSafeBitSet... bitSets) {
        if (bitSets.length == 0) return new ThreadSafeBitSet();

        int log2SegmentSize = bitSets[0].log2SegmentSize;
        int numLongsPerSegment = bitSets[0].numLongsPerSegment;

        ThreadSafeBitSetSegments segments[] = new ThreadSafeBitSetSegments[bitSets.length];
        int maxNumSegments = 0;

        for (int i = 0; i < bitSets.length; i++) {
            if (bitSets[i].log2SegmentSize != log2SegmentSize)
                throw new IllegalArgumentException("Segment sizes must be the same");

            segments[i] = bitSets[i].segments.get();
            if (segments[i].numSegments() > maxNumSegments)
                maxNumSegments = segments[i].numSegments();
        }

        ThreadSafeBitSetSegments newSegments =
                new ThreadSafeBitSetSegments(maxNumSegments, numLongsPerSegment);

        AtomicLongArray segment[] = new AtomicLongArray[segments.length];

        for (int i = 0; i < maxNumSegments; i++) {
            for (int j = 0; j < segments.length; j++) {
                segment[j] = i < segments[j].numSegments() ? segments[j].getSegment(i) : null;
            }

            AtomicLongArray newSegment = newSegments.getSegment(i);

            for (int j = 0; j < numLongsPerSegment; j++) {
                long value = 0;
                for (int k = 0; k < segments.length; k++) {
                    if (segment[k] != null) value |= segment[k].get(j);
                }
                newSegment.set(j, value);
            }
        }

        ThreadSafeBitSet or = new ThreadSafeBitSet(log2SegmentSize);
        or.segments.set(newSegments);
        return or;
    }

    /**
     * Get the segment at <code>segmentIndex</code>.  If this segment does not yet exist, create it.
     *
     * @param segmentIndex the segment index
     * @return the segment
     */
    private AtomicLongArray getSegment(int segmentIndex) {
        ThreadSafeBitSetSegments visibleSegments = segments.get();

        while (visibleSegments.numSegments() <= segmentIndex) {
            /// Thread safety:  newVisibleSegments contains all of the segments from the currently
            // visible segments, plus extra.
            /// all of the segments in the currently visible segments are canonical and will not
            // change.
            ThreadSafeBitSetSegments newVisibleSegments =
                    new ThreadSafeBitSetSegments(
                            visibleSegments, segmentIndex + 1, numLongsPerSegment);

            /// because we are using a compareAndSet, if this thread "wins the race" and
            // successfully sets this variable, then the segments
            /// which are newly defined in newVisibleSegments become canonical.
            if (segments.compareAndSet(visibleSegments, newVisibleSegments)) {
                visibleSegments = newVisibleSegments;
            } else {
                /// If we "lose the race" and are growing the ThreadSafeBitSet segments larger,
                /// then we will gather the new canonical sets from the update which we missed on
                // the next iteration of this loop.
                /// Newly defined segments in newVisibleSegments will be discarded, they do not get
                // to become canonical.
                visibleSegments = segments.get();
            }
        }

        return visibleSegments.getSegment(segmentIndex);
    }

    private static class ThreadSafeBitSetSegments {

        private final AtomicLongArray segments[];

        private ThreadSafeBitSetSegments(int numSegments, int segmentLength) {
            AtomicLongArray segments[] = new AtomicLongArray[numSegments];

            for (int i = 0; i < numSegments; i++) {
                segments[i] = new AtomicLongArray(segmentLength);
            }

            /// Thread safety: Because this.segments is final, the preceding operations in this
            // constructor are guaranteed to be visible to any
            /// other thread which accesses this.segments.
            this.segments = segments;
        }

        private ThreadSafeBitSetSegments(
                ThreadSafeBitSetSegments copyFrom, int numSegments, int segmentLength) {
            AtomicLongArray segments[] = new AtomicLongArray[numSegments];

            for (int i = 0; i < numSegments; i++) {
                segments[i] =
                        i < copyFrom.numSegments()
                                ? copyFrom.getSegment(i)
                                : new AtomicLongArray(segmentLength);
            }

            /// see above re: thread-safety of this assignment
            this.segments = segments;
        }

        public int numSegments() {
            return segments.length;
        }

        public AtomicLongArray getSegment(int index) {
            return segments[index];
        }
    }

    public void serializeBitsTo(DataOutputStream os) throws IOException {
        ThreadSafeBitSetSegments segments = this.segments.get();

        os.writeInt(segments.numSegments() * numLongsPerSegment);

        for (int i = 0; i < segments.numSegments(); i++) {
            AtomicLongArray arr = segments.getSegment(i);

            for (int j = 0; j < arr.length(); j++) {
                os.writeLong(arr.get(j));
            }
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof ThreadSafeBitSet)) return false;

        ThreadSafeBitSet other = (ThreadSafeBitSet) obj;

        if (other.log2SegmentSize != log2SegmentSize)
            throw new IllegalArgumentException("Segment sizes must be the same");

        ThreadSafeBitSetSegments thisSegments = this.segments.get();
        ThreadSafeBitSetSegments otherSegments = other.segments.get();

        for (int i = 0; i < thisSegments.numSegments(); i++) {
            AtomicLongArray thisArray = thisSegments.getSegment(i);
            AtomicLongArray otherArray =
                    (i < otherSegments.numSegments()) ? otherSegments.getSegment(i) : null;

            for (int j = 0; j < thisArray.length(); j++) {
                long thisLong = thisArray.get(j);
                long otherLong = (otherArray == null) ? 0 : otherArray.get(j);

                if (thisLong != otherLong) return false;
            }
        }

        for (int i = thisSegments.numSegments(); i < otherSegments.numSegments(); i++) {
            AtomicLongArray otherArray = otherSegments.getSegment(i);

            for (int j = 0; j < otherArray.length(); j++) {
                long l = otherArray.get(j);

                if (l != 0) return false;
            }
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = log2SegmentSize;
        result = 31 * result + Arrays.hashCode(segments.get().segments);
        return result;
    }

    /**
     * @return a new BitSet with same bits set
     */
    public BitSet toBitSet() {
        BitSet resultSet = new BitSet();
        int ordinal = this.nextSetBit(0);
        while (ordinal != -1) {
            resultSet.set(ordinal);
            ordinal = this.nextSetBit(ordinal + 1);
        }
        return resultSet;
    }

    @Override
    public String toString() {
        return toBitSet().toString();
    }
}
