/*
 * Copyright 2011 LMAX Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.lmax.disruptor;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import com.lmax.disruptor.util.Util;

/**
 * Base class for the various sequencer types (single/multi).  Provides
 * common functionality like the management of gating sequences (add/remove) and
 * ownership of the current cursor.
 */
public abstract class AbstractSequencer implements Sequencer
{
    private static final AtomicReferenceFieldUpdater<AbstractSequencer, Sequence[]> SEQUENCE_UPDATER =
        AtomicReferenceFieldUpdater.newUpdater(AbstractSequencer.class, Sequence[].class, "gatingSequences");

    protected final int bufferSize;
    protected final WaitStrategy waitStrategy;
    protected final Sequence cursor = new Sequence(Sequencer.INITIAL_CURSOR_VALUE);
    protected volatile Sequence[] gatingSequences = new Sequence[0];

    /**
     * Create with the specified buffer size and wait strategy.
     *
     * @param bufferSize   The total number of entries, must be a positive power of 2.
     * @param waitStrategy The wait strategy used by this sequencer
     */
    public AbstractSequencer(int bufferSize, WaitStrategy waitStrategy)
    {
        // 如果缓冲大小小于1
        if (bufferSize < 1)
        {
            // 不细究
            throw new IllegalArgumentException("bufferSize must not be less than 1");
        }
        // 如果缓冲大小的值转换成2进制后，1的个数不为1
        if (Integer.bitCount(bufferSize) != 1)
        {
            // 不细究
            throw new IllegalArgumentException("bufferSize must be a power of 2");
        }

        // 设置当前序列器的缓冲大小、等待策略
        this.bufferSize = bufferSize;
        this.waitStrategy = waitStrategy;
    }

    /**
     * @see Sequencer#getCursor()
     */
    @Override
    public final long getCursor()
    {
        // 获取并返回游标
        return cursor.get();
    }

    /**
     * @see Sequencer#getBufferSize()
     */
    @Override
    public final int getBufferSize()
    {
        // 返回当前序列器的缓冲大小
        return bufferSize;
    }

    /**
     * @see Sequencer#addGatingSequences(Sequence...)
     */
    @Override
    public final void addGatingSequences(Sequence... gatingSequences)
    {
        // 序列组增加序列
        SequenceGroups.addSequences(this, SEQUENCE_UPDATER, this, gatingSequences);
    }

    /**
     * @see Sequencer#removeGatingSequence(Sequence)
     */
    @Override
    public boolean removeGatingSequence(Sequence sequence)
    {
        return SequenceGroups.removeSequence(this, SEQUENCE_UPDATER, sequence);
    }

    /**
     * @see Sequencer#getMinimumSequence()
     */
    @Override
    public long getMinimumSequence()
    {
        return Util.getMinimumSequence(gatingSequences, cursor.get());
    }

    /**
     * @see Sequencer#newBarrier(Sequence...)
     */
    @Override
    public SequenceBarrier newBarrier(Sequence... sequencesToTrack)
    {
        // 实例化并返回处理序列栅栏
        return new ProcessingSequenceBarrier(this, waitStrategy, cursor, sequencesToTrack);
    }

    /**
     * Creates an event poller for this sequence that will use the supplied data provider and
     * gating sequences.
     *
     * @param dataProvider    The data source for users of this event poller
     * @param gatingSequences Sequence to be gated on.
     * @return A poller that will gate on this ring buffer and the supplied sequences.
     */
    @Override
    public <T> EventPoller<T> newPoller(DataProvider<T> dataProvider, Sequence... gatingSequences)
    {
        return EventPoller.newInstance(dataProvider, this, new Sequence(), cursor, gatingSequences);
    }

    @Override
    public String toString()
    {
        return "AbstractSequencer{" +
            "waitStrategy=" + waitStrategy +
            ", cursor=" + cursor +
            ", gatingSequences=" + Arrays.toString(gatingSequences) +
            '}';
    }
}