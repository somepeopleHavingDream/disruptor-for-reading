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


/**
 * {@link SequenceBarrier} handed out for gating {@link EventProcessor}s on a cursor sequence and optional dependent {@link EventProcessor}(s),
 * using the given WaitStrategy.
 */
final class ProcessingSequenceBarrier implements SequenceBarrier
{
    private final WaitStrategy waitStrategy;
    private final Sequence dependentSequence;
    private volatile boolean alerted = false;
    private final Sequence cursorSequence;
    private final Sequencer sequencer;

    ProcessingSequenceBarrier(
        final Sequencer sequencer,
        final WaitStrategy waitStrategy,
        final Sequence cursorSequence,
        final Sequence[] dependentSequences)
    {
        // 设置当前处理序列栅栏的序列器、等待策略、游标序列
        this.sequencer = sequencer;
        this.waitStrategy = waitStrategy;
        this.cursorSequence = cursorSequence;

        // 如果所依赖的序列数组长度为0
        if (0 == dependentSequences.length)
        {
            // 将当前处理序列栅栏所依赖的序列，设置为游标序列
            dependentSequence = cursorSequence;
        }
        else
        {
            // 不细究
            dependentSequence = new FixedSequenceGroup(dependentSequences);
        }
    }

    @Override
    public long waitFor(final long sequence)
        throws AlertException, InterruptedException, TimeoutException
    {
        // 检查警告
        checkAlert();

        // 等待策略，做等待操作，直到获得可用的序列
        long availableSequence = waitStrategy.waitFor(sequence, cursorSequence, dependentSequence, this);
        // 如果可用序列号小于入参序列号
        if (availableSequence < sequence)
        {
            // 不细究
            return availableSequence;
        }

        // 获得并返回序列器的最高发布序列
        return sequencer.getHighestPublishedSequence(sequence, availableSequence);
    }

    @Override
    public long getCursor()
    {
        return dependentSequence.get();
    }

    @Override
    public boolean isAlerted()
    {
        return alerted;
    }

    @Override
    public void alert()
    {
        alerted = true;
        waitStrategy.signalAllWhenBlocking();
    }

    @Override
    public void clearAlert()
    {
        // 将当前正在处理的序列栅栏的警告标记值置为假
        alerted = false;
    }

    @Override
    public void checkAlert() throws AlertException
    {
        // 如果当前正在处理序列栅栏被警告了
        if (alerted)
        {
            // 抛出警告异常
            throw AlertException.INSTANCE;
        }
    }
}