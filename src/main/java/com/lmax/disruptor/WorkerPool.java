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

import com.lmax.disruptor.util.Util;

import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * WorkerPool contains a pool of {@link WorkProcessor}s that will consume sequences so jobs can be farmed out across a pool of workers.
 * Each of the {@link WorkProcessor}s manage and calls a {@link WorkHandler} to process the events.
 *
 * @param <T> event to be processed by a pool of workers
 */
public final class WorkerPool<T>
{
    private final AtomicBoolean started = new AtomicBoolean(false);
    private final Sequence workSequence = new Sequence(Sequencer.INITIAL_CURSOR_VALUE);
    private final RingBuffer<T> ringBuffer;
    // WorkProcessors are created to wrap each of the provided WorkHandlers
    private final WorkProcessor<?>[] workProcessors;

    /**
     * Create a worker pool to enable an array of {@link WorkHandler}s to consume published sequences.
     * <p>
     * This option requires a pre-configured {@link RingBuffer} which must have {@link RingBuffer#addGatingSequences(Sequence...)}
     * called before the work pool is started.
     *
     * @param ringBuffer       of events to be consumed.
     * @param sequenceBarrier  on which the workers will depend.
     * @param exceptionHandler to callback when an error occurs which is not handled by the {@link WorkHandler}s.
     * @param workHandlers     to distribute the work load across.
     */
    @SafeVarargs
    public WorkerPool(
        final RingBuffer<T> ringBuffer,
        final SequenceBarrier sequenceBarrier,
        final ExceptionHandler<? super T> exceptionHandler,
        final WorkHandler<? super T>... workHandlers)
    {
        // 设置当前工作池的环形缓冲
        this.ringBuffer = ringBuffer;

        // 获得工作者的数量
        final int numWorkers = workHandlers.length;
        // 实例化出对应工作者数量的工作处理器数组，并赋值为当前工作池的工作处理器集
        workProcessors = new WorkProcessor[numWorkers];

        // 初始化当前工作池的所有工作处理器
        for (int i = 0; i < numWorkers; i++)
        {
            // 初始化当前工作处理器
            workProcessors[i] = new WorkProcessor<>(
                ringBuffer,
                sequenceBarrier,
                workHandlers[i],
                exceptionHandler,
                workSequence);
        }
    }

    /**
     * Construct a work pool with an internal {@link RingBuffer} for convenience.
     * <p>
     * This option does not require {@link RingBuffer#addGatingSequences(Sequence...)} to be called before the work pool is started.
     *
     * @param eventFactory     for filling the {@link RingBuffer}
     * @param exceptionHandler to callback when an error occurs which is not handled by the {@link WorkHandler}s.
     * @param workHandlers     to distribute the work load across.
     */
    @SafeVarargs
    public WorkerPool(
        final EventFactory<T> eventFactory,
        final ExceptionHandler<? super T> exceptionHandler,
        final WorkHandler<? super T>... workHandlers)
    {
        ringBuffer = RingBuffer.createMultiProducer(eventFactory, 1024, new BlockingWaitStrategy());
        final SequenceBarrier barrier = ringBuffer.newBarrier();
        final int numWorkers = workHandlers.length;
        workProcessors = new WorkProcessor[numWorkers];

        for (int i = 0; i < numWorkers; i++)
        {
            workProcessors[i] = new WorkProcessor<>(
                ringBuffer,
                barrier,
                workHandlers[i],
                exceptionHandler,
                workSequence);
        }

        ringBuffer.addGatingSequences(getWorkerSequences());
    }

    /**
     * Get an array of {@link Sequence}s representing the progress of the workers.
     *
     * @return an array of {@link Sequence}s representing the progress of the workers.
     */
    public Sequence[] getWorkerSequences()
    {
        // 实例化出当前工作处理器加1个数量的序列数组
        final Sequence[] sequences = new Sequence[workProcessors.length + 1];
        // 遍历实例化出来的序列数组，初始化数组中的每个序列
        for (int i = 0, size = workProcessors.length; i < size; i++)
        {
            // 获得对应下标的工作处理器，获得该工作处理器的序列，赋值到数组当前位置中
            sequences[i] = workProcessors[i].getSequence();
        }
        // 将序列数组中下标为序列数组长度减一位置的序列设值为当前工作池的工作序列
        sequences[sequences.length - 1] = workSequence;

        // 返回序列集
        return sequences;
    }

    /**
     * Start the worker pool processing events in sequence.
     *
     * @param executor providing threads for running the workers.
     * @return the {@link RingBuffer} used for the work queue.
     * @throws IllegalStateException if the pool has already been started and not halted yet
     */
    public RingBuffer<T> start(final Executor executor)
    {
        // 如果设值当前工作池的启动状态失败
        if (!started.compareAndSet(false, true))
        {
            // 不细究
            throw new IllegalStateException("WorkerPool has already been started and cannot be restarted until halted.");
        }

        // 获得当前工作池的环形缓冲的游标值
        final long cursor = ringBuffer.getCursor();
        // 将当前工作池的工作序列值设值为获得的游标值
        workSequence.set(cursor);

        // 遍历当前工作池中的所有工作处理器
        for (WorkProcessor<?> processor : workProcessors)
        {
            // 获得工作处理器的序列，设值为游标值
            processor.getSequence().set(cursor);
            // 用入参执行器执行当前处理器
            executor.execute(processor);
        }

        // 返回当前工作池的环形缓冲
        return ringBuffer;
    }

    /**
     * Wait for the {@link RingBuffer} to drain of published events then halt the workers.
     */
    public void drainAndHalt()
    {
        Sequence[] workerSequences = getWorkerSequences();
        while (ringBuffer.getCursor() > Util.getMinimumSequence(workerSequences))
        {
            Thread.yield();
        }

        for (WorkProcessor<?> processor : workProcessors)
        {
            processor.halt();
        }

        started.set(false);
    }

    /**
     * Halt all workers immediately at the end of their current cycle.
     */
    public void halt()
    {
        for (WorkProcessor<?> processor : workProcessors)
        {
            processor.halt();
        }

        started.set(false);
    }

    public boolean isRunning()
    {
        return started.get();
    }
}
