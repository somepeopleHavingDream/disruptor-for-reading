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

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * <p>A {@link WorkProcessor} wraps a single {@link WorkHandler}, effectively consuming the sequence
 * and ensuring appropriate barriers.</p>
 *
 * <p>Generally, this will be used as part of a {@link WorkerPool}.</p>
 *
 * @param <T> event implementation storing the details for the work to processed.
 */
public final class WorkProcessor<T>
    implements EventProcessor
{
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final Sequence sequence = new Sequence(Sequencer.INITIAL_CURSOR_VALUE);
    private final RingBuffer<T> ringBuffer;
    private final SequenceBarrier sequenceBarrier;
    private final WorkHandler<? super T> workHandler;
    private final ExceptionHandler<? super T> exceptionHandler;
    private final Sequence workSequence;

    private final EventReleaser eventReleaser = new EventReleaser()
    {
        @Override
        public void release()
        {
            sequence.set(Long.MAX_VALUE);
        }
    };

    private final TimeoutHandler timeoutHandler;

    /**
     * Construct a {@link WorkProcessor}.
     *
     * @param ringBuffer       to which events are published.
     * @param sequenceBarrier  on which it is waiting.
     * @param workHandler      is the delegate to which events are dispatched.
     * @param exceptionHandler to be called back when an error occurs
     * @param workSequence     from which to claim the next event to be worked on.  It should always be initialised
     *                         as {@link Sequencer#INITIAL_CURSOR_VALUE}
     */
    public WorkProcessor(
        final RingBuffer<T> ringBuffer,
        final SequenceBarrier sequenceBarrier,
        final WorkHandler<? super T> workHandler,
        final ExceptionHandler<? super T> exceptionHandler,
        final Sequence workSequence)
    {
        // 设置当前工作处理器的环形缓冲、序列栅栏、工作处理者、异常处理器、工作序列
        this.ringBuffer = ringBuffer;
        this.sequenceBarrier = sequenceBarrier;
        this.workHandler = workHandler;
        this.exceptionHandler = exceptionHandler;
        this.workSequence = workSequence;

        // 如果当前工作处理器的工作处理者是事件释放感知实例
        if (this.workHandler instanceof EventReleaseAware)
        {
            // 不细究
            ((EventReleaseAware) this.workHandler).setEventReleaser(eventReleaser);
        }

        // 设值当前工作处理器的超时处理器
        timeoutHandler = (workHandler instanceof TimeoutHandler) ? (TimeoutHandler) workHandler : null;
    }

    @Override
    public Sequence getSequence()
    {
        // 返回当前工作处理器的序列
        return sequence;
    }

    @Override
    public void halt()
    {
        running.set(false);
        sequenceBarrier.alert();
    }

    @Override
    public boolean isRunning()
    {
        return running.get();
    }

    /**
     * It is ok to have another thread re-run this method after a halt().
     *
     * @throws IllegalStateException if this processor is already running
     */
    @Override
    public void run()
    {
        // 如果cas地设置当前工作处理器的运行状态失败
        if (!running.compareAndSet(false, true))
        {
            // 不细究
            throw new IllegalStateException("Thread is already running");
        }
        // 当前工作处理器的序列栅栏清除警告
        sequenceBarrier.clearAlert();

        // 通知启动
        notifyStart();

        // 初始化处理序列为真
        boolean processedSequence = true;
        // 获得缓存的可用序列值
        long cachedAvailableSequence = Long.MIN_VALUE;
        // 从当前工作处理器序列中获得下一个序列
        long nextSequence = sequence.get();
        T event = null;

        while (true)
        {
            try
            {
                // if previous sequence was processed - fetch the next sequence and set
                // that we have successfully processed the previous sequence
                // typically, this will be true
                // this prevents the sequence getting too far forward if an exception
                // is thrown from the WorkHandler

                // 如果处理序列标记为真
                if (processedSequence)
                {
                    // 置处理序列标记为假（应该代表，一个序列没处理完）
                    processedSequence = false;

                    do
                    {
                        // 通过当前工作处理器的工作序列，计算出下一个序列值
                        nextSequence = workSequence.get() + 1L;
                        // 将当前工作处理器的序列设值为下一个序列值减1
                        sequence.set(nextSequence - 1L);
                    }
                    // cas地将当前工作处理器的工作序列设值为计算出的下一个序列值
                    while (!workSequence.compareAndSet(nextSequence - 1L, nextSequence));
                }

                // 如果缓存的可用序列值大于等于计算出来的下一个序列值
                if (cachedAvailableSequence >= nextSequence)
                {
                    // 从当前工作处理器所用环形缓冲中，获得下一个序列值对应的事件
                    event = ringBuffer.get(nextSequence);
                    // 调用当前工作处理器的工作处理者的事件触发方法
                    workHandler.onEvent(event);

                    // 置处理序列标记为真（应该代表处理完了一个序列）
                    processedSequence = true;
                }
                else
                {
                    // 序列栅栏做等待操作，获得缓存的可用序列值
                    cachedAvailableSequence = sequenceBarrier.waitFor(nextSequence);
                }
            }
            catch (final TimeoutException e)
            {
                // 不细究
                notifyTimeout(sequence.get());
            }
            catch (final AlertException ex)
            {
                /*
                    以下不细究
                 */
                if (!running.get())
                {
                    break;
                }
            }
            catch (final Throwable ex)
            {
                /*
                    以下不细究
                 */
                // handle, mark as processed, unless the exception handler threw an exception
                exceptionHandler.handleEventException(ex, nextSequence, event);
                processedSequence = true;
            }
        }

        // 通知关闭
        notifyShutdown();

        // 将当前工作处理器的运行状态设置为假
        running.set(false);
    }

    private void notifyTimeout(final long availableSequence)
    {
        try
        {
            if (timeoutHandler != null)
            {
                timeoutHandler.onTimeout(availableSequence);
            }
        }
        catch (Throwable e)
        {
            exceptionHandler.handleEventException(e, availableSequence, null);
        }
    }

    private void notifyStart()
    {
        // 如果当前工作处理器的工作处理者是生命周期感知实例
        if (workHandler instanceof LifecycleAware)
        {
            /*
                以下不细究
             */
            try
            {
                ((LifecycleAware) workHandler).onStart();
            }
            catch (final Throwable ex)
            {
                exceptionHandler.handleOnStartException(ex);
            }
        }
    }

    private void notifyShutdown()
    {
        // 如果当前工作处理器的工作处理者是生命周期感知实例
        if (workHandler instanceof LifecycleAware)
        {
            /*
                以下不细究
             */
            try
            {
                ((LifecycleAware) workHandler).onShutdown();
            }
            catch (final Throwable ex)
            {
                exceptionHandler.handleOnShutdownException(ex);
            }
        }
    }
}
