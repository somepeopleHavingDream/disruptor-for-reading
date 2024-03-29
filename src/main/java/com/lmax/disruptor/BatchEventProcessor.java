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

import java.util.concurrent.atomic.AtomicInteger;


/**
 * Convenience class for handling the batching semantics of consuming entries from a {@link RingBuffer}
 * and delegating the available events to an {@link EventHandler}.
 * <p>
 * If the {@link EventHandler} also implements {@link LifecycleAware} it will be notified just after the thread
 * is started and just before the thread is shutdown.
 *
 * @param <T> event implementation storing the data for sharing during exchange or parallel coordination of an event.
 */
public final class BatchEventProcessor<T>
    implements EventProcessor
{
    private static final int IDLE = 0;
    private static final int HALTED = IDLE + 1;
    private static final int RUNNING = HALTED + 1;

    private final AtomicInteger running = new AtomicInteger(IDLE);
    private ExceptionHandler<? super T> exceptionHandler;
    private final DataProvider<T> dataProvider;
    private final SequenceBarrier sequenceBarrier;
    private final EventHandler<? super T> eventHandler;
    private final Sequence sequence = new Sequence(Sequencer.INITIAL_CURSOR_VALUE);
    private final TimeoutHandler timeoutHandler;
    private final BatchStartAware batchStartAware;

    /**
     * Construct a {@link EventProcessor} that will automatically track the progress by updating its sequence when
     * the {@link EventHandler#onEvent(Object, long, boolean)} method returns.
     *
     * @param dataProvider    to which events are published.
     * @param sequenceBarrier on which it is waiting.
     * @param eventHandler    is the delegate to which events are dispatched.
     */
    public BatchEventProcessor(
        final DataProvider<T> dataProvider,
        final SequenceBarrier sequenceBarrier,
        final EventHandler<? super T> eventHandler)
    {
        // 设置当前批量事件处理器的数据提供器、序列栅栏、事件处理器
        this.dataProvider = dataProvider;
        this.sequenceBarrier = sequenceBarrier;
        this.eventHandler = eventHandler;

        // 如果入参事件处理器是序列报告事件处理器实例
        if (eventHandler instanceof SequenceReportingEventHandler)
        {
            // 不细究
            ((SequenceReportingEventHandler<?>) eventHandler).setSequenceCallback(sequence);
        }

        // 设置当前批事件处理器的批量启动感知器、超时处理器
        batchStartAware =
            (eventHandler instanceof BatchStartAware) ? (BatchStartAware) eventHandler : null;
        timeoutHandler =
            (eventHandler instanceof TimeoutHandler) ? (TimeoutHandler) eventHandler : null;
    }

    @Override
    public Sequence getSequence()
    {
        // 返回当前批量事件处理器的序列
        return sequence;
    }

    @Override
    public void halt()
    {
        // 将当前批事件处理器的运行状态改为停止状态
        running.set(HALTED);
        // 警告当前批事件处理器的序列栅栏者
        sequenceBarrier.alert();
    }

    @Override
    public boolean isRunning()
    {
        // 如果当前批事件处理器的运行状态不是空闲状态，说明当前批事件处理器处于运行中
        return running.get() != IDLE;
    }

    /**
     * Set a new {@link ExceptionHandler} for handling exceptions propagated out of the {@link BatchEventProcessor}
     *
     * @param exceptionHandler to replace the existing exceptionHandler.
     */
    public void setExceptionHandler(final ExceptionHandler<? super T> exceptionHandler)
    {
        if (null == exceptionHandler)
        {
            throw new NullPointerException();
        }

        this.exceptionHandler = exceptionHandler;
    }

    /**
     * It is ok to have another thread rerun this method after a halt().
     *
     * @throws IllegalStateException if this object instance is already running in a thread
     */
    @Override
    public void run()
    {
        // cas地将当前批量事件处理器的运行状态从空闲状态改为运行状态
        if (running.compareAndSet(IDLE, RUNNING))
        {
            // 当前批事件处理器的序列栅栏器清空警告
            sequenceBarrier.clearAlert();

            // 通知启动（一般是通知用户自定义的事件处理者，如果该事件处理器实现了生命周期感知接口
            notifyStart();
            try
            {
                // 再次判断，如果当前批事件处理器正在运行
                if (running.get() == RUNNING)
                {
                    // 处理事件
                    processEvents();
                }
            }
            finally
            {
                // 当退出处理事件循环，该批量事件处理器做通知关闭操作
                notifyShutdown();
                // 将当前批事件处理器的运行状态改为空闲状态
                running.set(IDLE);
            }
        }
        else
        {
            /*
                以下不细究
             */
            // This is a little bit of guess work.  The running state could of changed to HALTED by
            // this point.  However, Java does not have compareAndExchange which is the only way
            // to get it exactly correct.
            if (running.get() == RUNNING)
            {
                throw new IllegalStateException("Thread is already running");
            }
            else
            {
                earlyExit();
            }
        }
    }

    private void processEvents()
    {
        T event = null;
        // 获得当前批事件处理器的下一个序列值
        long nextSequence = sequence.get() + 1L;

        while (true)
        {
            try
            {
                // 当前批事件处理器的序列栅栏做等待操作，直到获得下一个可用序列
                final long availableSequence = sequenceBarrier.waitFor(nextSequence);
                // 如果该批事件处理器的批量开始感知实例存在
                if (batchStartAware != null)
                {
                    // 不细究
                    batchStartAware.onBatchStart(availableSequence - nextSequence + 1);
                }

                // 当下一个序列值小于等于可用序列值（该可用序列号代表生产者的生产进度）
                while (nextSequence <= availableSequence)
                {
                    // 传入下一个序列值，从数据提供者（一般为ringBuffer）中获得事件
                    event = dataProvider.get(nextSequence);
                    // 将获取到的事件交由事件处理者（一般为用户代码）处理
                    eventHandler.onEvent(event, nextSequence, nextSequence == availableSequence);
                    // 将下一个序列号加一
                    nextSequence++;
                }

                // 将当前批事件处理器的序列设值为可用序列值
                sequence.set(availableSequence);
            }
            catch (final TimeoutException e)
            {
                // 不细究
                notifyTimeout(sequence.get());
            }
            catch (final AlertException ex)
            {
                // 捕获到了警告异常
                // 如果当前批事件处理器的运行状态不是正在运行状态
                if (running.get() != RUNNING)
                {
                    // 退出循环
                    break;
                }
            }
            catch (final Throwable ex)
            {
                /*
                    以下不细究
                 */
                handleEventException(ex, nextSequence, event);
                sequence.set(nextSequence);
                nextSequence++;
            }
        }
    }

    private void earlyExit()
    {
        notifyStart();
        notifyShutdown();
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
            handleEventException(e, availableSequence, null);
        }
    }

    /**
     * Notifies the EventHandler when this processor is starting up
     */
    private void notifyStart()
    {
        // 如果当前批事件处理器的事件处理器是生命周期感知实例
        if (eventHandler instanceof LifecycleAware)
        {
            /*
                以下不细究
             */
            try
            {
                ((LifecycleAware) eventHandler).onStart();
            }
            catch (final Throwable ex)
            {
                handleOnStartException(ex);
            }
        }
    }

    /**
     * Notifies the EventHandler immediately prior to this processor shutting down
     */
    private void notifyShutdown()
    {
        // 如果该批事件处理器是生命周期感知实例
        if (eventHandler instanceof LifecycleAware)
        {
            /*
                以下不细究
             */
            try
            {
                ((LifecycleAware) eventHandler).onShutdown();
            }
            catch (final Throwable ex)
            {
                handleOnShutdownException(ex);
            }
        }
    }

    /**
     * Delegate to {@link ExceptionHandler#handleEventException(Throwable, long, Object)} on the delegate or
     * the default {@link ExceptionHandler} if one has not been configured.
     */
    private void handleEventException(final Throwable ex, final long sequence, final T event)
    {
        getExceptionHandler().handleEventException(ex, sequence, event);
    }

    /**
     * Delegate to {@link ExceptionHandler#handleOnStartException(Throwable)} on the delegate or
     * the default {@link ExceptionHandler} if one has not been configured.
     */
    private void handleOnStartException(final Throwable ex)
    {
        getExceptionHandler().handleOnStartException(ex);
    }

    /**
     * Delegate to {@link ExceptionHandler#handleOnShutdownException(Throwable)} on the delegate or
     * the default {@link ExceptionHandler} if one has not been configured.
     */
    private void handleOnShutdownException(final Throwable ex)
    {
        getExceptionHandler().handleOnShutdownException(ex);
    }

    private ExceptionHandler<? super T> getExceptionHandler()
    {
        ExceptionHandler<? super T> handler = exceptionHandler;
        if (handler == null)
        {
            return ExceptionHandlers.defaultHandler();
        }
        return handler;
    }
}