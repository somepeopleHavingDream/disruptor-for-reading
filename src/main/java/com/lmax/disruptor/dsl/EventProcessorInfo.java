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
package com.lmax.disruptor.dsl;

import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.EventProcessor;
import com.lmax.disruptor.Sequence;
import com.lmax.disruptor.SequenceBarrier;

import java.util.concurrent.Executor;

/**
 * <p>Wrapper class to tie together a particular event processing stage</p>
 * <p>
 * <p>Tracks the event processor instance, the event handler instance, and sequence barrier which the stage is attached to.</p>
 *
 * @param <T> the type of the configured {@link EventHandler}
 */
class EventProcessorInfo<T> implements ConsumerInfo
{
    private final EventProcessor eventprocessor;
    private final EventHandler<? super T> handler;
    private final SequenceBarrier barrier;
    private boolean endOfChain = true;

    EventProcessorInfo(
        final EventProcessor eventprocessor, final EventHandler<? super T> handler, final SequenceBarrier barrier)
    {
        // 设置当前事件处理器信息的事件处理器、事件处理者、序列栅栏
        this.eventprocessor = eventprocessor;
        this.handler = handler;
        this.barrier = barrier;
    }

    public EventProcessor getEventProcessor()
    {
        return eventprocessor;
    }

    @Override
    public Sequence[] getSequences()
    {
        // 从当前事件处理器信息的事件处理器中，拿到序列，封装成数组返回
        return new Sequence[]{eventprocessor.getSequence()};
    }

    public EventHandler<? super T> getHandler()
    {
        return handler;
    }

    @Override
    public SequenceBarrier getBarrier()
    {
        return barrier;
    }

    @Override
    public boolean isEndOfChain()
    {
        // 返回当前事件处理器信息是否在链尾
        return endOfChain;
    }

    @Override
    public void start(final Executor executor)
    {
        // 用入参执行器执行当前事件处理器信息的事件处理器
        executor.execute(eventprocessor);
    }

    @Override
    public void halt()
    {
        // 关闭当前事件处理器信息的事件处理器
        eventprocessor.halt();
    }

    /**
     *
     */
    @Override
    public void markAsUsedInBarrier()
    {
        // 将当前事件处理器信息的链尾标记置为假
        endOfChain = false;
    }

    @Override
    public boolean isRunning()
    {
        // 返回当前事件处理器信息中的事件处理器是否正在运行
        return eventprocessor.isRunning();
    }
}
