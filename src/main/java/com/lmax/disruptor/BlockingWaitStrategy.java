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

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.lmax.disruptor.util.ThreadHints;

/**
 * Blocking strategy that uses a lock and condition variable for {@link EventProcessor}s waiting on a barrier.
 * <p>
 * This strategy can be used when throughput and low-latency are not as important as CPU resource.
 */
public final class BlockingWaitStrategy implements WaitStrategy
{
    private final Lock lock = new ReentrantLock();
    private final Condition processorNotifyCondition = lock.newCondition();

    @Override
    public long waitFor(long sequence, Sequence cursorSequence, Sequence dependentSequence, SequenceBarrier barrier)
        throws AlertException, InterruptedException
    {
        long availableSequence;

        // 如果入参游标序列值小于入参期望序列值
        if (cursorSequence.get() < sequence)
        {
            // 上锁
            lock.lock();
            try
            {
                // 当游标序列值（一般为单生产序列器的值）小于入参序列值
                while (cursorSequence.get() < sequence)
                {
                    // 序列栅栏检查警告
                    barrier.checkAlert();
                    // 等待处理器通知条件成立（这个地方会使线程挂起，直到生产者产生数据，因此该类叫做阻塞等待策略）
                    processorNotifyCondition.await();
                }
            }
            finally
            {
                // 解锁
                lock.unlock();
            }
        }

        // 当从依赖序列中获得的可用序列值小于入参序列值时
        while ((availableSequence = dependentSequence.get()) < sequence)
        {
            /*
                不细究
             */
            barrier.checkAlert();
            ThreadHints.onSpinWait();
        }

        // 返回可用序列值
        return availableSequence;
    }

    @Override
    public void signalAllWhenBlocking()
    {
        // 上锁
        lock.lock();
        try
        {
            // 处理器通知条件满足，通知所有线程
            processorNotifyCondition.signalAll();
        }
        finally
        {
            // 解锁
            lock.unlock();
        }
    }

    @Override
    public String toString()
    {
        return "BlockingWaitStrategy{" +
            "processorNotifyCondition=" + processorNotifyCondition +
            '}';
    }
}
