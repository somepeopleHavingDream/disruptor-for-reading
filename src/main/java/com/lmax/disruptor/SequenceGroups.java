/*
 * Copyright 2012 LMAX Ltd.
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

import static java.util.Arrays.copyOf;

import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

/**
 * Provides static methods for managing a {@link SequenceGroup} object.
 */
class SequenceGroups
{
    static <T> void addSequences(
        final T holder,
        final AtomicReferenceFieldUpdater<T, Sequence[]> updater,
        final Cursored cursor,
        final Sequence... sequencesToAdd)
    {
        long cursorSequence;
        Sequence[] updatedSequences;
        Sequence[] currentSequences;

        do
        {
            // 通过入参升级器，拿到当前序列数组
            currentSequences = updater.get(holder);
            // 复制到新的序列数组
            updatedSequences = copyOf(currentSequences, currentSequences.length + sequencesToAdd.length);

            // 从入参游标器中获得游标序列值
            cursorSequence = cursor.getCursor();

            // 记录原来序列数组的长度
            int index = currentSequences.length;
            // 遍历要添加的序列
            for (Sequence sequence : sequencesToAdd)
            {
                // 将每个序列的值设置为当前游标的序列值
                sequence.set(cursorSequence);
                // 将当前要添加的序列，放置到更新序列数组中
                updatedSequences[index++] = sequence;
            }
        }
        // cas操作，更新入参拥有器的序列数组
        while (!updater.compareAndSet(holder, currentSequences, updatedSequences));

        // 获得当前游标的序列值（之所以在这里再取一次游标的序列值，可能是因为在上述cas操作中，游标的序列值可能会发生改变）
        cursorSequence = cursor.getCursor();
        // 遍历待添加序列
        for (Sequence sequence : sequencesToAdd)
        {
            // 设置每个序列的值为当前游标的序列值
            sequence.set(cursorSequence);
        }
    }

    static <T> boolean removeSequence(
        final T holder,
        final AtomicReferenceFieldUpdater<T, Sequence[]> sequenceUpdater,
        final Sequence sequence)
    {
        int numToRemove;
        Sequence[] oldSequences;
        Sequence[] newSequences;

        do
        {
            // 获得原有的序列集
            oldSequences = sequenceUpdater.get(holder);
            // 计算出要移除的序列个数
            numToRemove = countMatching(oldSequences, sequence);

            // 如果不需要移除
            if (0 == numToRemove)
            {
                // 直接退出
                break;
            }

            final int oldSize = oldSequences.length;
            newSequences = new Sequence[oldSize - numToRemove];

            for (int i = 0, pos = 0; i < oldSize; i++)
            {
                final Sequence testSequence = oldSequences[i];
                if (sequence != testSequence)
                {
                    newSequences[pos++] = testSequence;
                }
            }
        }
        while (!sequenceUpdater.compareAndSet(holder, oldSequences, newSequences));

        return numToRemove != 0;
    }

    private static <T> int countMatching(T[] values, final T toMatch)
    {
        /*
            统计要移除的个数
         */
        int numToRemove = 0;
        for (T value : values)
        {
            if (value == toMatch) // Specifically uses identity
            {
                numToRemove++;
            }
        }
        return numToRemove;
    }
}
