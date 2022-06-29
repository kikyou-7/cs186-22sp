package edu.berkeley.cs186.database.common.iterator;

import java.util.List;

/**
 * Backtracking iterator over an array.
 * 可回溯迭代器, 本质是将迭代对象记忆化到一个数组中
 */
public class ArrayBacktrackingIterator<T> extends IndexBacktrackingIterator<T> {
    protected T[] array;

    public ArrayBacktrackingIterator(T[] array) {
        super(array.length);
        this.array = array;
    }

    public ArrayBacktrackingIterator(List<T> list) {
        this((T[]) list.toArray());
    }

    @Override
    protected int getNextNonEmpty(int currentIndex) {
        return currentIndex + 1;
    }

    @Override
    protected T getValue(int index) {
        return this.array[index];
    }
}

