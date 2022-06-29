package edu.berkeley.cs186.database.query.join;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterator;
import edu.berkeley.cs186.database.query.JoinOperator;
import edu.berkeley.cs186.database.query.MaterializeOperator;
import edu.berkeley.cs186.database.query.QueryOperator;
import edu.berkeley.cs186.database.query.SortOperator;
import edu.berkeley.cs186.database.table.Record;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class SortMergeOperator extends JoinOperator {
    public SortMergeOperator(QueryOperator leftSource,
                             QueryOperator rightSource,
                             String leftColumnName,
                             String rightColumnName,
                             TransactionContext transaction) {
        super(prepareLeft(transaction, leftSource, leftColumnName),
              prepareRight(transaction, rightSource, rightColumnName),
              leftColumnName, rightColumnName, transaction, JoinType.SORTMERGE);
        this.stats = this.estimateStats();
    }

    /**
     * If the left source is already sorted on the target column then this
     * returns the leftSource, otherwise it wraps the left source in a sort
     * operator.
     */
    private static QueryOperator prepareLeft(TransactionContext transaction,
                                             QueryOperator leftSource,
                                             String leftColumn) {
        leftColumn = leftSource.getSchema().matchFieldName(leftColumn);
        if (leftSource.sortedBy().contains(leftColumn)) return leftSource;
        return new SortOperator(transaction, leftSource, leftColumn);
    }

    /**
     * If the right source isn't sorted, wraps the right source in a sort
     * operator. Otherwise, if it isn't materialized, wraps the right source in
     * a materialize operator. Otherwise, simply returns the right source. Note
     * that the right source must be materialized since we may need to backtrack
     * over it, unlike the left source.
     */
    //右表的record涉及回溯操纵，如果只是普通的迭代器，是不支持回溯的，需要可回溯迭代器(BacktrackingIterator)
    //只有物质化的operator才支持可回溯迭代器 (可回溯迭代器其实就是把整段数据写进内存 以数组形式)
    private static QueryOperator prepareRight(TransactionContext transaction,
                                              QueryOperator rightSource,
                                              String rightColumn) {
        rightColumn = rightSource.getSchema().matchFieldName(rightColumn);
        if (!rightSource.sortedBy().contains(rightColumn)) {
            return new SortOperator(transaction, rightSource, rightColumn);
        } else if (!rightSource.materialized()) {
            return new MaterializeOperator(rightSource, transaction);
        }
        return rightSource;
    }

    @Override
    public Iterator<Record> iterator() {
        return new SortMergeIterator();
    }

    @Override
    public List<String> sortedBy() {
        return Arrays.asList(getLeftColumnName(), getRightColumnName());
    }

    @Override
    public int estimateIOCost() {
        //does nothing
        return 0;
    }

    /**
     * An implementation of Iterator that provides an iterator interface for this operator.
     *    See lecture slides.
     *
     * Before proceeding, you should read and understand SNLJOperator.java
     *    You can find it in the same directory as this file.
     *
     * Word of advice: try to decompose the problem into distinguishable sub-problems.
     *    This means you'll probably want to add more methods than those given (Once again,
     *    SNLJOperator.java might be a useful reference).
     *
     */
    private class SortMergeIterator implements Iterator<Record> {
        /**
        * Some member variables are provided for guidance, but there are many possible solutions.
        * You should implement the solution that's best for you, using any member variables you need.
        * You're free to use these member variables, but you're not obligated to.
        */
        private Iterator<Record> leftIterator;
        private BacktrackingIterator<Record> rightIterator;
        private Record leftRecord;
        private Record nextRecord;
        private Record rightRecord;
        private boolean marked;

        private SortMergeIterator() {
            super();
            leftIterator = getLeftSource().iterator();
            rightIterator = getRightSource().backtrackingIterator();
            rightIterator.markNext();

            if (leftIterator.hasNext() && rightIterator.hasNext()) {
                leftRecord = leftIterator.next();
                rightRecord = rightIterator.next();
            }

            this.marked = false;
        }

        /**
         * @return true if this iterator has another record to yield, otherwise
         * false
         */
        @Override
        public boolean hasNext() {
            if (this.nextRecord == null) this.nextRecord = fetchNextRecord();
            return this.nextRecord != null;
        }

        /**
         * @return the next record from this iterator
         * @throws NoSuchElementException if there are no more records to yield
         */
        @Override
        public Record next() {
            if (!this.hasNext()) throw new NoSuchElementException();
            Record nextRecord = this.nextRecord;
            this.nextRecord = null;
            return nextRecord;
        }

        /**
         *
         * @param leftRecord
         * @param rightRecord
         * @return true if these two tables have matched tuples
         */
        //找到第一条匹配
        private boolean checkEqualRecordsLeft() {
            if (leftRecord == null || rightRecord == null) return false;
            //如果找不到第一条匹配 说明两个表能匹配的record已经全部找完了
            while (compare(leftRecord, rightRecord) != 0) {
                int f = compare(leftRecord, rightRecord);
                if (f > 0) {
                    if (rightIterator.hasNext()) {
                        rightRecord = rightIterator.next();
                    }
                    else {
                        return false;
                    }
                }
                else {
                    if (leftIterator.hasNext()) {
                        leftRecord = leftIterator.next();
                    }
                    else {
                        return false;
                    }
                }
            }
            assert(compare(leftRecord, rightRecord) == 0);
            this.marked = true;
            return true;
        }
        private Record getNextRecord(Iterator<Record> it) {
            if (it.hasNext()) {
                return it.next();
            }
            return null;
        }
        /**
         * Returns the next record that should be yielded from this join,
         * or null if there are no more records to join.
         */
        /**
         * 先调整左右表指针，使得处于右指针第一条的匹配阶段,标记marked=true 右迭代器markPrev 表示此时开启一段匹配
         * 只要是marked==true,不断地去右表匹配中 直到匹配失败(此时右表指针可能为null)
         * 此时需要左指针下移 右指针回溯,marked=false
         * 如果marked == false,我们继续去找第一条匹配
         */
        private Record fetchNextRecord() {
            while (true) {
                //此时是仍处于右边的一段匹配
                if (marked) {
                    //1.匹配失败 左表指针下移 右表指针回溯
                    if (rightRecord == null || compare(leftRecord, rightRecord) != 0) {
                        leftRecord = getNextRecord(leftIterator);
                        rightIterator.reset();
                        rightRecord = rightIterator.next();
                        marked = false;
                    }
                    //匹配成功 继续匹配
                    else {
                        assert(compare(leftRecord, rightRecord) == 0);
                        Record ret = leftRecord.concat(rightRecord);
                        rightRecord = getNextRecord(rightIterator);
                        return ret;
                    }
                }
                else {
                    //先找到第一处匹配 没找到的话就是null
                    if (!checkEqualRecordsLeft()) {
                        return null;
                    }
                    assert(compare(leftRecord, rightRecord) == 0);
                    Record ret = leftRecord.concat(rightRecord);
                    //此时是ret第一条匹配
                    rightIterator.markPrev();
                    rightRecord = getNextRecord(rightIterator);
                    return ret;
                }
            }
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}
