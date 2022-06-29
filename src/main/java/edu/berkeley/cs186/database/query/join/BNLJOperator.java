package edu.berkeley.cs186.database.query.join;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterator;
import edu.berkeley.cs186.database.query.JoinOperator;
import edu.berkeley.cs186.database.query.QueryOperator;
import edu.berkeley.cs186.database.table.Record;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Performs an equijoin between two relations on leftColumnName and
 * rightColumnName respectively using the Block Nested Loop Join algorithm.
 */
public class BNLJOperator extends JoinOperator {
    protected int numBuffers;

    public BNLJOperator(QueryOperator leftSource,
                        QueryOperator rightSource,
                        String leftColumnName,
                        String rightColumnName,
                        TransactionContext transaction) {
        super(leftSource, materialize(rightSource, transaction),
                leftColumnName, rightColumnName, transaction, JoinType.BNLJ
        );
        this.numBuffers = transaction.getWorkMemSize();
        this.stats = this.estimateStats();
    }

    @Override
    public Iterator<Record> iterator() {
        return new BNLJIterator();
    }

    @Override
    public int estimateIOCost() {
        //This method implements the IO cost estimation of the Block Nested Loop Join
        int usableBuffers = numBuffers - 2;
        int numLeftPages = getLeftSource().estimateStats().getNumPages();
        int numRightPages = getRightSource().estimateIOCost();
        return ((int) Math.ceil((double) numLeftPages / (double) usableBuffers)) * numRightPages +
               getLeftSource().estimateIOCost();
    }

    /**
     * A record iterator that executes the logic for a simple nested loop join.
     * Look over the implementation in SNLJOperator if you want to get a feel
     * for the fetchNextRecord() logic.
     */

    /**
     * 枚举左表的一个block, 枚举右表的一个page，对于block中每一条record,遍历一遍右表的page
     * 当右表读入新page，block要回溯到第一条record
     * 所以，每次读入新的block、page 都需要对迭代器进行markNext()
     */
    private class BNLJIterator implements Iterator<Record>{
        // Iterator over all the records of the left source
        private Iterator<Record> leftSourceIterator;
        // Iterator over all the records of the right source
        private BacktrackingIterator<Record> rightSourceIterator;
        // Iterator over records in the current block of left pages
        private BacktrackingIterator<Record> leftBlockIterator;
        // Iterator over records in the current right page
        private BacktrackingIterator<Record> rightPageIterator;
        // The current record from the left relation
        private Record leftRecord;
        // The next record to return
        private Record nextRecord;

        private BNLJIterator() {
            super();
            this.leftSourceIterator = getLeftSource().iterator();
            this.fetchNextLeftBlock();
            this.rightSourceIterator = getRightSource().backtrackingIterator();
            this.rightSourceIterator.markNext();
            this.fetchNextRightPage();

            this.nextRecord = null;
        }

        /**
         * Fetch the next block of records from the left source.
         * leftBlockIterator should be set to a backtracking iterator over up to
         * B-2 pages of records from the left source, and leftRecord should be
         * set to the first record in this block.
         *
         * If there are no more records in the left source, this method should
         * do nothing.
         *
         * You may find QueryOperator#getBlockIterator useful here.
         */
        private void fetchNextLeftBlock() {
            // TODO(proj3_part1): implement
            leftBlockIterator = getBlockIterator(leftSourceIterator, getLeftSource().getSchema(), numBuffers - 2);
            leftBlockIterator.markNext();
        }

        /**
         * Fetch the next page of records from the right source.
         * rightPageIterator should be set to a backtracking iterator over up to
         * one page of records from the right source.
         *
         * If there are no more records in the right source, this method should
         * do nothing.
         *
         * You may find QueryOperator#getBlockIterator useful here.
         */
        private void fetchNextRightPage() {
            // TODO(proj3_part1): implement
            rightPageIterator = getBlockIterator(rightSourceIterator, getRightSource().getSchema(), 1);
            rightPageIterator.markNext();
        }

        /**
         * Returns the next record that should be yielded from this join,
         * or null if there are no more records to join.
         *
         * You may find JoinOperator#compare useful here. (You can call compare
         * function directly from this file, since BNLJOperator is a subclass
         * of JoinOperator).
         */

        /** 左表读取新的record时 有三种情况 需要分类讨论
         * 1. 左表的block仍有record剩余，此时直接将右表page reset一下 并更新leftRecord即可
         * 2. block无record剩余，且右表仍有page剩余，此时对于当前block需要继续匹配右边剩余的page
         * 3. block无record剩余，右表也无page剩余, 此时左表需要读取新的block, 并将右表reset
         */
        private void getLeftRecord() {
            // 1.
            if (leftBlockIterator.hasNext()) {
                leftRecord = leftBlockIterator.next();
                rightPageIterator.reset();
            }
            //2. 对于当前page, 左表block所有record都遍历了一次 我们读取右表下一个page
            //3. 如果右表已经穷尽了, 读取下一个block 并回溯到右表初始位置 读取第一个page
            else {
                if (!rightSourceIterator.hasNext()) {
                    rightSourceIterator.reset();
                    fetchNextRightPage();
                    fetchNextLeftBlock();
                }
                else {
                    fetchNextRightPage();
                    leftBlockIterator.reset();
                }
                //此时如果左表已经遍历完了, leftRecord的值为null
                if (leftBlockIterator.hasNext()) {
                    leftRecord = leftBlockIterator.next();
                }
                else {
                    leftRecord = null;
                }
            }
        }
        private Record fetchNextRecord() {
            // TODO(proj3_part1): implement
            while (true) {
                if (leftRecord == null) {
                    getLeftRecord();
                }
                if (leftRecord == null) return null;
                if (rightPageIterator.hasNext()) {
                    Record rightRecord = rightPageIterator.next();
                    if (compare(leftRecord, rightRecord) == 0) {
                        return leftRecord.concat(rightRecord);
                    }
                }
                //对于当前leftRecord 右表page遍历结束, 更新leftRecord 分类讨论有三种情况
                else {
                    getLeftRecord();
                }
            }
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
    }
}
