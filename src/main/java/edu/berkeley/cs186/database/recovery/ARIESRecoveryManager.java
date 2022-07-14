package edu.berkeley.cs186.database.recovery;

import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.common.Pair;
import edu.berkeley.cs186.database.concurrency.DummyLockContext;
import edu.berkeley.cs186.database.io.DiskSpaceManager;
import edu.berkeley.cs186.database.memory.BufferManager;
import edu.berkeley.cs186.database.memory.Page;
import edu.berkeley.cs186.database.recovery.records.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * Implementation of ARIES. start!
 */
public class ARIESRecoveryManager implements RecoveryManager {
    // Disk space manager.
    DiskSpaceManager diskSpaceManager;
    // Buffer manager.
    BufferManager bufferManager;

    // Function to create a new transaction for recovery with a given
    // transaction number.
    private Function<Long, Transaction> newTransaction;

    // Log manager
    LogManager logManager;
    // Dirty page table (page number -> recLSN).
    Map<Long, Long> dirtyPageTable = new ConcurrentHashMap<>();
    // Transaction table (transaction number -> entry).
    // ATT中的事务只可能是RUNNING COMMITTING ABORTING RECOVERY_ABORTING 四种状态
    Map<Long, TransactionTableEntry> transactionTable = new ConcurrentHashMap<>();
    // true if redo phase of restart has terminated, false otherwise. Used
    // to prevent DPT entries from being flushed during restartRedo.
    boolean redoComplete;

    public ARIESRecoveryManager(Function<Long, Transaction> newTransaction) {
        this.newTransaction = newTransaction;
    }

    /**
     * Initializes the log; only called the first time the database is set up.
     * The master record should be added to the log, and a checkpoint should be
     * taken.
     */
    @Override
    public void initialize() {
        this.logManager.appendToLog(new MasterLogRecord(0));
        this.checkpoint();
    }

    /**
     * Sets the buffer/disk managers. This is not part of the constructor
     * because of the cyclic dependency between the buffer manager and recovery
     * manager (the buffer manager must interface with the recovery manager to
     * block page evictions until the log has been flushed, but the recovery
     * manager needs to interface with the buffer manager to write the log and
     * redo changes).
     * @param diskSpaceManager disk space manager
     * @param bufferManager buffer manager
     */
    @Override
    public void setManagers(DiskSpaceManager diskSpaceManager, BufferManager bufferManager) {
        this.diskSpaceManager = diskSpaceManager;
        this.bufferManager = bufferManager;
        this.logManager = new LogManager(bufferManager);
    }

    // Forward Processing //////////////////////////////////////////////////////

    /**
     * Called when a new transaction is started.
     *
     * The transaction should be added to the transaction table.
     *
     * @param transaction new transaction
     */
    @Override
    // 开始一个事务 放入ATT中
    public synchronized void startTransaction(Transaction transaction) {
        this.transactionTable.put(transaction.getTransNum(), new TransactionTableEntry(transaction));
    }
    /**
     * Called when a transaction is about to start committing.
     *
     * A commit record should be appended, the log should be flushed,
     * and the transaction table and the transaction status should be updated.
     *
     * @param transNum transaction being committed
     * @return LSN of the commit record
     */
    //set TXN' status to COMMITTING; update the TXN' lastLSN; update the ATT; flush the log before return
    @Override
    public long commit(long transNum) {
        // TODO(proj5): implement
        // 写一条committing日志 并更新事务的lastLSN
        long preLSN = transactionTable.get(transNum).lastLSN;
        LogRecord logRecord = new CommitTransactionLogRecord(transNum, preLSN);
        transactionTable.get(transNum).lastLSN = logManager.appendToLog(logRecord);
        transactionTable.get(transNum).transaction.setStatus(Transaction.Status.COMMITTING);
        // 返回前 让日志刷盘 因为commit的本质就是日志落盘
        logManager.flushToLSN(transactionTable.get(transNum).lastLSN);
        return transactionTable.get(transNum).lastLSN;
    }

    /**
     * Called when a transaction is set to be aborted.
     *
     * An abort record should be appended, and the transaction table and
     * transaction status should be updated. Calling this function should not
     * perform any rollbacks.
     *
     * @param transNum transaction being aborted
     * @return LSN of the abort record
     */
    //set TXN' status to ABORTING ; update the TXN' lastLSN; update the ATT
    @Override
    // start to abort ，no need to remove the TXN from ATT
    public long abort(long transNum) {
        // TODO(proj5): implement
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        // 更新ATT中事务的状态 -> ABORTING
        transactionEntry.transaction.setStatus(Transaction.Status.ABORTING);
        // 写一条ABORTING日志 并更新事务的lastLSN
        long preLSN = transactionEntry.lastLSN;
        LogRecord logRecorde = new AbortTransactionLogRecord(transNum, preLSN);
        transactionEntry.lastLSN = logManager.appendToLog(logRecorde);
        return transactionEntry.lastLSN;
    }

    /**
     * Called when a transaction is cleaning up; this should roll back
     * changes if the transaction is aborting (see the rollbackToLSN helper
     * function below).
     *
     * Any changes that need to be undone should be undone, the transaction should
     * be removed from the transaction table, the end record should be appended,
     * and the transaction status should be updated.
     *
     * @param transNum transaction to end
     * @return LSN of the end record
     */
    @Override
    // 正常运行的时候程序会自己调用end()
    // 在clean up() 中调用end()
    // 正常运行，end时 需要写一条END_TXN日志 将事务状态设为COMPLETE并移出ATT
    public long end(long transNum) {
        // TODO(proj5): implement
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        //如果是ABORTING 需要rollBack 回溯找到事务的第一条log
        if (transactionEntry.transaction.getStatus().equals(Transaction.Status.ABORTING)) {
            LogRecord firstLog = logManager.fetchLogRecord(transactionEntry.lastLSN);
            while (firstLog.getPrevLSN().isPresent()) {
                firstLog = logManager.fetchLogRecord(firstLog.getPrevLSN().get());
            }
            this.rollbackToLSN(transNum, firstLog.getLSN());
        }
        // 更新ATT中事务的状态 -> complete
        transactionEntry.transaction.setStatus(Transaction.Status.COMPLETE);
        // 写一条EndTransaction record日志 并更新事务的lastLSN
        long preLSN = transactionEntry.lastLSN;
        LogRecord logRecorde = new EndTransactionLogRecord(transNum, preLSN);
        transactionEntry.lastLSN = logManager.appendToLog(logRecorde);
        //最后要remove
        this.transactionTable.remove(transNum);
        return transactionEntry.lastLSN;
    }

    /**
     * Recommended helper function: performs a rollback of all of a
     * transaction's actions, up to (but not including) a certain LSN.
     * Starting with the LSN of the most recent record that hasn't been undone:
     * - while the current LSN is greater than the LSN we're rolling back to:
     *    - if the record at the current LSN is undoable:
     *       - Get a compensation log record (CLR) by calling undo on the record
     *       - Append the CLR
     *       - Call redo on the CLR to perform the undo
     *    - update the current LSN to that of the next record to undo
     *
     * Note above that calling .undo() on a record does not perform the undo, it
     * just creates the compensation log record.
     *
     * @param transNum transaction to perform a rollback for
     * @param LSN LSN to which we should rollback
     */
    // 每undo一条操作 就写一条对应的CLR 更新事务的lastRecordLSN 最后记得更新ATT
    private void rollbackToLSN(long transNum, long LSN) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        LogRecord lastRecord = logManager.fetchLogRecord(transactionEntry.lastLSN);
        //该事务的最后一条日志,写CLR后要维护
        long lastRecordLSN = lastRecord.getLSN();
        // Small optimization: if the last record is a CLR we can start rolling
        // back from the next record that hasn't yet been undone.
        long currentLSN = lastRecord.getUndoNextLSN().orElse(lastRecordLSN);
        // TODO(proj5) implement the rollback logic described above
        LogRecord currentRecord;
        while (currentLSN > LSN) {
            currentRecord = logManager.fetchLogRecord(currentLSN);
            if (currentRecord.isUndoable()) {
                // 细节 CLR的preLSN是所属事务的lastLSN
                LogRecord clr = currentRecord.undo(lastRecordLSN);
                // 维护lastRecordLSN
                lastRecordLSN = logManager.appendToLog(clr);
                clr.redo(this, diskSpaceManager, bufferManager);
            }
            // 小优化 如果currentRecord是CLR 会跳到上次未被回滚处 否则 跳到当前log的preLSN
            currentLSN = currentRecord.getUndoNextLSN().orElse(
                    currentRecord.getPrevLSN().orElse((long) -1));
        }
        //更新ATT
        transactionTable.get(transNum).lastLSN = lastRecordLSN;
    }

    /**
     * Called before a page is flushed from the buffer cache. This
     * method is never called on a log page.
     *
     * The log should be as far as necessary.
     *
     * @param pageLSN pageLSN of page about to be flushed
     */
    @Override
    public void pageFlushHook(long pageLSN) {
        logManager.flushToLSN(pageLSN);
    }

    /**
     * Called when a page has been updated on disk.
     *
     * As the page is no longer dirty, it should be removed from the
     * dirty page table.
     *
     * @param pageNum page number of page updated on disk
     */
    @Override
    public void diskIOHook(long pageNum) {
        if (redoComplete) dirtyPageTable.remove(pageNum);
    }

    /**
     * Called when a write to a page happens.
     *
     * This method is never called on a log page. Arguments to the before and after params
     * are guaranteed to be the same length.
     *
     * The appropriate log record should be appended, and the transaction table
     * and dirty page table should be updated accordingly.
     *
     * @param transNum transaction performing the write
     * @param pageNum page number of page being written
     * @param pageOffset offset into page where write begins
     * @param before bytes starting at pageOffset before the write
     * @param after bytes starting at pageOffset after the write
     * @return LSN of last record written to log
     */
    @Override
    // write操作前需要调用, 作用是加一条UPDATE日志
    // 写在buffer page中 所以日志不需要刷盘
    public long logPageWrite(long transNum, long pageNum, short pageOffset, byte[] before,
                             byte[] after) {
        assert (before.length == after.length);
        assert (before.length <= BufferManager.EFFECTIVE_PAGE_SIZE / 2);
        // TODO(proj5): implement
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);
        long preLSN = transactionEntry.lastLSN;
        LogRecord logRecord = new UpdatePageLogRecord(transNum, pageNum, preLSN, pageOffset,
                before, after);
        long LSN = logManager.appendToLog(logRecord);
        transactionTable.get(transNum).lastLSN = LSN;
        // 如果之前刷过盘, 即：不存在与DPT中，那么就更新上去
        if (!dirtyPageTable.containsKey(pageNum)) {
            dirtyPageTable.put(pageNum, LSN);
        }
        //不需要刷盘
        return LSN;
    }

    /**
     * Called when a new partition is allocated. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the partition is the log partition.
     *
     * The appropriate log record should be appended, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the allocation
     * @param partNum partition number of the new partition
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logAllocPart(long transNum, int partNum) {
        // Ignore if part of the log.
        if (partNum == 0) return -1L;
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new AllocPartLogRecord(transNum, partNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN
        transactionEntry.lastLSN = LSN;
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Called when a partition is freed. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the partition is the log partition.
     *
     * The appropriate log record should be appended, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the partition be freed
     * @param partNum partition number of the partition being freed
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logFreePart(long transNum, int partNum) {
        // Ignore if part of the log.
        if (partNum == 0) return -1L;

        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new FreePartLogRecord(transNum, partNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN
        transactionEntry.lastLSN = LSN;
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Called when a new page is allocated. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the page is in the log partition.
     *
     * The appropriate log record should be appended, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the allocation
     * @param pageNum page number of the new page
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logAllocPage(long transNum, long pageNum) {
        // Ignore if part of the log.
        if (DiskSpaceManager.getPartNum(pageNum) == 0) return -1L;

        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new AllocPageLogRecord(transNum, pageNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN
        transactionEntry.lastLSN = LSN;
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Called when a page is freed. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the page is in the log partition.
     *
     * The appropriate log record should be appended, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the page be freed
     * @param pageNum page number of the page being freed
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logFreePage(long transNum, long pageNum) {
        // Ignore if part of the log.
        if (DiskSpaceManager.getPartNum(pageNum) == 0) return -1L;

        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new FreePageLogRecord(transNum, pageNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN
        transactionEntry.lastLSN = LSN;
        dirtyPageTable.remove(pageNum);
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Creates a savepoint for a transaction. Creating a savepoint with
     * the same name as an existing savepoint for the transaction should
     * delete the old savepoint.
     *
     * The appropriate LSN should be recorded so that a partial rollback
     * is possible later.
     *
     * @param transNum transaction to make savepoint for
     * @param name name of savepoint
     */
    @Override
    public void savepoint(long transNum, String name) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);
        transactionEntry.addSavepoint(name);
    }

    /**
     * Releases (deletes) a savepoint for a transaction.
     * @param transNum transaction to delete savepoint for
     * @param name name of savepoint
     */
    @Override
    public void releaseSavepoint(long transNum, String name) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);
        transactionEntry.deleteSavepoint(name);
    }

    /**
     * Rolls back transaction to a savepoint.
     *
     * All changes done by the transaction since the savepoint should be undone,
     * in reverse order, with the appropriate CLRs written to log. The transaction
     * status should remain unchanged.
     *
     * @param transNum transaction to partially rollback
     * @param name name of savepoint
     */
    @Override
    // 调用rollbackToLSN即可
    public void rollbackToSavepoint(long transNum, String name) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        // All of the transaction's changes strictly after the record at LSN should be undone.
        long savepointLSN = transactionEntry.getSavepoint(name);

        // TODO(proj5): implement
        rollbackToLSN(transNum, savepointLSN);
    }

    /**
     * Create a checkpoint.
     *
     * First, a begin checkpoint record should be written.
     *
     * Then, end checkpoint records should be filled up as much as possible first
     * using recLSNs from the DPT, then status/lastLSNs from the transactions
     * table, and written when full (or when nothing is left to be written).
     * You may find the method EndCheckpointLogRecord#fitsInOneRecord here to
     * figure out when to write an end checkpoint record.
     *
     * Finally, the master record should be rewritten with the LSN of the
     * begin checkpoint record.
     */
    @Override
    public synchronized void checkpoint() {
        // Create begin checkpoint log record and write to log
        LogRecord beginRecord = new BeginCheckpointLogRecord();
        long beginLSN = logManager.appendToLog(beginRecord);
        Map<Long, Long> chkptDPT = new HashMap<>();
        Map<Long, Pair<Transaction.Status, Long>> chkptTxnTable = new HashMap<>();
        // TODO(proj5): generate end checkpoint record(s) for DPT and transaction table
        // 维护一下DPT ATT信息，相当于拷贝一份给ENDchekckpointLog
        // 但是由于ATT DPT信息可能太多了, 1个page装不下 需要在即将装满后就刷盘
        // Last end checkpoint record
        int dptEntries = 0, ATTEntries = 0; // 维护临时哈希表中的条目大小
        int cnt = 0;// 刷盘的endLog数量，保证不为0 即使两个表都空 也要写进去
        for (long id : dirtyPageTable.keySet()) {
            if (EndCheckpointLogRecord.fitsInOneRecord(dptEntries + 1, ATTEntries)) {
                chkptDPT.put(id, dirtyPageTable.get(id));
                dptEntries++;
                continue;
            }
            // 不能加了 刷盘后再加
            LogRecord endRecord = new EndCheckpointLogRecord(chkptDPT, chkptTxnTable);
            chkptDPT.clear();
            logManager.appendToLog(endRecord);
            flushToLSN(endRecord.getLSN());
            cnt++;
            dptEntries = 0;

            chkptDPT.put(id, dirtyPageTable.get(id));
            dptEntries++;
        }
        // 此时chkptDPT中也许还有entries 继续添加ATT entry直至填满
        for (long id : transactionTable.keySet()) {
            // 对应的条目
            Pair<Transaction.Status, Long> t = new Pair<>(transactionTable.get(id).transaction.getStatus(),
                    transactionTable.get(id).lastLSN);
            if (EndCheckpointLogRecord.fitsInOneRecord(dptEntries, ATTEntries + 1)) {
                chkptTxnTable.put(id, t);
                ATTEntries++;
                continue;
            }
            // 不能加了 刷盘后再加
            LogRecord endRecord = new EndCheckpointLogRecord(chkptDPT, chkptTxnTable);
            chkptDPT.clear();
            chkptTxnTable.clear();
            logManager.appendToLog(endRecord);
            flushToLSN(endRecord.getLSN());
            cnt++;
            ATTEntries = 0;
            dptEntries = 0;
            chkptTxnTable.put(id, t);
            ATTEntries++;
        }
        // 此时 要么cnt==0, 要么还有ATT DPT条目剩余
        // 所以我们要进行最后一次日志刷盘
        LogRecord endRecord = new EndCheckpointLogRecord(chkptDPT, chkptTxnTable);
        logManager.appendToLog(endRecord);
        flushToLSN(endRecord.getLSN());
        // Update master record
        MasterLogRecord masterRecord = new MasterLogRecord(beginLSN);
        logManager.rewriteMasterRecord(masterRecord);
    }

    /**
     * Flushes the log to at least the specified record,
     * essentially flushing up to and including the page
     * that contains the record specified by the LSN.
     *
     * @param LSN LSN up to which the log should be flushed
     */
    @Override
    public void flushToLSN(long LSN) {
        this.logManager.flushToLSN(LSN);
    }

    @Override
    public void dirtyPage(long pageNum, long LSN) {
        dirtyPageTable.putIfAbsent(pageNum, LSN);
        // Handle race condition where earlier log is beaten to the insertion by
        // a later log.
        dirtyPageTable.computeIfPresent(pageNum, (k, v) -> Math.min(LSN,v));
    }

    @Override
    public void close() {
        this.checkpoint();
        this.logManager.close();
    }

    // Restart Recovery ////////////////////////////////////////////////////////

    /**
     * Called whenever the database starts up, and performs restart recovery.
     * Recovery is complete when the Runnable returned is run to termination.
     * New transactions may be started once this method returns.
     *
     * This should perform the three phases of recovery, and also clean the
     * dirty page table of non-dirty pages (pages that aren't dirty in the
     * buffer manager) between redo and undo, and perform a checkpoint after
     * undo.
     */
    //故障后重启的过程
    @Override
    public void restart() {
        this.restartAnalysis();
        this.restartRedo();
        this.redoComplete = true;
        this.cleanDPT();
        this.restartUndo();
        this.checkpoint();
    }
    private void updateTXNLastLSN(TransactionTableEntry transactionEntry, long LSN) {
        transactionEntry.lastLSN = Math.max(transactionEntry.lastLSN, LSN);
    }
    //analysis phase, check the logRecord and update ATT
    private void updateATTAndDPT(LogRecord logRecord, TransactionTableEntry transactionEntry,
                           Set<Long> endedTransactions) {
        // 更新ATT
        long LSN = logRecord.getLSN();
        updateTXNLastLSN(transactionEntry, LSN);
        // TXN status
        // COMMITTING
        if (logRecord.getType().equals(LogType.COMMIT_TRANSACTION)) {
            transactionEntry.transaction.setStatus(Transaction.Status.COMMITTING);
        }
        // 此时是在restart的时候 设为RECOVERY_ABORTING
        else if (logRecord.getType().equals(LogType.ABORT_TRANSACTION)) {
            transactionEntry.transaction.setStatus(Transaction.Status.RECOVERY_ABORTING);
        }
        // 事务 END, 状态设为COMPLETE并移出ATT
        // 需要手动clean up()一下
        else if (logRecord.getType().equals(LogType.END_TRANSACTION)) {
            transactionEntry.transaction.cleanup();
            transactionEntry.transaction.setStatus(Transaction.Status.COMPLETE);
            assert(logRecord.getTransNum().isPresent());
            endedTransactions.add(logRecord.getTransNum().get());
            transactionTable.remove(logRecord.getTransNum().get());
        }

        if(logRecord.getPageNum().isPresent()) {
            Long pageNum = logRecord.getPageNum().get();
            if (logRecord.getType().equals(LogType.UPDATE_PAGE) || logRecord.getType().equals(LogType.UNDO_UPDATE_PAGE)) {
                // 对Page进行了更新, 若DPT中不存在对应Page应该加入到DPT中
                this.dirtyPageTable.putIfAbsent(pageNum, LSN);
            }
            if (logRecord.getType().equals(LogType.FREE_PAGE) ||logRecord.getType().equals(LogType.UNDO_ALLOC_PAGE)) {
                // 相当于刷新了(删除了)Page, 也要从DPT中删掉
                this.dirtyPageTable.remove(pageNum);
            }
            // don't need to do anything for AllocPage / UndoFreePage
        }
    }
    /**
     * util for judging the before and after for the Transaction Status
     * @param status1
     * @param status2
     * @return status1 < status2
     */
    private boolean transactionStatusBefore (Transaction.Status status1, Transaction.Status status2) {
        if (status1.equals(Transaction.Status.RUNNING)) {
            return status2.equals(Transaction.Status.COMMITTING) || status2.equals(Transaction.Status.COMPLETE)|| status2.equals(Transaction.Status.ABORTING);
        } else if (status1.equals(Transaction.Status.COMMITTING) || status1.equals(Transaction.Status.ABORTING)) {
            return status2.equals(Transaction.Status.COMPLETE);
        } else {
            return false;
        }
    }
    /**
     * This method performs the analysis pass of restart recovery.
     *
     * First, the master record should be read (LSN 0). The master record contains
     * one piece of information: the LSN of the last successful checkpoint.
     *
     * We then begin scanning log records, starting at the beginning of the
     * last successful checkpoint.
     *
     * If the log record is for a transaction operation (getTransNum is present)
     * - update the transaction table
     *
     * If the log record is page-related (getPageNum is present), update the dpt
     *   - update/undoupdate page will dirty pages
     *   - free/undoalloc page always flush changes to disk
     *   - no action needed for alloc/undofree page
     *
     * If the log record is for a change in transaction status:
     * - update transaction status to COMMITTING/RECOVERY_ABORTING/COMPLETE
     * - update the transaction table
     * - if END_TRANSACTION: clean up transaction (Transaction#cleanup), remove
     *   from txn table, and add to endedTransactions
     *
     * If the log record is an end_checkpoint record:
     * - Copy all entries of checkpoint DPT (replace existing entries if any)
     * - Skip txn table entries for transactions that have already ended
     * - Add to transaction table if not already present
     * - Update lastLSN to be the larger of the existing entry's (if any) and
     *   the checkpoint's
     * - The status's in the transaction table should be updated if it is possible
     *   to transition from the status in the table to the status in the
     *   checkpoint. For example, running -> aborting is a possible transition,
     *   but aborting -> running is not.
     *
     * After all records in the log are processed, for each ttable entry:
     *  - if COMMITTING: clean up the transaction, change status to COMPLETE,
     *    remove from the ttable, and append an end record
     *  - if RUNNING: change status to RECOVERY_ABORTING, and append an abort
     *    record
     *  - if RECOVERY_ABORTING: no action needed
     */
    // 从最近一次存档点BEGIN日志开始遍历日志队列
    // 如果遇到END_CHKP, 需要把该record中的ATT DPT与当前内存的DPT ATT整合起来
    // 遇到别的,如果对ATT DPT有影响，就要进行相应的维护
    // 如果logRecord存在getTransNum (getTransNum is present), 就说明对ATT有影响，DPT同理
    void restartAnalysis() {
        // Read master record, master record LSN 就是0
        LogRecord record = logManager.fetchLogRecord(0L);
        // Type checking
        assert (record != null && record.getType() == LogType.MASTER);
        MasterLogRecord masterRecord = (MasterLogRecord) record;
        // Get start checkpoint LSN 上一次checkpoint处
        long LSN = masterRecord.lastCheckpointLSN;
        // Set of transactions that have completed
        // 由于COMPLETE之后会被移除ATT 所以需要额外一个表来记录一下 防止事务二次clean up()
        Set<Long> endedTransactions = new HashSet<>();
        // TODO(proj5): implement
        Iterator<LogRecord> it = logManager.scanFrom(LSN);
        while (it.hasNext()) {
            LogRecord cur = it.next();
            // 特殊处理 将END_CHKP中的DPT ATT与内存中的整合起来
            if (cur.getType().equals(LogType.END_CHECKPOINT)) {
                // DPT
                Map<Long, Long> checkpointDirtyPageTable = cur.getDirtyPageTable();
                for (Long tranNum : checkpointDirtyPageTable.keySet()) {
                    // 因为checkpoing记录的recLSN一定是最早的
                    if (!dirtyPageTable.containsKey(tranNum)) {
                        dirtyPageTable.put(tranNum, checkpointDirtyPageTable.get(tranNum));
                    } else {
                        dirtyPageTable.put(tranNum, Math.min(checkpointDirtyPageTable.get(tranNum), dirtyPageTable.get(tranNum)));
                    }
                }
                // ATT
                Map<Long, Pair<Transaction.Status, Long>> transTable = cur.getTransactionTable();
                for (long transNum : transTable.keySet()) {
                    // 已经COMPLETE的事务不需要加
                    // 因为END_CHKP的ATT中的TXN可能已经COMPLETE了 此时如果
                    if (endedTransactions.contains(transNum)) {
                       // System.out.println("TXN COMPLETED:" + transNum);
                        continue;
                    }
                    // 如果END_CHKP中的ATT中的事务 不在内存ATT中，需要加上去
                    if (!transactionTable.containsKey(transNum)) {
                        Transaction t = newTransaction.apply(transNum);
                        // 记得先修改status 再加给ATT
                        t.setStatus(transTable.get(transNum).getFirst());
                        this.startTransaction(t);
                        transactionTable.get(transNum).lastLSN = transTable.get(transNum).getSecond();
                    }
                    // 如果存在于内存ATT 更新lastLSN, status
                    // 此处可能会加入COMPLETE?
                    else {
                        updateTXNLastLSN(transactionTable.get(transNum), transTable.get(transNum).getSecond());
                        if (transactionStatusBefore(transactionTable.get(transNum).transaction.getStatus()
                                ,Transaction.Status.RUNNING)) {
                            transactionTable.get(transNum).transaction.setStatus(transTable.get(transNum).getFirst());
                        }
                        if (transactionTable.get(transNum).transaction.getStatus().equals(Transaction.Status.ABORTING)) {
                            transactionTable.get(transNum).transaction.setStatus(Transaction.Status.RECOVERY_ABORTING);
                        }
                    }
                }
                continue;
            }
            // 如果不会影响ATT DPT 就不用管
            // 根据文档 这俩属性用来判断该日志是否对ATT DPT有影响
            if (!cur.getTransNum().isPresent() && !cur.getPageNum().isPresent()) {
                continue;
            }
            long transNum = cur.getTransNum().get();
            // 事务不存在 new一个新事务
            if (!transactionTable.containsKey(transNum)) {
                Transaction t = newTransaction.apply(transNum);
                t.setStatus(Transaction.Status.RUNNING);
                this.startTransaction(t);
                transactionTable.get(transNum).lastLSN = cur.getLSN();
            }
            if (endedTransactions.contains(transNum)) continue;
            updateATTAndDPT(cur, transactionTable.get(transNum), endedTransactions);
        }
        // 此时已经扫描完了全部的log
        // 遍历一下ATT 将COMMITTING 的事务结束,ABORTING RUNNING的事务设为RECOVERY_ABORTING
        for (long transNum : transactionTable.keySet()) {
            TransactionTableEntry t = transactionTable.get(transNum);
            //RUNNING -> RECOVERY_ABORTING
            // ABORT log在此处写
            if (t.transaction.getStatus().equals(Transaction.Status.RUNNING)) {
                LogRecord logRecorde = new AbortTransactionLogRecord(transNum, t.lastLSN);
                t.lastLSN = logManager.appendToLog(logRecorde);
                t.transaction.setStatus(Transaction.Status.RECOVERY_ABORTING);
            }
            //COMMITTING -> COMPLETE
            else if (t.transaction.getStatus().equals(Transaction.Status.COMMITTING)) {
                // 需要手动clean up()
                t.transaction.cleanup();
                t.transaction.setStatus(Transaction.Status.COMPLETE);
                LogRecord logRecorde = new EndTransactionLogRecord(transNum, t.lastLSN);
                t.lastLSN = logManager.appendToLog(logRecorde);
                this.transactionTable.remove(transNum);
                endedTransactions.add(transNum);
            }
            // ABORTING -> RECOVERY_ABORTING
            else if (t.transaction.getStatus().equals(Transaction.Status.ABORTING)) {
                t.transaction.setStatus(Transaction.Status.RECOVERY_ABORTING);
            }
        }
    }

    /**
     * This method performs the redo pass of restart recovery.
     *
     * First, determine the starting point for REDO from the dirty page table.
     *
     * Then, scanning from the starting point, if the record is redoable and
     * - partition-related (Alloc/Free/UndoAlloc/UndoFree..Part), always redo it
     * - allocates a page (AllocPage/UndoFreePage), always redo it
     * - modifies a page (Update/UndoUpdate/Free/UndoAlloc....Page) in
     *   the dirty page table with LSN >= recLSN, the page is fetched from disk,
     *   the pageLSN is checked, and the record is redone if needed.
     */
    // 如果跟分区相关, redo
    // allocate a page, redo
    // modify a page, page in DPT && recLSN <= curLSN && diskPageLSN<curLSN, redo
    // (page在DPT中, recLSN<=curLSN, 该page对应的磁盘中的page的pageLSN<curLSN
    void restartRedo() {
        // TODO(proj5): implement
        if (dirtyPageTable.isEmpty()) return ;
        long startLSN = dirtyPageTable.values().iterator().next();
        for (long recLSN : dirtyPageTable.values()) {
            startLSN = Math.min(startLSN, recLSN);
        }
        Iterator<LogRecord> it = logManager.scanFrom(startLSN);
        while (it.hasNext()) {
            LogRecord cur = it.next();
            switch (cur.getType()) {
                case UNDO_UPDATE_PAGE:
                case FREE_PAGE:
                case UNDO_ALLOC_PAGE:
                case UPDATE_PAGE:
                    assert(cur.getPageNum().isPresent());
                    if (dirtyPageTable.containsKey(cur.getPageNum().get())
                    && dirtyPageTable.get(cur.getPageNum().get()) <= cur.getLSN()) {
                        Page page = bufferManager.fetchPage(new DummyLockContext(), cur.getPageNum().get());
                        boolean f = true;
                        try {
                            if (page.getPageLSN() >= cur.getLSN()) f = false;
                        } finally {
                            page.unpin();
                        }
                        if (f) {
                            cur.redo(this, diskSpaceManager, bufferManager);
                        }
                    }
                    break;
                case ALLOC_PART:
                case UNDO_FREE_PART:
                case FREE_PART:
                case UNDO_ALLOC_PART:
                case ALLOC_PAGE:
                case UNDO_FREE_PAGE:
                    cur.redo(this, diskSpaceManager, bufferManager);
                    break;
            }
        }
    }

    /**
     * This method performs the undo pass of restart recovery.

     * First, a priority queue is created sorted on lastLSN of all aborting
     * transactions.
     *
     * Then, always working on the largest LSN in the priority queue until we are done,
     * - if the record is undoable, undo it, and append the appropriate CLR
     * - replace the entry with a new one, using the undoNextLSN if available,
     *   if the prevLSN otherwise.
     * - if the new LSN is 0, clean up the transaction, set the status to complete,
     *   and remove from transaction table.
     */
    // 用优先队列进行回溯 每次取出最大的LSN 往上走一格 (undo一条操作前记得写CLR日志
    void restartUndo() {
        // TODO(proj5): implement
        Queue<Pair<Long, LogRecord>> q = new PriorityQueue<>(new PairFirstReverseComparator<>());
        for (long id : transactionTable.keySet()) {
            if (transactionTable.get(id).transaction.getStatus().equals(Transaction.Status.RECOVERY_ABORTING)) {
                long lsn = transactionTable.get(id).lastLSN;
                LogRecord logRecord = logManager.fetchLogRecord(lsn);
                q.add(new Pair<>(lsn, logRecord));
            }
        }
        while (q.size() > 0) {
            Pair<Long, LogRecord> pk = q.poll();
            LogRecord cur = pk.getSecond();
            assert(cur.getTransNum().isPresent());
            long lastLSN = transactionTable.get(cur.getTransNum().get()).lastLSN;
            // UPDATE record, undo一下
            if (cur.isUndoable()) {
                LogRecord clr = cur.undo(lastLSN);
                transactionTable.get(cur.getTransNum().get()).lastLSN = logManager.appendToLog(clr);
                // WA点, 写一条CLR后记得更新lastLSN 不然后面写END_TXN的时候会出错
                lastLSN = transactionTable.get(cur.getTransNum().get()).lastLSN;
                clr.redo(this, diskSpaceManager, bufferManager);
            }
            // 不是UPDATE, 如果undoNextLSN存在 就是CLR
            long nextLSN = 0;
            if (cur.getUndoNextLSN().isPresent()) {
                nextLSN = cur.getUndoNextLSN().get();
            }
            else if (cur.getPrevLSN().isPresent()) {
                nextLSN = cur.getPrevLSN().get();
            }
            // 该事务结束了,没有上一条了
            if (nextLSN == 0) {
                LogRecord end_TXN = new EndTransactionLogRecord(cur.getTransNum().get(), lastLSN);
                transactionTable.get(cur.getTransNum().get()).lastLSN = logManager.appendToLog(end_TXN);
                transactionTable.get(cur.getTransNum().get()).transaction.cleanup();
                transactionTable.get(cur.getTransNum().get()).transaction.setStatus(
                        Transaction.Status.COMPLETE);
                transactionTable.remove(cur.getTransNum().get());
            }
            // 事务没结束
            else {
                q.add(new Pair<>(nextLSN, logManager.fetchLogRecord(nextLSN)));
            }
        }
    }

    /**
     * Removes pages from the DPT that are not dirty in the buffer manager.
     * This is slow and should only be used during recovery.
     */
    void cleanDPT() {
        Set<Long> dirtyPages = new HashSet<>();
        bufferManager.iterPageNums((pageNum, dirty) -> {
            if (dirty) dirtyPages.add(pageNum);
        });
        Map<Long, Long> oldDPT = new HashMap<>(dirtyPageTable);
        dirtyPageTable.clear();
        for (long pageNum : dirtyPages) {
            if (oldDPT.containsKey(pageNum)) {
                dirtyPageTable.put(pageNum, oldDPT.get(pageNum));
            }
        }
    }

    // Helpers /////////////////////////////////////////////////////////////////
    /**
     * Comparator for Pair<A, B> comparing only on the first element (type A),
     * in reverse order.
     */
    private static class PairFirstReverseComparator<A extends Comparable<A>, B> implements
            Comparator<Pair<A, B>> {
        @Override
        public int compare(Pair<A, B> p0, Pair<A, B> p1) {
            return p1.getFirst().compareTo(p0.getFirst());
        }
    }

}
