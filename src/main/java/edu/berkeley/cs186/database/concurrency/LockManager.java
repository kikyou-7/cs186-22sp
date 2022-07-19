package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.TransactionContext;

import java.util.*;

/**
 * LockManager maintains the bookkeeping for what transactions have what locks
 * on what resources and handles queuing logic. The lock manager should generally
 * NOT be used directly: instead, code should call methods of LockContext to
 * acquire/release/promote/escalate locks.
 *
 * The LockManager is primarily concerned with the mappings between
 * transactions, resources, and locks, and does not concern itself with multiple
 * levels of granularity. Multigranularity is handled by LockContext instead.
 *
 * Each resource the lock manager manages has its own queue of LockRequest
 * objects representing a request to acquire (or promote/acquire-and-release) a
 * lock that could not be satisfied at the time. This queue should be processed
 * every time a lock on that resource gets released, starting from the first
 * request, and going in order until a request cannot be satisfied. Requests
 * taken off the queue should be treated as if that transaction had made the
 * request right after the resource was released in absence of a queue (i.e.
 * removing a request by T1 to acquire X(db) should be treated as if T1 had just
 * requested X(db) and there were no queue on db: T1 should be given the X lock
 * on db, and put in an unblocked state via Transaction#unblock).
 *
 * This does mean that in the case of:
 *    queue: S(A) X(A) S(A)
 * only the first request should be removed from the queue when the queue is
 * processed.
 */
public class LockManager {
    // transactionLocks is a mapping from transaction number to a list of lock
    // objects held by that transaction.

    // 每个事务 拥有的锁的集合
    // 在grant release之后记得更新维护
    private Map<Long, List<Lock>> transactionLocks = new HashMap<>();

    // resourceEntries is a mapping from resource names to a ResourceEntry
    // object, which contains a list of Locks on the object, as well as a
    // queue for requests on that resource.

    //source对应的锁的hash表
    private Map<ResourceName, ResourceEntry> resourceEntries = new HashMap<>();

    // wait-for graph 事务只能被一个事务阻塞
    // 图中的点代表一个被阻塞的事务, 在resource的waitingQueue中挂起了请求
    // 当事务的请求入队时,将事务加入图中; 出队则从图中删掉
    HashMap<Long, Long> waitForGraph = new HashMap<>();
    // 监控死锁的后台线程  在构造方法中创建
    Thread deadLockChecker;
    // 监控死锁的后台线程是否正在运作
    boolean checkIsRunning = false;
    // 当前活跃的事务
    private Map<Long, Transaction> runningTransaction = new HashMap<>();

    // A ResourceEntry contains the list of locks on a resource, as well as
    // the queue for requests for locks on the resource.
    private class ResourceEntry {
        // List of currently granted locks on the resource.
        List<Lock> locks = new ArrayList<>();
        // Queue for yet-to-be-satisfied lock requests on this resource.
        Deque<LockRequest> waitingQueue = new ArrayDeque<>();

        // Below are a list of helper methods we suggest you implement.
        // You're free to modify their type signatures, delete, or ignore them.

        /**
         * Check if `lockType` is compatible with preexisting locks. Allows
         * conflicts for locks held by transaction with id `except`, which is
         * useful when a transaction tries to replace a lock it already has on
         * the resource.
         */
        public boolean checkCompatible(LockType lockType, long except) {
            // TODO(proj4_part1): implement
            for (Lock lock : locks) {
                if (!LockType.compatible(lockType, lock.lockType) &&
                        lock.transactionNum != except) {
                    return false;
                }
            }
            return true;
        }
        // 返回一个与lockType不兼容的锁
        public Lock getIncompatibleLock(LockType lockType, long except) {
            for (Lock lock : locks) {
                if (lock.transactionNum == except) continue;
                if (!LockType.compatible(lock.lockType, lockType)) {
                    return lock;
                }
            }
            throw new IllegalStateException("no Incompatible locks!");
        }
        //维护lock被删除/授予后 事务的hash表
        private void processTransactionLocks(Lock lock, boolean delete) {
            long transNum = lock.transactionNum;
            //删除lock
            if (delete) {
                assert(transactionLocks.containsKey(transNum) &&
                        transactionLocks.get(transNum).contains(lock));
                transactionLocks.get(transNum).remove(lock);
                if (transactionLocks.get(transNum).isEmpty()) {
                    transactionLocks.remove(transNum);
                }
            }
            //加入lock
            else {
                assert(!transactionLocks.containsKey(transNum) ||
                        !transactionLocks.get(transNum).contains(lock));
                transactionLocks.putIfAbsent(transNum, new ArrayList<>());
                transactionLocks.get(transNum).add(lock);
            }
        }

        /**
         * Gives the transaction the lock `lock`. Assumes that the lock is
         * compatible. Updates lock on resource if the transaction already has a
         * lock.
         */
        //updateLock的时候 旧锁不是release 而是从两个hash表删除即可 不需要处理waitingQueue
        public void grantOrUpdateLock(Lock lock) {
            // TODO(proj4_part1): implement
            assert(!locks.contains(lock));
            assert(checkCompatible(lock.lockType, lock.transactionNum));
            // 删除resource上的旧锁
            locks.removeIf(oldLock -> Objects.equals(lock.transactionNum,
                    oldLock.transactionNum));
            // 给resource加上新锁
            locks.add(lock);
            // 维护 事务->锁 的hash表 加入或删除lock
            // 如果是promote的话,这里不会删除旧锁 需要在promote方法中删除
            processTransactionLocks(lock, false);
        }

        /**
         * Releases the lock `lock` and processes the queue. Assumes that the
         * lock has been granted before.
         */
        public void releaseLock(Lock lock) {
            // TODO(proj4_part1): implement
            assert(locks.contains(lock));
            //维护 对象->锁 的hash表
            locks.remove(lock);
            //维护一下 事务->锁 的hash表
            processTransactionLocks(lock, true);
            //维护队列
            processQueue();
        }
        //不处理waitingQueue
        public void naiveReleaseLock(Lock lock) {
            assert(locks.contains(lock));
            //维护 对象->锁 的hash表
            locks.remove(lock);
            //维护一下 事务->锁 的hash表
            processTransactionLocks(lock, true);
        }
        /**
         * Adds `request` to the front of the queue if addFront is true, or to
         * the end otherwise.
         */
        public void addToQueue(LockRequest request, boolean addFront) {
            // TODO(proj4_part1): implement
            if (addFront) {
                waitingQueue.addFirst(request);
            }
            else waitingQueue.addLast(request);
        }

        /**
         * Grant locks to request from front to back of the queue, stopping
         * when the next lock cannot be granted. Once a request is completely
         * granted, the transaction that made the request can be unblocked.
         */
        private void processQueue() {
            Iterator<LockRequest> requests = waitingQueue.iterator();
            while (requests.hasNext()) {
                LockRequest request = requests.next();
                Lock lockToGranted = request.lock;
                if (checkCompatible(lockToGranted.lockType, lockToGranted.transactionNum)) {
                    //释放请求
                    //此处不该产生递归调用processQueue
                    //这里可能有隐式的lock update, 比如之前已经存在锁S，队列中有一个请求锁X
                    //不去调用外部的release,promote,acquire,acquireAndRelease 因为会产生递归调用processQueue
                    for (Lock lockToRelease : request.releasedLocks) {
                        //请求释放的锁有存在于当前resource的locks中
                        //不用递归去处理waitingQueue 单纯地释放锁就好
                        if (this.locks.contains(lockToRelease)) {
                           this.naiveReleaseLock(lockToRelease);
                        }
                        Long transNum = lockToRelease.transactionNum;
                        // 请求被处理后该事务就不被阻塞了, 移出等待图
                        waitForGraph.remove(transNum);
                        if (waitForGraph.isEmpty()) {
                            stopDeadLockChecker();
                        }
                    }
                    // 授予或升级锁
                    grantOrUpdateLock(lockToGranted);
                    waitingQueue.removeFirst();
                    request.transaction.unblock();
                }
                else {
                    break;
                }
            }
        }

        /**
         * Gets the type of lock `transaction` has on this resource.
         */
        public LockType getTransactionLockType(long transaction) {
            // TODO(proj4_part1): implement
            for (Lock lock : locks) {
                if (lock.transactionNum == transaction) {
                    return lock.lockType;
                }
            }
            return LockType.NL;
        }
        @Override
        public String toString() {
            return "Active Locks: " + Arrays.toString(this.locks.toArray()) +
                    ", Queue: " + Arrays.toString(this.waitingQueue.toArray());
        }
    }

    // You should not modify or use this directly.
    private Map<String, LockContext> contexts = new HashMap<>();

    /**
     * Helper method to fetch the resourceEntry corresponding to `name`.
     * Inserts a new (empty) resourceEntry into the map if no entry exists yet.
     */
    private ResourceEntry getResourceEntry(ResourceName name) {
        resourceEntries.putIfAbsent(name, new ResourceEntry());
        return resourceEntries.get(name);
    }

    /**
     * Acquire a `lockType` lock on `name`, for transaction `transaction`, and
     * releases all locks on `releaseNames` held by the transaction after
     * acquiring the lock in one atomic action.
     *
     * Error checking must be done before any locks are acquired or released. If
     * the new lock is not compatible with another transaction's lock on the
     * resource, the transaction is blocked and the request is placed at the
     * FRONT of the resource's queue.
     *
     * Locks on `releaseNames` should be released only after the requested lock
     * has been acquired. The corresponding queues should be processed.
     *
     * An acquire-and-release that releases an old lock on `name` should NOT
     * change the acquisition time of the lock on `name`, i.e. if a transaction
     * acquired locks in the order: S(A), X(B), acquire X(A) and release S(A),
     * the lock on A is considered to have been acquired before the lock on B.
     *
     * @throws DuplicateLockRequestException if a lock on `name` is already held
     * by `transaction` and isn't being released
     * @throws NoLockHeldException if `transaction` doesn't hold a lock on one
     * or more of the names in `releaseNames`
     */
    public void acquireAndRelease(TransactionContext transaction, ResourceName name,
                                  LockType lockType, List<ResourceName> releaseNames)
            throws DuplicateLockRequestException, NoLockHeldException {
        // TODO(proj4_part1): implement
        // You may modify any part of this method. You are not required to keep
        // all your code within the given synchronized block and are allowed to
        // move the synchronized block elsewhere if you wish.
        boolean shouldBlock = false;
        synchronized (this) {
            long transNum = transaction.getTransNum();
            ResourceEntry resourceEntry = getResourceEntry(name);
            LockType typeExist = getLockType(transaction, name);
            //当前事务 在当前name上 要释放的锁
            LockType releasedType = LockType.NL;
            Lock lock = new Lock(name, lockType, transNum);
            //需要被释放的锁 存进List中
            List<Lock> releasedLocks = new ArrayList<>();
            for (ResourceName n : releaseNames) {
                ResourceEntry resource = getResourceEntry(n);
                //巧妙避免了没有锁的情况 NL会在processQueue的时候隐式remove
                LockType type = resource.getTransactionLockType(transNum);
                if (n == name) {
                    releasedType = type;
                }
                releasedLocks.add(new Lock(n, type, transNum));
            }
            //如果已经存在锁 且不是要被释放的
            if (typeExist != LockType.NL
            && typeExist != releasedType) {
                throw new DuplicateLockRequestException("DuplicateLock in acquireAndRelease!");
            }
            //只要不冲突就授予锁，优先度高于队列中的请求
            if (resourceEntry.checkCompatible(lockType, transNum)) {
                //先释放 因为promote的时候要删除旧锁
                for (ResourceName name1 : releaseNames) {
                    release(transaction, name1);
                }
                // 再授予锁
                resourceEntry.grantOrUpdateLock(lock);
            }
            else {
                shouldBlock = true;
                //将其生成为一个请求并放到队列开头
                resourceEntry.addToQueue(new LockRequest(transaction, lock, releasedLocks), true);
                // 只要有进队 就要维护waitForGraph
                waitForGraph.put(transNum,
                        resourceEntry.getIncompatibleLock(lockType, transNum).transactionNum);
                if (!checkIsRunning) startDeadLockChecker();
            }
            if (shouldBlock) {
                transaction.prepareBlock();
            }
        }
        if (shouldBlock) {
            transaction.block();
        }
    }

    /**
     * Acquire a `lockType` lock on `name`, for transaction `transaction`.
     *
     * Error checking must be done before the lock is acquired. If the new lock
     * is not compatible with another transaction's lock on the resource, or if there are
     * other transaction in queue for the resource, the transaction is
     * blocked and the request is placed at the **back** of NAME's queue.
     *
     * @throws DuplicateLockRequestException if a lock on `name` is held by
     * `transaction`
     */
    public void acquire(TransactionContext transaction, ResourceName name,
                        LockType lockType) throws DuplicateLockRequestException {
        // TODO(proj4_part1): implement
        // You may modify any part of this method. You are not required to keep all your
        // code within the given synchronized block and are allowed to move the
        // synchronized block elsewhere if you wish.
        boolean shouldBlock = false;
        synchronized (this) {
            long transNum = transaction.getTransNum();
            ResourceEntry resourceEntry = getResourceEntry(name);
            LockType typeExist = getLockType(transaction, name);
            Lock lock = new Lock(name, lockType, transNum);
            //如果已经存在锁 抛出异常, 不允许隐式升级锁
            if (typeExist != LockType.NL) {
                throw new DuplicateLockRequestException("DuplicateLockRequestException!");
            }
            if (!resourceEntry.waitingQueue.isEmpty()) {
                shouldBlock = true;
                resourceEntry.addToQueue(new LockRequest(transaction, lock), false);
            }
            else if (resourceEntry.checkCompatible(lockType, transNum)) {
                resourceEntry.grantOrUpdateLock(lock);
            }
            else {
                shouldBlock = true;
                resourceEntry.addToQueue(new LockRequest(transaction, lock), false);
                // 只要有进队 就维护waitForGraph
                waitForGraph.put(transNum,
                        resourceEntry.getIncompatibleLock(lockType, transNum).transactionNum);
                if (!checkIsRunning) startDeadLockChecker();
            }
            if (shouldBlock) {
                transaction.prepareBlock();
            }
        }
        if (shouldBlock) {
            transaction.block();
        }
    }

    /**
     * Release `transaction`'s lock on `name`. Error checking must be done
     * before the lock is released.
     *
     * The resource name's queue should be processed after this call. If any
     * requests in the queue have locks to be released, those should be
     * released, and the corresponding queues also processed.
     *
     * @throws NoLockHeldException if no lock on `name` is held by `transaction`
     */
    public void release(TransactionContext transaction, ResourceName name)
            throws NoLockHeldException {
        // TODO(proj4_part1): implement
        // You may modify any part of this method.
        long transNum = transaction.getTransNum();
        synchronized (this) {
            ResourceEntry resourceEntry = getResourceEntry(name);
            LockType lockType = getLockType(transaction, name);
            if (lockType == LockType.NL) {
                throw new NoLockHeldException("NoLockHeld when release!");
            }
            Lock lock = new Lock(name, lockType, transNum);
            resourceEntry.releaseLock(lock);
        }
    }

    /**
     * Promote a transaction's lock on `name` to `newLockType` (i.e. change
     * the transaction's lock on `name` from the current lock type to
     * `newLockType`, if it's a valid substitution).
     *
     * Error checking must be done before any locks are changed. If the new lock
     * is not compatible with another transaction's lock on the resource, the
     * transaction is blocked and the request is placed at the FRONT of the
     * resource's queue.
     *
     * A lock promotion should NOT change the acquisition time of the lock, i.e.
     * if a transaction acquired locks in the order: S(A), X(B), promote X(A),
     * the lock on A is considered to have been acquired before the lock on B.
     *
     * @throws DuplicateLockRequestException if `transaction` already has a
     * `newLockType` lock on `name`
     * @throws NoLockHeldException if `transaction` has no lock on `name`
     * @throws InvalidLockException if the requested lock type is not a
     * promotion. A promotion from lock type A to lock type B is valid if and
     * only if B is substitutable for A, and B is not equal to A.
     */
    public void promote(TransactionContext transaction, ResourceName name,
                        LockType newLockType)
            throws DuplicateLockRequestException, NoLockHeldException, InvalidLockException {
        // TODO(proj4_part1): implement
        // You may modify any part of this method.
        assert(newLockType != LockType.SIX);
        boolean shouldBlock = false;
        synchronized (this) {
            long transNum = transaction.getTransNum();
            ResourceEntry resourceEntry = getResourceEntry(name);
            LockType typeExist = getLockType(transaction, name);
            Lock lock = new Lock(name, newLockType, transNum);
            //如果不存在锁
            if (typeExist == LockType.NL) {
                throw new NoLockHeldException("NoLockHeld when promote!");
            }
            //已经存在newLockType
            if (typeExist == newLockType) {
                throw new DuplicateLockRequestException("DuplicateLock when promote!");
            }
            //已经存在的锁不能升级成newLock (newLock 无法代替
            if (!LockType.substitutable(newLockType, typeExist)) {
                throw new InvalidLockException("fail to substitute when promote!");
            }
            //可以请求锁
            if (resourceEntry.checkCompatible(newLockType, transNum)) {
                List<ResourceName> release = new ArrayList<>();
                release.add(name);
                //旧锁的删除 会在AndRelease中进行
                // promote应该视为一个原子性的过程
                this.acquireAndRelease(transaction, name, newLockType,
                        release);
            }
            //有冲突
            else {
                shouldBlock = true;
                resourceEntry.addToQueue(new LockRequest(transaction, lock), true);
                // 更新waits-for信息
                waitForGraph.put(transNum,
                        resourceEntry.getIncompatibleLock(newLockType, transNum).transactionNum);
                if (!checkIsRunning) startDeadLockChecker();
            }
            if (shouldBlock) {
                transaction.prepareBlock();
            }
        }
        if (shouldBlock) {
            transaction.block();
        }
    }

    /**
     * Return the type of lock `transaction` has on `name` or NL if no lock is
     * held.
     */
    public synchronized LockType getLockType(TransactionContext transaction, ResourceName name) {
        // TODO(proj4_part1): implement
        ResourceEntry resourceEntry = getResourceEntry(name);
        return resourceEntry.getTransactionLockType(transaction.getTransNum());
    }

    /**
     * Returns the list of locks held on `name`, in order of acquisition.
     */
    public synchronized List<Lock> getLocks(ResourceName name) {
        return new ArrayList<>(resourceEntries.getOrDefault(name, new ResourceEntry()).locks);
    }

    /**
     * Returns the list of locks held by `transaction`, in order of acquisition.
     */
    //创建一个新的List 不能用来修改transactionLocks, 小心wa点
    public synchronized List<Lock> getLocks(TransactionContext transaction) {
        return new ArrayList<>(transactionLocks.getOrDefault(transaction.getTransNum(),
                Collections.emptyList()));
    }

    /**
     * Creates a lock context. See comments at the top of this file and the top
     * of LockContext.java for more information.
     */
    public synchronized LockContext context(String name) {
        if (!contexts.containsKey(name)) {
            contexts.put(name, new LockContext(this, null, name));
        }
        return contexts.get(name);
    }

    /**
     * Create a lock context for the database. See comments at the top of this
     * file and the top of LockContext.java for more information.
     */
    public synchronized LockContext databaseContext() {
        return context("database");
    }

    /** register()发生在DataBase.java 事务开始时, lock manager需要追踪活跃的事务
     */
    public synchronized void register(Transaction transaction) {
        runningTransaction.put(transaction.getTransNum(), transaction);
    }

    public synchronized void deregister(Transaction transaction) {
        runningTransaction.remove(transaction.getTransNum());
    }

    /** 加入死锁检测, wait-for graph + 拓扑排序判环
     */
    public LockManager() {
        // 后台线程检测死锁
        deadLockChecker = new Thread(new Runnable() {
            @Override
            public void run() {
                while (!Thread.currentThread().isInterrupted()) {
                    // 每间隔1s检测
                    try {
                        Thread.sleep(10000);
                    } catch (InterruptedException e) {
                        // 休眠时被打断则再次抛出异常
                        Thread.currentThread().interrupt();
                    }
                    deadLockCheck();
                }
            }
        });
    }
    public void startDeadLockChecker() {
        checkIsRunning = true;
        deadLockChecker.start();
    }

    public void stopDeadLockChecker() {
        checkIsRunning = false;
        deadLockChecker.interrupt();
    }
    private void deadLockCheck () {
        // 检查waits for关系中是否存在环
        // 统计节点的入度
        Map <Long, Long> ins = new HashMap<>();
        synchronized (this) {
            for (Long tid : waitForGraph.keySet()) {
                ins.putIfAbsent(tid, 0L);
            }
            for (Long tid : waitForGraph.values()) {
                ins.putIfAbsent(tid, 0L);
                ins.put(tid, ins.get(tid) + 1);
            }
        }

        // 拓扑排序
        while (ins.size() > 0) {
            // 找到一个没有入度的点了, 说明此时不存在环
            boolean found = false;
            for (Long tid : ins.keySet()) {
                if (ins.get(tid) == 0) {
                    found = true;
                    ins.remove(tid);
                    Long waited = waitForGraph.get(tid);
                    // 删掉那个点后, 更新出点 出点的入度-1
                    if(ins.containsKey(waited)) ins.put(waited, ins.get(waited) - 1);
                }
            }
            if (!found) break; // 找到环
        }
        // 对于找到死锁的情况
        if (ins.size() != 0) {
            System.out.println("found dead lock: " + ins.keySet());
            // 回滚最年轻的事务
            Long tid = Collections.max(ins.keySet());
            runningTransaction.get(tid).rollback();
            runningTransaction.remove(tid);
        }
    }
}
