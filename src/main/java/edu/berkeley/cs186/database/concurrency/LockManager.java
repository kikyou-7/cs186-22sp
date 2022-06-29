package edu.berkeley.cs186.database.concurrency;

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
            assert(!locks.contains(lock) && checkCompatible(lock.lockType, lock.transactionNum));

            // 维护 对象->锁 的hash表
            locks.removeIf(oldLock -> Objects.equals(lock.transactionNum,
                    oldLock.transactionNum));
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
                    // 释放请求
                    for (Lock lockToRelease : request.releasedLocks) {
                        //请求释放的锁有存在于当前resource的locks中
                        //递归去处理队列 单纯地释放锁就好
                        if (this.locks.contains(lockToRelease)) {
                           release(request.transaction, request.lock.name);
                        }
                    }
                    // 授予锁
                    grantOrUpdateLock(lockToGranted);
                    /* 不能用acquireAndRelease 可能会产生duplicate exception
                    已经有一个S 再次请求一个X 此时属于upgrade的情况, 在acquireAndRelease会抛异常
                    List<ResourceName> releaseNames = new ArrayList<>();
                    for (Lock lockToRelease : request.releasedLocks) {
                        releaseNames.add(lockToRelease.name);
                    }
                    acquireAndRelease(request.transaction,
                            lockToGranted.name, lockToGranted.lockType,
                            releaseNames);*/

                    // 出队 更新队列
                    waitingQueue.removeFirst();
                    // 入队表示该事务的请求被挂起 事务被锁住 所以出队的时候要将事务解锁
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
                //先授予锁 释放的时候要避开当前的source
                resourceEntry.grantOrUpdateLock(lock);
                //再释放
                for (ResourceName name1 : releaseNames) {
                    //如果不存在对应的锁 会抛出异常
                    if (name1 == name) continue;
                    release(transaction, name1);
                }
            }
            else {
                shouldBlock = true;
                //将其生成为一个请求并放到队列开头
                resourceEntry.addToQueue(new LockRequest(transaction, lock, releasedLocks), true);
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
            List<Lock> locks = getLocks(transaction);
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
                resourceEntry.grantOrUpdateLock(lock);
                //隐式地把旧锁删掉, resource中的信息也已经在上一行调用中隐式维护了
                locks.remove(new Lock(name, typeExist, transNum));
                transactionLocks.get(transNum).remove(new Lock(name, typeExist, transNum));

            } else {
                shouldBlock = true;
                resourceEntry.addToQueue(new LockRequest(transaction, lock), true);
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
}
