package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.TransactionContext;

/**
 * LockUtil is a declarative layer which simplifies multigranularity lock
 * acquisition for the user (you, in the last task of Part 2). Generally
 * speaking, you should use LockUtil for lock acquisition instead of calling
 * LockContext methods directly.
 */

public class LockUtil {
    /**
     * Ensure that the current transaction can perform actions requiring
     * `requestType` on `lockContext`.
     *
     * `requestType` is guaranteed to be one of: S, X, NL.
     *
     * This method should promote/escalate/acquire as needed, but should only
     * grant the least permissive set of locks needed. We recommend that you
     * think about what to do in each of the following cases:
     * - The current lock type can effectively substitute the requested type
     * - The current lock type is IX and the requested lock is S
     * - The current lock type is an intent lock
     * - None of the above: In this case, consider what values the explicit
     *   lock type can be, and think about how ancestor looks will need to be
     *   acquired or changed.
     *
     * You may find it useful to create a helper method that ensures you have
     * the appropriate locks on all ancestors.
     */
    public static void ensureSufficientLockHeld(LockContext lockContext, LockType requestType) {
        // requestType must be S, X, or NL
        assert (requestType == LockType.S || requestType == LockType.X || requestType == LockType.NL);

        // Do nothing if the transaction or lockContext is null
        TransactionContext transaction = TransactionContext.getTransaction();
        if (transaction == null || lockContext == null) return;

        // You may find these variables useful
        LockContext parentContext = lockContext.parentContext();
        LockType effectiveLockType = lockContext.getEffectiveLockType(transaction);
        LockType explicitLockType = lockContext.getExplicitLockType(transaction);

        // TODO(proj4_part2): implement
        // 不需要升级/获取新锁 check隐式锁
        if (effectiveLockType.equals(requestType)
        || LockType.substitutable(effectiveLockType, requestType)) {
            return ;
        }
        // 检查当前隔离等级, 若为read uncommitted则不加S锁
//        if (Database.DBisolationLevel.equals(IsolationLevel.READ_UNCOMMITTED) && requestType.equals(LockType.S)) {
//            return;
//        }

        // 祖先节点中没有足够权限的锁 需要在当前节点挂上requestType锁
        // 当前锁是IX并且申请S, 此时需要变成SIX锁，单独讨论
        if (explicitLockType.equals(LockType.IX) &&
        requestType.equals(LockType.S)) {
            getIntendLock(transaction, lockContext.parentContext(), LockType.IX);
            lockContext.promote(transaction, LockType.SIX);
        }
        // 当前的锁是意向锁 需要escalate?
        // IS->S/X , IX->X, SIX->X
        else if (explicitLockType.isIntent()){
            getIntendLock(transaction, lockContext.parentContext(),
                    LockType.parentLock(requestType));
            //IS -> X,先promote 然后escalate
            if (explicitLockType.equals(LockType.IS) && requestType.equals(LockType.X)) {
                lockContext.promote(transaction, requestType);
                lockContext.escalate(transaction);
            }
            //
            else {
                lockContext.escalate(transaction);
            }
        }
        // 当前锁的S/NL
        else {
            //S -> X
            if (explicitLockType.equals(LockType.S)) {
                getIntendLock(transaction, lockContext.parentContext(),
                        LockType.parentLock(requestType));
                lockContext.promote(transaction, requestType);
            }
            //NL->S/X
            else {
                getIntendLock(transaction, lockContext.parentContext(),
                        LockType.parentLock(requestType));
                lockContext.acquire(transaction, requestType);
            }
        }
    }
    // TODO(proj4_part2) add any helper methods you want
    //lockContext 获取requestType (IS/IX)
    private static void getIntendLock(TransactionContext transaction,
                                      LockContext lockContext, LockType requestType) {
        if (lockContext == null) return;
        assert (requestType.isIntent());
        LockType explicitLockType = lockContext.getExplicitLockType(transaction);
        // 原先的锁不可能是X SIX IX, 因为X一定可以赋予隐式锁, SIX IX可以作为意向锁
        // 所以explicitLockType只能是S IS NL中的一个
        if (explicitLockType.equals(LockType.X) || explicitLockType.equals(LockType.IX)
                || explicitLockType.equals(LockType.SIX)) {
            return ;
        }
        //IS获取IS 相等 不需要申请
        if (explicitLockType.equals(requestType)) {
            return ;
        }
        LockContext parent = lockContext.parentContext();
        // NL
        if (explicitLockType.equals(LockType.NL)) {
            getIntendLock(transaction, parent, requestType);
            lockContext.acquire(transaction, requestType);
        }
        // IS获取IX
        else if (explicitLockType.equals(LockType.IS)) {
            getIntendLock(transaction, parent, requestType);
            lockContext.promote(transaction, requestType);
        }
        //S 获取 IS/IX
        else {
            // S会授予隐式锁给后代, 不会产生IS申请
            assert(requestType.equals(LockType.IX));
            getIntendLock(transaction, parent, requestType);
            lockContext.promote(transaction, LockType.SIX);
        }
    }
}
