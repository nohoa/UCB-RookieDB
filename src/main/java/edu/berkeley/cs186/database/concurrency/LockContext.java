package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * LockContext wraps around LockManager to provide the hierarchical structure
 * of multigranularity locking. Calls to acquire/release/etc. locks should
 * be mostly done through a LockContext, which provides access to locking
 * methods at a certain point in the hierarchy (database, table X, etc.)
 */
public class LockContext {
    // You should not remove any of these fields. You may add additional
    // fields/methods as you see fit.

    // The underlying lock manager.
    protected final LockManager lockman;

    // The parent LockContext object, or null if this LockContext is at the top of the hierarchy.
    protected final LockContext parent;

    // The name of the resource this LockContext represents.
    protected ResourceName name;

    // Whether this LockContext is readonly. If a LockContext is readonly, acquire/release/promote/escalate should
    // throw an UnsupportedOperationException.
    protected boolean readonly;

    // A mapping between transaction numbers, and the number of locks on children of this LockContext
    // that the transaction holds.
    protected final Map<Long, Integer> numChildLocks;

    // You should not modify or use this directly.
    protected final Map<String, LockContext> children;

    // Whether or not any new child LockContexts should be marked readonly.
    protected boolean childLocksDisabled;

    public LockContext(LockManager lockman, LockContext parent, String name) {
        this(lockman, parent, name, false);
    }

    protected LockContext(LockManager lockman, LockContext parent, String name,
                          boolean readonly) {
        this.lockman = lockman;
        this.parent = parent;
        if (parent == null) {
            this.name = new ResourceName(name);
        } else {
            this.name = new ResourceName(parent.getResourceName(), name);
        }
        this.readonly = readonly;
        this.numChildLocks = new ConcurrentHashMap<>();
        this.children = new ConcurrentHashMap<>();
        this.childLocksDisabled = readonly;
    }

    /**
     * Gets a lock context corresponding to `name` from a lock manager.
     */
    public static LockContext fromResourceName(LockManager lockman, ResourceName name) {
        Iterator<String> names = name.getNames().iterator();
        LockContext ctx;
        String n1 = names.next();
        ctx = lockman.context(n1);
        while (names.hasNext()) {
            String n = names.next();
            ctx = ctx.childContext(n);
        }
        return ctx;
    }

    /**
     * Get the name of the resource that this lock context pertains to.
     */
    public ResourceName getResourceName() {
        return name;
    }

    /**
     * Update num children
     *
     */
    void updateNumChild(TransactionContext transaction, LockType lockType, LockContext parent){
        if(parent == null) return ;
        Long transNo = transaction.getTransNum();
        if(!parent.numChildLocks.containsKey(transNo)){
            parent.numChildLocks.put(transNo,1);
        }
        else {
            parent.numChildLocks.put(transNo,parent.numChildLocks.get(transNo)+1);
        }
        updateNumChild(transaction,lockType,parent.parent);
    }

    /**
     * Update num children for release
     *
     */
    void relseLock(TransactionContext transaction,LockContext parent){
        if(parent == null) return ;
        Long transNo = transaction.getTransNum();
            if(parent.numChildLocks.containsKey(transNo))  {
                //System.out.println(parent.numChildLocks.get(transNo));
                relseLock(transaction,parent.parent);
                parent.numChildLocks.put(transNo,parent.numChildLocks.get(transNo)-1);
                if(parent.numChildLocks.get(transNo) == 0){
                    parent.numChildLocks.remove(transNo);
                }
            }
    }


    void relseEscalateLock(TransactionContext transaction,LockContext parent,int value ){
        if(parent == null) return ;
        Long transNo = transaction.getTransNum();
        parent.numChildLocks.put(transNo,parent.numChildLocks.get(transNo)-value);
        if(parent.numChildLocks.get(transNo) == 0){
            parent.numChildLocks.remove(transNo);
        }
        relseLock(transaction,parent.parent);
    }


    /**
     * Acquire a `lockType` lock, for transaction `transaction`.
     *
     * Note: you must make any necessary updates to numChildLocks, or else calls
     * to LockContext#getNumChildren will not work properly.
     *
     * @throws InvalidLockException if the request is invalid
     * @throws DuplicateLockRequestException if a lock is already held by the
     * transaction.
     * @throws UnsupportedOperationException if context is readonly
     */
    public void acquire(TransactionContext transaction, LockType lockType)
            throws InvalidLockException, DuplicateLockRequestException {
        // TODO(proj4_part2): implement
        if (readonly) {
            throw new UnsupportedOperationException("readonly");
        }
        if (lockType == LockType.NL) {
            throw new InvalidLockException("Exception Lock");
        }
        if (lockman.getLockType(transaction, name).equals(lockType))
            throw new DuplicateLockRequestException("A lock is already held by the transaction");

        if (parent != null) {
            List<Lock> lockList = lockman.getLocks(transaction);
            boolean compatible = true;

            LockType par = parent.getExplicitLockType(transaction);
            if (!LockType.canBeParentLock(par, lockType)) {
                throw new InvalidLockException("Exception Lock");
            }
        }
        lockman.acquire(transaction, name, lockType);
        updateNumChild(transaction,lockType,parent);
    }

    /**
     * Release `transaction`'s lock on `name`.
     *
     * Note: you *must* make any necessary updates to numChildLocks, or
     * else calls to LockContext#getNumChildren will not work properly.
     *
     * @throws NoLockHeldException if no lock on `name` is held by `transaction`
     * @throws InvalidLockException if the lock cannot be released because
     * doing so would violate multigranularity locking constraints
     * @throws UnsupportedOperationException if context is readonly
     */
    public void release(TransactionContext transaction)
            throws NoLockHeldException, InvalidLockException {
        // TODO(proj4_part2): implement
        if(readonly && !(lockman.getLockType(transaction,name).equals(LockType.S)||lockman.getLockType(transaction,name).equals(LockType.IS))) throw new UnsupportedOperationException("Context is read only");
        boolean exist = false;
        for(Lock l : lockman.getLocks(transaction)){

            if(l.name.equals(name)){
                exist = true;
                break;
            }
        }
        if(!exist){
            throw new NoLockHeldException("No lock");
        }
        boolean can_release = true ;
        for(Lock l : lockman.getLocks(transaction)){
            LockContext par = fromResourceName(lockman,l.name).parent;
            while(par != null){
                if(par.getResourceName() == name){
                    if(l.lockType == LockType.X || l.lockType == LockType.S) {
                        can_release = false;
                        break;
                    }
                }
                par = fromResourceName(par.lockman,par.name).parent;
            }
        }
        if(!can_release) {
            throw new InvalidLockException("mutigranity release problem");
        }
        lockman.release(transaction,name);
        relseLock(transaction,parent);
        return;
    }

    /**
     * Promote `transaction`'s lock to `newLockType`. For promotion to SIX from
     * IS/IX, all S and IS locks on descendants must be simultaneously
     * released. The helper function sisDescendants may be helpful here.
     *
     * Note: you *must* make any necessary updates to numChildLocks, or else
     * calls to LockContext#getNumChildren will not work properly.
     *
     * @throws DuplicateLockRequestException if `transaction` already has a
     * `newLockType` lock
     * @throws NoLockHeldException if `transaction` has no lock
     * @throws InvalidLockException if the requested lock type is not a
     * promotion or promoting would cause the lock manager to enter an invalid
     * state (e.g. IS(parent), X(child)). A promotion from lock type A to lock
     * type B is valid if B is substitutable for A and B is not equal to A, or
     * if B is SIX and A is IS/IX/S, and invalid otherwise. hasSIXAncestor may
     * be helpful here.
     * @throws UnsupportedOperationException if context is readonly
     */
    public void promote(TransactionContext transaction, LockType newLockType)
            throws DuplicateLockRequestException, NoLockHeldException, InvalidLockException {
        // TODO(proj4_part2): implement

        if(readonly){
            throw new UnsupportedOperationException("Readonly exception");
        }
        List<Lock> all_Lock = lockman.getLocks(transaction);
        for(Lock l : all_Lock){
            if(l.lockType.equals(newLockType) && l.name.equals(name)){
                throw new DuplicateLockRequestException("Lock already held");
            }
        }
        boolean exist = true ;
        if(all_Lock.isEmpty()) {
            throw new NoLockHeldException("no lock");
        }
        LockType oldLock = getExplicitLockType(transaction);
        if(oldLock == newLockType){
            throw new InvalidLockException("Can't promote");
        }
        if(!LockType.substitutable(newLockType,oldLock)){
            throw new InvalidLockException("Can't promote");
        }
        if(newLockType == LockType.SIX){
            if(hasSIXAncestor(transaction)){
                throw new InvalidLockException("Can't promote");
            }
            else if(oldLock != LockType.S && oldLock!= LockType.IS && oldLock != LockType.IX){
                throw new InvalidLockException("Can't promote");
            }
        }
        if(newLockType == LockType.SIX){
            List<ResourceName> descent = sisDescendants(transaction);
            descent.add(name);

            lockman.acquireAndRelease(transaction,name,newLockType,descent);
            for(ResourceName name : descent){
                relseLock(transaction,fromResourceName(lockman,name).parent);
            }
        }
         else lockman.promote(transaction,name,newLockType);

    }

    /**
     * Escalate `transaction`'s lock from descendants of this context to this
     * level, using either an S or X lock. There should be no descendant locks
     * after this call, and every operation valid on descendants of this context
     * before this call must still be valid. You should only make *one* mutating
     * call to the lock manager, and should only request information about
     * TRANSACTION from the lock manager.
     *
     * For example, if a transaction has the following locks:
     *
     *                    IX(database)
     *                    /         \
     *               IX(table1)    S(table2)
     *                /      \
     *    S(table1 page3)  X(table1 page5)
     *
     * then after table1Context.escalate(transaction) is called, we should have:
     *
     *                    IX(database)
     *                    /         \
     *               X(table1)     S(table2)
     *
     * You should not make any mutating calls if the locks held by the
     * transaction do not change (such as when you call escalate multiple times
     * in a row).
     *
     * Note: you *must* make any necessary updates to numChildLocks of all
     * relevant contexts, or else calls to LockContext#getNumChildren will not
     * work properly.
     *
     * @throws NoLockHeldException if `transaction` has no lock at this level
     * @throws UnsupportedOperationException if context is readonly
     */
    public void escalate(TransactionContext transaction) throws NoLockHeldException {
        // TODO(proj4_part2): implement
        List<Lock> lock_list = lockman.getLocks(transaction);
        if(lock_list.isEmpty()){
            throw new NoLockHeldException("No lock");
        }
        if(readonly) {
            throw new UnsupportedOperationException("read only");
        }
        List<ResourceName> release = new ArrayList<>();
        List<LockType> tp = new ArrayList<>();
        for(Lock l : lock_list){
           LockContext context = fromResourceName(lockman,l.name);
           if(isAncestor(context,this)){
               release.add(l.name);
               tp.add(l.lockType);
           }
        }
        LockType fin = LockType.S;
        tp.add(lockman.getLockType(transaction,name));
        boolean all_fixed = false ;
        if(lockman.getLockType(transaction,name) == LockType.S || lockman.getLockType(transaction,name) == LockType.X){
                all_fixed = true ;
        }
        for(LockType l : tp){
            if(l == LockType.X || l == LockType.IX){
                fin = LockType.X;
                break;
            }
        }
        release.add(name);

        if(!all_fixed)lockman.acquireAndRelease(transaction,name,fin,release);
        if(numChildLocks.containsKey(transaction.getTransNum())) relseEscalateLock(transaction,fromResourceName(lockman,name).parent,numChildLocks.get(transaction.getTransNum()));
        numChildLocks.clear();
        return;
    }

    /**
     * Get the type of lock that `transaction` holds at this level, or NL if no
     * lock is held at this level.
     */
    public LockType getExplicitLockType(TransactionContext transaction) {
        if (transaction == null) return LockType.NL;
        // TODO(proj4_part2): implement
        return lockman.getLockType(transaction,name);
    }

    /**
     * Gets the type of lock that the transaction has at this level, either
     * implicitly (e.g. explicit S lock at higher level implies S lock at this
     * level) or explicitly. Returns NL if there is no explicit nor implicit
     * lock.
     */
    public LockType getEffectiveLockType(TransactionContext transaction) {
        if (transaction == null) return LockType.NL;
        // TODO(proj4_part2): implement
        LockType parentLockType;
        parentLockType = getExplicitLockType(transaction);
        if(parentLockType.equals(LockType.S)||parentLockType.equals(LockType.X)) return parentLockType;
        LockContext p = parent;
//        parentLockType = p.getExplicitLockType(transaction);
        while(p!=null)
        {
            parentLockType = p.getExplicitLockType(transaction);
            if(hasSIXAncestor(transaction)){
                if(p.getExplicitLockType(transaction).equals(LockType.IX)){
                    parentLockType = LockType.SIX;
                }
                else{
                    parentLockType = LockType.S;
                }
                break;
            }
            if(parentLockType.equals(LockType.S)||parentLockType.equals(LockType.X)) break;
            p = p.parent;

        }
        if(parentLockType.equals(LockType.SIX)
                || parentLockType.equals(LockType.X)
                || parentLockType.equals(LockType.S)) return parentLockType;
        else{
            return LockType.NL;
        }
    }

    /**
     * Helper method to see if the transaction holds a SIX lock at an ancestor
     * of this context
     * @param transaction the transaction
     * @return true if holds a SIX at an ancestor, false if not
     */
    private boolean hasSIXAncestor(TransactionContext transaction) {
        // TODO(proj4_part2): implement
        LockContext par = parent;
        while(par != null){
            if(par.getExplicitLockType(transaction) == LockType.SIX){
                return true;
            }
            par = par.parent;
        }
        return false;
    }

    /**
     * Helper method to get a list of resourceNames of all locks that are S or
     * IS and are descendants of current context for the given transaction.
     * @param transaction the given transaction
     * @return a list of ResourceNames of descendants which the transaction
     * holds an S or IS lock.
     */
    private List<ResourceName> sisDescendants(TransactionContext transaction) {
        // TODO(proj4_part2): implement
        List<Lock > resouce = lockman.getLocks(transaction);
        ArrayList<ResourceName> descendants = new ArrayList<>();
        for (Lock l : resouce){
            LockContext current = fromResourceName(lockman,l.name);
            if(isAncestor(current,this)){
                if(l.lockType == LockType.S || l.lockType == LockType.IS){
                    descendants.add(l.name);
                }
            }
        }
        return descendants;
    }

    /**
     * Disables locking descendants. This causes all new child contexts of this
     * context to be readonly. This is used for indices and temporary tables
     * (where we disallow finer-grain locks), the former due to complexity
     * locking B+ trees, and the latter due to the fact that temporary tables
     * are only accessible to one transaction, so finer-grain locks make no
     * sense.
     */
    public void disableChildLocks() {
        this.childLocksDisabled = true;
    }

    /**
     * Gets the parent context.
     */
    public LockContext parentContext() {
        return parent;
    }

    /**
     * Gets the context for the child with name `name` and readable name
     * `readable`
     */
    public synchronized LockContext childContext(String name) {
        LockContext temp = new LockContext(lockman, this, name,
                this.childLocksDisabled || this.readonly);
        LockContext child = this.children.putIfAbsent(name, temp);
        if (child == null) child = temp;
        return child;
    }

    /**
     * Gets the context for the child with name `name`.
     */
    public synchronized LockContext childContext(long name) {
        return childContext(Long.toString(name));
    }

    /**
     * Gets the number of locks held on children a single transaction.
     */
    public int getNumChildren(TransactionContext transaction) {
        return numChildLocks.getOrDefault(transaction.getTransNum(), 0);
    }

    @Override
    public String toString() {
        return "LockContext(" + name.toString() + ")";
    }

    private boolean isAncestor(LockContext current, LockContext parent){
        current = current.parent;
       while(current != null){
           if(current.equals(parent)){
               return true;
           }
           current = current.parent;
       }
       return false;
    }

}

