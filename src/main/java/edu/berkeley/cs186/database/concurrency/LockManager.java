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
    private Map<Long, List<Lock>> transactionLocks = new HashMap<>();

    // resourceEntries is a mapping from resource names to a ResourceEntry
    // object, which contains a list of Locks on the object, as well as a
    // queue for requests on that resource.
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

            for(Lock currLock : locks){
                if(currLock.transactionNum == except) continue;
                if(!LockType.compatible(lockType, currLock.lockType)) return false ;
            }
            Iterator<LockRequest> dq = waitingQueue.iterator();
            while(dq.hasNext()){
                LockRequest currentRequest = dq.next();
                if(!LockType.compatible(lockType,currentRequest.lock.lockType)) {
                    if(currentRequest.transaction.getTransNum() != except) return false;
                }
            }
            return true ;
        }

        /**
         * Gives the transaction the lock `lock`. Assumes that the lock is
         * compatible. Updates lock on resource if the transaction already has a
         * lock.
         */
        public void grantOrUpdateLock(Lock lock) {
            // TODO(proj4_part1): implement
            for(Lock l : locks){
                if(l.transactionNum .equals(lock.transactionNum)){
                    l.lockType = lock.lockType;
                    return ;
                }
            }
            locks.add(lock);
            Long transNo = lock.transactionNum;
            if(transactionLocks.containsKey(transNo)){
                transactionLocks.get(transNo).add(lock);
            }
            else {
                List<Lock> lockList = new ArrayList<>();
                lockList.add(lock);
                transactionLocks.put(transNo, lockList);
            }

            return;
        }

        /**
         * Releases the lock `lock` and processes the queue. Assumes that the
         * lock has been granted before.
         */
        public void releaseLock(Lock lock) {
            // TODO(proj4_part1): implement
            int index = 0;
            for(int i = 0 ;i < locks.size() ;i ++){
                if(lock.equals(locks.get(i))){
                    index = i ;
                    break;
                }
            }
            locks.remove(index);
            List<Lock> lockList = transactionLocks.get(lock.transactionNum);
            lockList.remove(lock);
            if(lockList.isEmpty()) transactionLocks.remove(lock.transactionNum);
            processQueue();

            return ;
        }

        /**
         * Adds `request` to the front of the queue if addFront is true, or to
         * the end otherwise.
         */
        public void addToQueue(LockRequest request, boolean addFront) {
            // TODO(proj4_part1): implement
            if(addFront) waitingQueue.addFirst(request);
            else waitingQueue.addLast(request);
        }

        /**
         * Grant locks to requests from front to back of the queue, stopping
         * when the next lock cannot be granted. Once a request is completely
         * granted, the transaction that made the request can be unblocked.
         */
        private void processQueue() {
            // TODO(proj4_part1): implement
            Iterator<LockRequest> requests = waitingQueue.iterator();
            while(requests.hasNext()) {
                if (locks.isEmpty()) {
                    LockRequest curr = requests.next();
                    grantOrUpdateLock(curr.lock);
                    curr.transaction.unblock();
                    waitingQueue.pop();
                } else {
                    LockRequest curr = requests.next();
                    boolean possible = checkCompatible(curr.lock.lockType, curr.transaction.getTransNum());
                    if (possible) {
                        waitingQueue.pop();
                        curr.transaction.unblock();
                        grantOrUpdateLock(curr.lock);
                    } else {
                        break;
                    }
                }
            }
            return;
        }

        /**
         * Gets the type of lock `transaction` has on this resource.
         */
        public LockType getTransactionLockType(long transaction) {
            // TODO(proj4_part1): implement
            for(Lock currLock : locks){
                if(currLock.transactionNum == transaction){
                    return currLock.lockType;
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
            // Acquire lock

            if(getLockType(transaction,name) != LockType.NL){
                 boolean release = false ;
                 for(ResourceName resName : releaseNames){
                     if(resName.equals(name)){
                         release = true ;
                         break;
                     }
                 }
                 if(!release) throw new DuplicateLockRequestException("Duplicate Lock on Lockmanager");
             }
            for(ResourceName rs : releaseNames){
                    boolean exist = false ;
                        for(Lock l : getLocks(transaction)){

                        if(l.name.equals(rs)){
                            exist = true;
                        }
                    }
                    if(!exist) throw new NoLockHeldException("no lock");
             }
            ResourceEntry entry = getResourceEntry(name);
            boolean compatible = entry.checkCompatible(lockType,transaction.getTransNum());
            Lock l = new Lock(name,lockType,transaction.getTransNum());
            if(!compatible){
                shouldBlock = true ;
                transaction.prepareBlock();
                entry.addToQueue(new LockRequest(transaction,l),true);
            }
            else {
                entry.grantOrUpdateLock(l);
                for(ResourceName rs : releaseNames){
                    if(!rs.equals(name)){
                       LockType curr = getLockType(transaction,rs);
                       Lock l1 = new Lock(rs,curr,transaction.getTransNum());
                        getResourceEntry(rs).releaseLock(l1);
                    }
                }
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
            if(getLockType(transaction,name) != LockType.NL){
                throw new DuplicateLockRequestException("Duplicate lock");
            }
            ResourceEntry resourceEntry = getResourceEntry(name);
            boolean compatible = resourceEntry.checkCompatible(lockType,transaction.getTransNum());
            Lock lock = new Lock(name,lockType,transaction.getTransNum());
            if(!compatible){
                shouldBlock = true ;
                transaction.prepareBlock();
                resourceEntry.addToQueue(new LockRequest(transaction,lock), false);
            }
            else {
                resourceEntry.grantOrUpdateLock(lock);
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
        synchronized (this) {
            ResourceEntry entry = getResourceEntry(name);
            if(getLockType(transaction,name) == LockType.NL) {
                throw new NoLockHeldException("No lock execption");
            }
            List<Lock> all_Lock = getLocks(transaction);
            Lock lock = all_Lock.get(0);
            for(Lock l : all_Lock){
                if(l.name.equals(name)){
                    lock = l ;
                    break;
                }
            }
            entry.releaseLock(lock) ;
            transaction.unblock();
        }
    }

    /**
     * Promote a transaction's lock on `name` to `newLockType` (i.e. change
     * the transaction's lock on `name` from the current lock type to
     * `newLockType`, if its a valid substitution).
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
        boolean shouldBlock = false;
        synchronized (this) {
            ResourceEntry resourceEntry = getResourceEntry(name);
            LockType currentType = getLockType(transaction,name);
            List<Lock> allLocks = getLocks(transaction);
            if(allLocks.isEmpty()){
                throw new NoLockHeldException("no current Lock");
            }
            if(currentType == newLockType){
                throw new DuplicateLockRequestException("duplicate lock");
            }
            boolean canPromote = LockType.substitutable(newLockType,currentType) ;
            if(canPromote){
                boolean doesCompatible = resourceEntry.checkCompatible(newLockType,transaction.getTransNum());
                if(doesCompatible){
                    Lock lock = new Lock(name,newLockType,transaction.getTransNum());
                    List<Lock> all_Lock = getLocks(name);
                    for(int i = 0 ;i < all_Lock.size();i ++){
                        if(all_Lock.get(i).transactionNum == transaction.getTransNum()){
                            resourceEntry.locks.set(i,lock);
                            Long trans_id = transaction.getTransNum();
                            transactionLocks.get(trans_id).remove(all_Lock.get(i));
                            transactionLocks.get(trans_id).add(lock);
                            break;
                        }
                    }
                }
                else {
                    Lock lock = new Lock(name,newLockType,transaction.getTransNum());
                    resourceEntry.addToQueue(new LockRequest(transaction,lock), true);
                    shouldBlock = true ;
                    transaction.prepareBlock();
                }
            }
            else {
                throw new InvalidLockException("can't promote");
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
        for(Lock lock : resourceEntry.locks){
            if(lock.transactionNum == transaction.getTransNum() ) return lock.lockType;
        }
        return LockType.NL;
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
