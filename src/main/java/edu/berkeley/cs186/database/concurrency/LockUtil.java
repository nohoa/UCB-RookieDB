package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

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
        if(explicitLockType == requestType) return ;
        // TODO(proj4_part2): implement
        List<LockContext> contextList = new ArrayList<LockContext>();
        contextList.add(lockContext);
        while(parentContext != null){
            contextList.add(parentContext);
            parentContext = parentContext.parentContext();
        }
        Collections.reverse(contextList);
        if(requestType == LockType.S){
            boolean is_start_S = false ;
            if(lockContext.getEffectiveLockType(transaction) == LockType.S){
                is_start_S = true ;
            }
            boolean does_X = false;
            boolean does_SIX = false ;

            for(int i = 0 ;i < contextList.size()  ;i ++){
                LockContext parent = contextList.get(i);
                LockType par = parent.getExplicitLockType(transaction);

                if(par == LockType.SIX){
                    does_SIX = true ;
                }
            }
            for(int i = 0 ;i < contextList.size()-1 ;i ++ ){
                LockContext parent = contextList.get(i);
                LockType par = parent.getExplicitLockType(transaction);
                if(par == LockType.NL){
                    if(does_SIX) continue;
                    else parent.acquire(transaction,LockType.IS);
                }
                else if(par == LockType.IX) {

                   // if(i == 0) continue ;
                   if(is_start_S) parent.promote(transaction,LockType.SIX);
                }
                else if(par == LockType.X) {
                    does_X = true;
                }
                else if(par == LockType.S){
                    if(does_X) parent.promote(transaction,LockType.SIX);
                    return ;
                }
            }

            if(contextList.get(contextList.size()-1).getExplicitLockType(transaction) == LockType.NL){
                 contextList.get(contextList.size()-1).acquire(transaction,LockType.S);
            }
            else if(contextList.get(contextList.size()-1).getExplicitLockType(transaction) == LockType.IX){
                contextList.get(contextList.size()-1).promote(transaction,LockType.SIX);
            }
            else {
                contextList.get(contextList.size()-1).escalate(transaction);
            }
        }
        else if(requestType == LockType.NL) ;
        else {
            for(int i = 0 ;i < contextList.size()-1 ;i ++ ){
                LockContext parent = contextList.get(i);
                LockType par = parent.getExplicitLockType(transaction);
                if(par == LockType.IS) parent.promote(transaction,LockType.IX);
                else if(par == LockType.NL) {
                    parent.acquire(transaction,LockType.IX);
                }
                else if(par == LockType.X){
                    return ;
                }
            }
                if(contextList.get(contextList.size()-1).getExplicitLockType(transaction) == LockType.NL) contextList.get(contextList.size()-1).acquire(transaction,LockType.X);
                else  if(contextList.get(contextList.size()-1).getExplicitLockType(transaction) == LockType.S )contextList.get(contextList.size()-1).promote(transaction,LockType.X);
        }
        return;
    }

    // TODO(proj4_part2) add any helper methods you want
}
