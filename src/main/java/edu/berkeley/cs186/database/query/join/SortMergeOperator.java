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
         * Returns the next record that should be yielded from this join,
         * or null if there are no more records to join.
         */
        private Record fetchNextRecord() {
            // TODO(proj3_part1): implement
            if(leftRecord == null) return null;
//            if(!marked){
//                rightIterator.markNext();
//                marked = true ;
//            }
//           rightIterator.reset();
//            while(leftIterator.hasNext()){
//                System.out.println(leftIterator.next());
//            }
//            System.out.println("\n");
//
//            while(rightIterator.hasNext()){
//                System.out.println(rightIterator.next());
//            }
//            System.out.println("\n");



//            while(true) {
//                if(compare(leftRecord,rightRecord) > 0){
//                    marked = false;
////                    System.out.println(leftRecord);
////                    System.out.println(rightRecord);
//                    while(rightIterator.hasNext() && compare(leftRecord,rightRecord) >0){
//                        rightRecord = rightIterator.next();
//                        rightIterator.markPrev();
//                    }
//                }
//                else if(compare(leftRecord,rightRecord) <0){
//                    marked = false;
//                    leftRecord = leftIterator.next();
//                    rightIterator.reset();
//                    rightRecord =rightIterator.next();
//                }
//                else if(compare(leftRecord,rightRecord) == 0){
//                    Record currLeft = leftRecord;
//                    Record currRight = rightRecord;
//                    if(leftIterator.hasNext()){
//                        if(rightIterator.hasNext()){
//                            currRight = rightRecord;
//                            rightRecord = rightIterator.next();
//                            return currLeft.concat(currRight);
//                        }
//                        else {
//                            rightIterator.reset();
//                            currRight = rightRecord;
//                            rightRecord =rightIterator.next();
//                            currLeft = leftRecord;
//                            leftRecord = leftIterator.next();
//                            return currLeft.concat(currRight);
//                        }
//
//                    }
//                    else {
//
//                        if(!marked){
//                            marked = true;
//                            rightIterator.reset();
//                        }
//                        while(rightIterator.hasNext()) {
//                            if(compare(leftRecord,rightRecord) == 0){
//                                currRight = rightRecord;
//                                rightRecord = rightIterator.next();
//                                return currLeft.concat(currRight);
//
//                            }
//                        }
//                        return null ;
//                    }
//                }
//            }
            //return null;
           while(true){
//               System.out.println(leftRecord);
//               System.out.println(rightRecord);
//               System.out.println("\n");
               if(leftRecord == null) return null;
               if(compare(leftRecord,rightRecord) < 0){
                   if(marked){
                       marked = false;
                       if(leftIterator.hasNext()) leftRecord = leftIterator.next();
                       else leftRecord = null;
                       rightIterator.reset();
                       if(rightIterator.hasNext()) rightRecord = rightIterator.next();
                       else leftRecord = null;
                   }
                   else {
                       if(rightIterator.hasNext()) {
                           rightRecord = rightIterator.next();
                       }
                       else {
                           rightIterator.reset();
                           rightRecord = rightIterator.next();
                           if(leftIterator.hasNext()) leftRecord = leftIterator.next();
                           else leftRecord = null;
                       }
                   }
               }
               else if(compare(leftRecord,rightRecord) > 0){
                   while(rightIterator.hasNext()){
                       rightRecord = rightIterator.next();
                       if(compare(rightRecord,leftRecord) >= 0){
                           rightIterator.markPrev();
                           break;
                       }
                   }
                   if(compare(rightRecord,leftRecord) < 0) leftRecord = null;
               }
               else {
                   Record currLeft = leftRecord;
                   Record currRight = rightRecord;
                   marked = true ;
                   if(rightIterator.hasNext()){
                       rightRecord = rightIterator.next();
                       return currLeft.concat(currRight);
                   }
                   else {
                       rightIterator.reset();
                       if(!rightIterator.hasNext()) leftRecord = null;
                       if(leftRecord == null){
                           return currLeft.concat(currRight);
                       }
                       rightRecord = rightIterator.next();
                       if(leftIterator.hasNext()) leftRecord = leftIterator.next();
                       else leftRecord = null;
                       return currLeft.concat(currRight);
                   }
               }
           }

        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}
