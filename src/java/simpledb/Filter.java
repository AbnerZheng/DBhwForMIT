package simpledb;

import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;
    private final Predicate predicate;
    private OpIterator opIterator;

    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     * 
     * @param p
     *            The predicate to filter tuples with
     * @param child
     *            The child operator
     */
    public Filter(Predicate p, OpIterator child) {
    	this.predicate = p;
    	this.opIterator = child;
    }

    public Predicate getPredicate() {
    	return predicate;
    }

    public TupleDesc getTupleDesc() {
    	return opIterator.getTupleDesc();
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
    	super.open();
        this.opIterator.open();
    }

    public void close() {
    	super.close();
    	this.opIterator.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
    	this.opIterator.rewind();
    }

    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     * 
     * @return The next tuple that passes the filter, or null if there are no
     *         more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {
    	while (this.opIterator.hasNext()){
		    final Tuple next = this.opIterator.next();
		    final boolean filter = predicate.filter(next);
		    if(filter){
		    	return next;
		    }
	    }
	    return null;
    }

	@Override
	public OpIterator[] getChildren() {
		return new OpIterator[]{this.opIterator};
	}

	@Override
	public void setChildren(OpIterator[] children) {
		assert children.length == 1;
		this.opIterator = children[0];
	}
}
