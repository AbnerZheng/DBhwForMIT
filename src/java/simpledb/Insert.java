package simpledb;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;
    private final OpIterator[] children = new OpIterator[1];
    private int count;
    private boolean opend = false;
    private int nextIndex = 0;

    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
    	this.count = 0;
    	this.children[0] = child;
        try {
            child.open();
            while (child.hasNext()){
                final Tuple next = child.next();
                Database.getBufferPool().insertTuple(t, tableId, next);
                this.count += 1;
            }
        } catch (TransactionAbortedException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public TupleDesc getTupleDesc() {
	    Type[] types = new Type[1];
	    types[0] = Type.INT_TYPE;
	    return new TupleDesc(types);
    }

    public void open() throws DbException, TransactionAbortedException {
    	this.opend = true;
    }

    public void close() {
    	this.opend = false;
    }

    public void rewind() throws DbException, TransactionAbortedException {
    	this.nextIndex = 0;
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
    	if(nextIndex == 0){
    		nextIndex += 1;
		    final Tuple tuple = new Tuple(getTupleDesc());
		    tuple.setField(0, new IntField(count));
		    return tuple;
	    }
	    return null;
    }

    @Override
    public OpIterator[] getChildren() {
        return children;
    }

    @Override
    public void setChildren(OpIterator[] children) {
    	assert children.length == 1;
    	this.children[0] = children[0];
    }
}
