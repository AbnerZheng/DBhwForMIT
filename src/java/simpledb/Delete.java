package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;
    private int count;
    private OpIterator[] children = new OpIterator[1];
    private int nextIndex;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, OpIterator child) {
        children[0] = child;
        try {
            child.open();
            while (child.hasNext()){
                Tuple next = child.next();
                Database.getBufferPool().deleteTuple(t, next);
                this.count += 1;
            }
        } catch (DbException e) {
            e.printStackTrace();
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
    	super.open();
    }

    public void close() {
    	super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
    	this.nextIndex = 0;
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
    	if(this.nextIndex == 0){
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
