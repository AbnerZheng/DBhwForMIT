package simpledb;

import java.util.*;

import static simpledb.Aggregator.NO_GROUPING;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;
	private final int gField;
	private final Aggregator.Op aop;
	private final int aField;
	private final OpIterator childIterator;
	private OpIterator child;

	/**
     * Constructor.
     * 
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     * 
     * 
     * @param child
     *            The OpIterator that is feeding us tuples.
     * @param afield
     *            The column over which we are computing an aggregate.
     * @param gfield
     *            The column over which we are grouping the result, or -1 if
     *            there is no grouping
     * @param aop
     *            The aggregation operator to use
     */
    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
    	this.child = child;
    	this.gField = gfield;
    	this.aField = afield;
    	this.aop = aop;
	    final Type fieldType = child.getTupleDesc().getFieldType(afield);
	    Type groupType = child.getTupleDesc().getFieldType(gfield);

	    Aggregator childAggregator;
	    if(fieldType.equals(Type.INT_TYPE)){
		    childAggregator = new IntegerAggregator(gfield, groupType, afield, aop);
	    }else {
	    	childAggregator = new StringAggregator(gfield, groupType, afield, aop);
	    }
	    try {
	    	child.rewind();
	    	child.open();
		    while (child.hasNext()){
			    final Tuple next = child.next();
			    childAggregator.mergeTupleIntoGroup(next);
		    }
	    } catch (DbException e) {
		    e.printStackTrace();
	    } catch (TransactionAbortedException e) {
		    e.printStackTrace();
	    }
	    this.childIterator = childAggregator.iterator();
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link simpledb.Aggregator#NO_GROUPING}
     * */
    public int groupField() {
    	return gField;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     *         null;
     * */
    public String groupFieldName() {
    	return child.getTupleDesc().getFieldName(gField);
    }

    /**
     * @return the aggregate field
     * */
    public int aggregateField() {
    	return aField;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     * */
    public String aggregateFieldName() {
    	return child.getTupleDesc().getFieldName(aField);
    }

    /**
     * @return return the aggregate operator
     * */
    public Aggregator.Op aggregateOp() {
    	return aop;
    }

	public static String nameOfAggregatorOp(Aggregator.Op aop) {
		return aop.toString();
	}

	public void open() throws NoSuchElementException, DbException,
		TransactionAbortedException {
		super.open();
		this.childIterator.open();
	}

	/**
	 * Returns the next tuple. If there is a group by field, then the first
	 * field is the field by which we are grouping, and the second field is the
	 * result of computing the aggregate. If there is no group by field, then
	 * the result tuple should contain one field representing the result of the
	 * aggregate. Should return null if there are no more tuples.
	 */
	protected Tuple fetchNext() throws TransactionAbortedException, DbException {
		if(this.childIterator.hasNext()){
			final Tuple next = this.childIterator.next();
			return next;
		}
		return null;
	}

	public void rewind() throws DbException, TransactionAbortedException {
		this.childIterator.rewind();
	}

	/**
	 * Returns the TupleDesc of this Aggregate. If there is no group by field,
	 * this will have one field - the aggregate column. If there is a group by
	 * field, the first field will be the group by field, and the second will be
	 * the aggregate value column.
	 *
	 * The name of an aggregate column should be informative. For example:
	 * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
	 * given in the constructor, and child_td is the TupleDesc of the child
	 * iterator.
	 */
	public TupleDesc getTupleDesc() {
		if(gField == NO_GROUPING){
			final Type[] types = new Type[1];
			types[0] = Type.INT_TYPE;
			return new TupleDesc(types);
		}

		final Type[] types = new Type[2];
		types[0] = child.getTupleDesc().getFieldType(aField);
		types[1] = Type.INT_TYPE;
		return new TupleDesc(types);
	}

	public void close() {
		super.close();
		this.childIterator.close();
	}

	@Override
	public OpIterator[] getChildren() {
		return new OpIterator[]{child};
	}

	@Override
	public void setChildren(OpIterator[] children) {
		this.child = children[0];
	}

}
