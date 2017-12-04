package simpledb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
	private final Op op;
	private final int afield;
	private final Type gbFieldType;
	private final int gbField;
	private final HashMap<Field, Integer> groupData;

	/**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
    	this.gbField = gbfield;
    	this.gbFieldType = gbfieldtype;
    	this.afield = afield;
    	this.op = what;
    	if(what != Op.COUNT){
    		throw new IllegalArgumentException();
	    }
	    this.groupData = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
    	Field key;
    	if(gbField == NO_GROUPING){
			key = null;
	    }else{
		    key = tup.getField(gbField);
	    }
	    groupData.compute(key, (k,v)->{
			if(v == null){
				return 1;
			}
			return v + 1;
	    });
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
    	return new OpIterator() {
		    private Iterator<Field> keyIterator;
		    private boolean opened;
		    @Override
		    public void open() throws DbException, TransactionAbortedException {
				opened = true;
				keyIterator = groupData.keySet().iterator();
		    }

		    @Override
		    public boolean hasNext() throws DbException, TransactionAbortedException {
		    	if(!opened) throw new DbException("not opened");
			    return keyIterator.hasNext();
		    }

		    @Override
		    public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
		    	if(hasNext()){
				    final Field key = keyIterator.next();
				    final Tuple tuple = new Tuple(getTupleDesc());
				    tuple.setField(0, key);
				    tuple.setField(1, new IntField(groupData.get(key)));
				    return tuple;
			    }
			    throw new NoSuchElementException();
		    }

		    @Override
		    public void rewind() throws DbException, TransactionAbortedException {
				if(!opened) throw new DbException("not opened");
				keyIterator = groupData.keySet().iterator();
		    }

		    @Override
		    public TupleDesc getTupleDesc() {
		    	Type[] types = new Type[2];
		    	types[0] = gbFieldType;
		    	types[1] = Type.INT_TYPE;
			    return new TupleDesc(types);
		    }

		    @Override
		    public void close() {
				opened = false;
		    }
	    };
    }

}
