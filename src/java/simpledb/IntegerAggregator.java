package simpledb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
	private final Op op;
	private final int aField;
	private final Type gbFieldType;
	private final int gbField;
	private final boolean hasGroup;
	private final HashMap<Field, Integer> groupData;
	private final HashMap<Field, Integer> groupCount;

	/**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
    	this.gbField = gbfield;
    	this.gbFieldType = gbfieldtype;
    	this.aField = afield;
    	this.op = what;
	    this.hasGroup = gbfield != NO_GROUPING;
	    this.groupData = new HashMap<>();
	    this.groupCount = new HashMap<>();
    }

	/**
	 * Merge a new tuple into the aggregate, grouping as indicated in the
	 * constructor
	 *
	 * @param tup
	 *            the Tuple containing an aggregate field and a group-by field
	 */
	public void mergeTupleIntoGroup(Tuple tup) {
		final int cur = ((IntField) tup.getField(aField)).getValue();
		Field key;
		if(!hasGroup){
			key = null;
		}else {
			key = tup.getField(gbField);
		}
		switch (op){
			case MIN:
				this.groupData.compute(key, (k,v)->{
					if(v == null){
						return cur;
					}
					if(cur < v){
						return cur;
					}
					return v;
				});
				break;
			case MAX:
				this.groupData.compute(key, (k,v)->{
					if(v == null){
						return cur;
					}
					if(cur > v) {
						return cur;
					}
					return v;
				});
				break;
			case AVG:
				this.groupCount.compute(key, (k,v)->{
					if(v != null){
						return v+1;
					}else{
						return 1;
					}
				});
				this.groupData.compute(key, (k,v)->{
					if(v == null){
						return cur;
					}else{
						return v + cur;
					}
				});
				break;
			case SUM:
				this.groupData.compute(key, (k,v)->{
					if(v==null){
						return cur;
					}else {
						return cur + v;
					}
				});
				break;
			case COUNT:
				this.groupData.compute(key, (k,v)->{
					if(v != null){
						return v+1;
					}else{
						return 1;
					}
				});
				break;
			case SC_AVG:
			case SUM_COUNT:
				System.out.println("not implement");
		}

	}

	/**
	 * Create a OpIterator over group aggregate results.
	 *
	 * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
	 *         if using group, or a single (aggregateVal) if no grouping. The
	 *         aggregateVal is determined by the type of aggregate specified in
	 *         the constructor.
	 */
	public OpIterator iterator() {
		return new OpIterator() {
			private Iterator<Field> keyIterator;
			private boolean opened;
			@Override
			public void open() throws DbException, TransactionAbortedException {
				this.opened = true;
				this.keyIterator = groupData.keySet().iterator();
			}


			@Override
			public boolean hasNext() throws DbException, TransactionAbortedException
			{
				return this.keyIterator.hasNext();
			}

			@Override
			public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
				if(!opened){
					throw new DbException("haven't been opened");
				}
				final Field key = this.keyIterator.next();
				final Tuple tuple = new Tuple(getTupleDesc());
				IntField result = null;
				switch (op){
					case SUM:
					case MAX:
					case MIN:
						result = new IntField(groupData.get(key));
						break;
					case COUNT:
						result = new IntField(groupData.get(key));
						break;
					case AVG:
						final int i = groupData.get(key) / groupCount.get(key);
						result = new IntField(i);
						break;
				}
				tuple.setField(0, key);
				tuple.setField(1, result);
				return tuple;

			}

			@Override
			public void rewind() throws DbException, TransactionAbortedException {
				this.keyIterator = groupData.keySet().iterator();
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
				this.opened = false;
			}
		};
	}

}
