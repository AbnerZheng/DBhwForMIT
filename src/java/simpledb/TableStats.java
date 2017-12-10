package simpledb;

import org.omg.CORBA.DATA_CONVERSION;

import javax.xml.crypto.Data;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query. 
 * 
 * This class is not needed in implementing lab1 and lab2.
 */
public class TableStats {

    private static final ConcurrentHashMap<String, TableStats> statsMap = new ConcurrentHashMap<>();

    static final int IOCOSTPERPAGE = 1000;
    private final ArrayList<IntHistogram> intHistograms;
    private final ArrayList<StringHistogram> stringHistograms;
    private final TupleDesc tupleDesc;
    private final int ioCostPerPage;
    private final int tableid;
    private int count;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }
    
    public static void setStatsMap(HashMap<String,TableStats> s)
    {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        } catch (SecurityException e) {
            e.printStackTrace();
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;

    private int indexOfField(int field){
        final Type fieldType = tupleDesc.getFieldType(field);
        int result = -1;
        for (int i = 0; i <= field; i++) {
            if(fieldType.equals(tupleDesc.getFieldType(i))){
                result += 1;
            }
        }
        return result;

    }

    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     * 
     * @param tableid
     *            The table over which to compute statistics
     * @param ioCostPerPage
     *            The cost per page of IO. This doesn't differentiate between
     *            sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.
        // some code goes here
	    this.ioCostPerPage = ioCostPerPage;
	    this.tableid = tableid;
        final DbFile databaseFile = Database.getCatalog().getDatabaseFile(tableid);
        final DbFileIterator iterator = databaseFile.iterator(new TransactionId());
        this.tupleDesc = databaseFile.getTupleDesc();
        final int numFields = tupleDesc.numFields();
        this.intHistograms = new ArrayList<>();
        this.stringHistograms = new ArrayList<>();
        this.count = 0;

        int[] maxs = new int[numFields];
        int[] mins = new int[numFields];
        for (int i = 0; i < numFields; i++) {
            maxs[i] = Integer.MIN_VALUE;
            mins[i] = Integer.MAX_VALUE;
        }

        // first phase to get min and max
        try {
        	iterator.open();
            while (iterator.hasNext()){
            	this.count += 1;
                final Tuple next = iterator.next();
                for (int i = 0; i < numFields; i++) {
                    final Field field = next.getField(i);
                    switch (field.getType()) {
                        case INT_TYPE:
                            final IntField field1 = (IntField) field;
                            final int value = field1.getValue();
                            if(value < mins[i]){
                                mins[i] = value;
                            }
                            if(value > maxs[i]){
                                maxs[i] = value;
                            }
                    }
                }
            }
            for (int i = 0; i < numFields; i++) {
                final Type fieldType = tupleDesc.getFieldType(i);
                switch (fieldType){
                    case INT_TYPE:
                        final int buckets_bin = Integer.min(NUM_HIST_BINS, maxs[i] - mins[i] + 1);
                        this.intHistograms.add(new IntHistogram(buckets_bin, mins[i], maxs[i]));
                        break;
                    case STRING_TYPE:
                        this.stringHistograms.add(new StringHistogram(NUM_HIST_BINS));
                        break;
                }
            }

            iterator.rewind();
            while (iterator.hasNext()){
                final Tuple next = iterator.next();
                for (int i = 0; i < numFields; i++) {
                    final Field field = next.getField(i);
                    final int index = indexOfField(i);
                    switch(field.getType()){
                        case STRING_TYPE:
                            final StringField field1 = (StringField) field;
                            this.stringHistograms.get(index).addValue(field1.getValue());
                            break;
                        case INT_TYPE:
                            final IntField field2 = (IntField) field;
                            this.intHistograms.get(index).addValue(field2.getValue());
                            break;
                    }
                }
            }
        } catch (DbException e) {
            e.printStackTrace();
        } catch (TransactionAbortedException e) {
            e.printStackTrace();
        }
    }


    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     *
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     *
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        final DbFile databaseFile = Database.getCatalog().getDatabaseFile(tableid);
        return ((HeapFile) databaseFile).numPages() * ioCostPerPage;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     *
     * @param selectivityFactor
     *            The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
    	return (int)(count * selectivityFactor);
    }

    /**
     * The average selectivity of the field under op.
     * @param field
     *        the index of the field
     * @param op
     *        the operator in the predicate
     * The semantic of the method is that, given the table, and then given a
     * tuple, of which we do not know the value of the field, return the
     * expected selectivity. You may estimate this value from the histograms.
     * */
    public double avgSelectivity(int field, Predicate.Op op) {
        // some code goes here
        return 1.0;
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     *
     * @param field
     *            The field over which the predicate ranges
     * @param op
     *            The logical operation in the predicate
     * @param constant
     *            The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        final Type fieldType = tupleDesc.getFieldType(field);
        final int index = indexOfField(field);

        switch (fieldType){
            case INT_TYPE:
                final IntHistogram intHistogram = intHistograms.get(index);
                final IntField constant1 = (IntField) constant;
                return intHistogram.estimateSelectivity(op, constant1.getValue());
            default:
                final StringHistogram stringHistogram = stringHistograms.get(index);
                final StringField constant2 = (StringField) constant;
                return stringHistogram.estimateSelectivity(op, constant2.getValue());
        }
    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
        // some code goes here
        return 0;
    }

}
