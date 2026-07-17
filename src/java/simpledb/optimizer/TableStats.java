package simpledb.optimizer;

import simpledb.common.Database;
import simpledb.common.Type;
import simpledb.execution.Predicate;
import simpledb.storage.*;
import simpledb.transaction.TransactionId;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query. 
 * 
 * This class is not needed in implementing lab1 and lab2.
 */
public class TableStats {

    private static final ConcurrentMap<String, TableStats> statsMap = new ConcurrentHashMap<>();

    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }
    
    public static void setStatsMap(Map<String,TableStats> s)
    {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException | IllegalAccessException | IllegalArgumentException | SecurityException e) {
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

    private int tableId;
    private int ioCostPerPage;
    private DbFile file;
    private IntHistogram[] intHistogram;
    private int[] minNum;
    private int[] maxNum;
    private int tupleNum;

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
        this.tableId = tableid;
        this.ioCostPerPage = ioCostPerPage;
        this.tupleNum = 0;

        this.file = Database.getCatalog().getDatabaseFile(tableid);
        TupleDesc td = this.file.getTupleDesc();

        int n = td.numFields();
        this.intHistogram = new IntHistogram[n];
        this.minNum = new int[n]; Arrays.fill(this.minNum, Integer.MAX_VALUE);
        this.maxNum = new int[n]; Arrays.fill(this.maxNum, Integer.MIN_VALUE);

        Type[] types = new Type[n];
        for(int i=0; i<n; i++){
            types[i] = td.getFieldType(i);
        }

        TransactionId tid = new TransactionId();
        DbFileIterator it = file.iterator(tid);

        this.setMinAndMax(n, types, it);
        this.setIntHistogram(n, types, it);
        // try{
        //     it.open();
        //     while(it.hasNext()){
        //         Tuple t = it.next();
        //         this.tupleNum ++;
        //         for(int i=0; i<n; i++){
        //             if(types[i] == Type.INT_TYPE){
        //                 this.intHistogram[i].addValue(((IntField)t.getField(i)).getValue());
        //             }
        //         }
        //         this.tupleNum ++;
        //     }
        // }catch(Exception e){
        //     return;
        // }
    }

    private void setMinAndMax(int num, Type[] types, DbFileIterator it){
        this.minNum = new int[num]; Arrays.fill(this.minNum, Integer.MAX_VALUE);
        this.maxNum = new int[num]; Arrays.fill(this.maxNum, Integer.MIN_VALUE);

        try{
            it.rewind();
            while(it.hasNext()){
                Tuple t = it.next();
                for(int i=0; i<num; i++){
                    if(types[i] == Type.INT_TYPE){
                        int value = ((IntField)t.getField(i)).getValue();
                        if(value < this.minNum[i]) this.minNum[i] = value;
                        if(value > this.maxNum[i]) this.maxNum[i] = value;
                    }
                }
            }
        }catch(Exception e){
            return;
        }
    }

    private void setIntHistogram(int num, Type[] types, DbFileIterator it){
        for(int i=0; i<num; i++){
            this.intHistogram[i] = new IntHistogram(
                TableStats.NUM_HIST_BINS, 
                this.minNum[i],
                this.maxNum[i]
            );
        }

        try{
            it.rewind();
            while(it.hasNext()){
                Tuple t = it.next();
                this.tupleNum ++;
                for(int i=0; i<num; i++){
                    if(types[i] == Type.INT_TYPE){
                        int value = ((IntField)t.getField(i)).getValue();
                        this.intHistogram[i].addValue(value);
                    }
                }
            }
        }catch(Exception e){
            return;
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
        // some code goes here
        DbFile file = Database.getCatalog().getDatabaseFile(this.tableId);
        return (double)(((HeapFile)file).numPages() * this.ioCostPerPage);
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
        // some code goes here
        return (int)(this.tupleNum * selectivityFactor);
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
        double result = 0;
    
        // 获取该字段的直方图
        if (field >= 0 && field < intHistogram.length && intHistogram[field] != null) {
            IntHistogram hist = intHistogram[field];
            
            switch (op) {
                case EQUALS:
                    // 平均相等选择率 = 直方图的平均选择率
                    return hist.avgSelectivity();
                case NOT_EQUALS:
                    // 平均不相等选择率 = 1 - 平均相等选择率
                    return 1.0 - hist.avgSelectivity();
                case LESS_THAN:
                case GREATER_THAN:
                    // 平均来说，一半的元组满足条件
                    return 0.5;
                case LESS_THAN_OR_EQ:
                case GREATER_THAN_OR_EQ:
                    // 包含等号，略大于0.5
                    return 0.5 + hist.avgSelectivity();
                default:
                    return 0.5;
            }
        }
        
        return result;
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
        // some code goes here
        if (field < 0 || field >= intHistogram.length || intHistogram[field] == null) {
            return 1.0;  // 如果没有直方图信息，返回保守估计
        }
        
        // 只处理 INT_TYPE 字段
        if (constant.getType() != Type.INT_TYPE) {
            return 1.0;
        }
        
        IntField intField = (IntField) constant;
        int value = intField.getValue();

        // 使用该字段的直方图来估算选择率
        return intHistogram[field].estimateSelectivity(op, value);
    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
        // some code goes here
        return this.tupleNum;
    }

}
