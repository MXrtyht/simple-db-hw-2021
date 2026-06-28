package simpledb.execution;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private class AggState{
        private int min;
        private int max;
        private int sum;
        private int avg;
        private int count;

        public AggState(){
            this.min = Integer.MAX_VALUE;
            this.max = Integer.MIN_VALUE;
            this.sum = 0;
            this.avg = 0;
            this.count = 0;
        }

        public void update(int num){
            if(num < this.min){
                this.min = num;
            }
            if(num > this.max){
                this.max = num;
            }
            this.sum += num;
            this.count ++;
            this.avg = this.sum / this.count;
        }

        public int getMin() {return this.min;}
        public int getMax() {return this.max;}
        public int getSum() {return this.sum;}
        public int getAvg() {return this.avg;}
        public int getCount() {return this.count;}
    }

    private int gbfield;
    private Type gbfieldType;
    private int afield;
    private Op what;
    private Map<Field, AggState> agg;

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
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldType = gbfieldtype;
        this.afield = afield;
        this.what = what;
        this.agg = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        Field key = this.gbfieldType == null ? new IntField(0) : tup.getField(this.gbfield);
        if (!this.agg.containsKey(key)) {
            this.agg.put(key, new AggState());
        }
        this.agg.get(key).update(((IntField) tup.getField(this.afield)).getValue());
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
        // some code goes here
        return new OpIterator() {
            List <Tuple> tList;
            int cursor;
            TupleDesc td;

            @Override
            public void open() throws DbException, TransactionAbortedException {
                this.tList = new ArrayList<>();
                this.cursor = 0;

                if(gbfield == NO_GROUPING){
                    Type[] fieldType = new Type[]{Type.INT_TYPE};
                    String[] field = new String[]{what.toString()};
                    this.td = new TupleDesc(fieldType, field);
                }else{
                    Type[] fieldType = new Type[]{gbfieldType,Type.INT_TYPE};
                    String[] field = new String[]{"GROUP",what.toString()};
                    this.td = new TupleDesc(fieldType, field);
                }

                for (Map.Entry<Field, AggState> entry : agg.entrySet()) {
                    Field key = entry.getKey();
                    AggState value = entry.getValue();

                    int result;
                    switch(what){
                        case MIN:    result = value.getMin(); break;
                        case MAX:    result = value.getMax(); break;
                        case SUM:    result = value.getSum(); break;
                        case AVG:    result = value.getAvg(); break;
                        case COUNT:  result = value.getCount(); break;
                        default:     result = 0;
                    }

                    if(gbfield == NO_GROUPING){
                        Tuple t = new Tuple(td);
                        t.setField(0, new IntField(result));
                        tList.add(t);
                    }else{
                        Tuple t = new Tuple(td);
                        t.setField(0, key);
                        t.setField(1, new IntField(result));
                        tList.add(t);
                    }
                }
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                if(this.cursor < this.tList.size()){
                    return true;
                }
                return false;
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if (cursor >= tList.size()) {
                    throw new NoSuchElementException("index out of range");
                }
                Tuple result = this.tList.get(cursor);
                cursor++;
                return result;
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                this.close();
                this.open();
            }

            @Override
            public TupleDesc getTupleDesc() {
                return td;
            }

            @Override
            public void close() {
                this.tList.clear();
                this.cursor = 0;
                this.td = null;
            }
            
        };
    }

}
