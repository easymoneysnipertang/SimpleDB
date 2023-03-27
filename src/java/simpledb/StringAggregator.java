package simpledb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbField;
    private Type gbFieldType;
    private int aField;
    private Op operator;
    private HashMap<Field,Integer>group=new HashMap<>();// 干脆直接用field做key，反正最后转回tuple都得变成field
    
    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
    	this.gbField=gbfield;
    	this.gbFieldType=gbfieldtype;
    	this.aField=afield;
    	this.operator=what;
    	if(operator!=Op.COUNT)
    		throw new IllegalArgumentException("wrong op!");
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {// String只是afield是String？那有啥用呢？
        // some code goes here
    	Field gb;

    	if(this.gbField==Aggregator.NO_GROUPING)
    		gb=null;
    	else {
    		gb=tup.getField(this.gbField);
        	if(gbFieldType!=gb.getType())
        		throw new IllegalArgumentException("wrong type!");
    	}
    	// 只做count！
    	if(!group.containsKey(gb))
    		group.put(gb, 1);
    	else
    		group.put(gb, group.get(gb)+1);
    	
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
        // some code goes here
        return new StrAggIterator();
    }
    
    public class StrAggIterator implements OpIterator{
    	private TupleDesc td;
    	private	Iterator<HashMap.Entry<Field,Integer>> it;
    	
    	public StrAggIterator() {
    		// 构造tupleDesc->typeAr,fieldAr
    		if(gbField==Aggregator.NO_GROUPING)
    			td=new TupleDesc(new Type[] {Type.INT_TYPE},new String[] {"aggregateVal"});
    		else 
    			td=new TupleDesc(new Type[] {gbFieldType,Type.INT_TYPE},new String []{"groupVal","aggregateVal"});
    	}
    	
		@Override
		public void open() throws DbException, TransactionAbortedException {
			// TODO Auto-generated method stub
			it=group.entrySet().iterator();
		}

		@Override
		public boolean hasNext() throws DbException, TransactionAbortedException {
			// TODO Auto-generated method stub
			return it.hasNext();
		}

		@Override
		public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
			// TODO Auto-generated method stub
			Map.Entry<Field, Integer> temp=it.next();
			Tuple ret=new Tuple(td);
			if(gbField==Aggregator.NO_GROUPING)// (aggregateVal)
				ret.setField(0, new IntField(temp.getValue()));
			else {// (groupVal,aggregateVal)
				ret.setField(0, temp.getKey());
				ret.setField(1, new IntField(temp.getValue()));
			}
			return ret;
		}

		@Override
		public void rewind() throws DbException, TransactionAbortedException {
			// TODO Auto-generated method stub
			it=group.entrySet().iterator();
		}

		@Override
		public TupleDesc getTupleDesc() {
			// TODO Auto-generated method stub
			return td;
		}

		@Override
		public void close() {
			// TODO Auto-generated method stub
			it=null;
		}
    	
    }

}
