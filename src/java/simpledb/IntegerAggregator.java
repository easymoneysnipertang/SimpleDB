package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 * 
 * 聚合（Aggregate）是指对表格中的数据进行计算并生成单个结果的操作。
 * 通常情况下，聚合函数（Aggregate Function）会对一列或多列数据进行计算，生成汇总值。
 * 
 * 分组（Group）是指将数据按照某一或某几个列的值进行分类，然后对每个分类中的数据进行聚合计算。
 * 这个操作会生成多个结果，每个结果对应一个或多个行，并按照分组列的值进行标识。
 * 
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbField;// index
    private Type gbFieldType;
    private int aField;// index
    private Op operator;
    
//    private HashMap<Integer,Integer> group=new HashMap<>();// (groupValue,aggregateValue)!
//    // 错了！麻烦了，你最后iterator返回的值还是field！groupValue只是schema :(
//    private HashMap<Integer,Integer> avgCount=new HashMap<>();// 为了求平均值，记录每个组有多少元素
    
    private HashMap<Field,Integer> group=new HashMap<>();
    private HashMap<Field,ArrayList<Integer>> avgCount=new HashMap<>();// 必须得用list去记录每一个值，不然在四舍五入过程中会造成结果错误
    
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
    	this.gbField=gbfield;
    	this.gbFieldType=gbfieldtype;
    	this.aField=afield;
    	this.operator=what;
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
    	// 对新加的一个tuple进行聚合操作，并进行分组
    	// 先得到gbField和aField两个字段
    	Field gb;
    	//int gbValue=-1;
    	if(this.gbField==Aggregator.NO_GROUPING)
    		gb=null;
    	else {
    		gb=tup.getField(this.gbField);
    		//gbValue=gbField.getValue();
    		if(gbFieldType!=gb.getType())// 类型不符
        		throw new IllegalArgumentException("wrong type!");
    	}
    	
    	IntField aField=(IntField) tup.getField(this.aField);
    	int aValue=aField.getValue();
    	
    	// 对新加的这一行先判断是哪一组，再做aggregate
    	switch(this.operator) {
    	case MIN:
    		if(!group.containsKey(gb))// 该组还没有东西
    			group.put(gb, aValue);
    		else// 组里有东西，执行一次aggregate
    			group.put(gb, Math.min(aValue, group.get(gb)));
    		break;
    	case MAX:
    		if(!group.containsKey(gb))
    			group.put(gb, aValue);
    		else
    			group.put(gb, Math.max(aValue, group.get(gb)));
    		break;
    	case SUM:
    		if(!group.containsKey(gb))
    			group.put(gb, aValue);
    		else
    			group.put(gb, aValue+group.get(gb));
    		break;
    	case COUNT:
    		if(!group.containsKey(gb))
    			group.put(gb, 1);// 第一次添加
    		else
    			group.put(gb, 1+group.get(gb));// 加1
    		break;
    	case AVG:
    		if(!group.containsKey(gb)) {
    			group.put(gb, aValue);
    			// 新建一个数组
    			ArrayList<Integer> newGroup = new ArrayList();
    			newGroup.add(aValue);
    			avgCount.put(gb, newGroup);// 用来记录该组的值
    		}
    		else
    		{
    			ArrayList <Integer> get=avgCount.get(gb);
    			get.add(aValue);// 加入新的一行
    			avgCount.put(gb, get);
    			
    			int sum = get.stream().reduce(Integer::sum).orElse(0);// 求和
    			int avg=sum/get.size();// 求平均值
    			group.put(gb, avg);
    		}
    		break;
    	default:
    		throw new IllegalArgumentException("Aggregate wrong!");
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
        // some code goes here
    	
        return new IntAggIterator();
    }
    
    private class IntAggIterator implements OpIterator{
    	// 要返回tuple->tupleDesc
    	private TupleDesc td;
    	// 在group里面迭代
    	private	Iterator<HashMap.Entry<Field,Integer>> it;
    	
    	public IntAggIterator(){
    		// 构造tupleDesc->typeAr,fieldAr
    		if(gbField==Aggregator.NO_GROUPING)
    			td=new TupleDesc(new Type[] {Type.INT_TYPE},new String[] {""});
    		else 
    			td=new TupleDesc(new Type[] {gbFieldType,Type.INT_TYPE},new String []{"",""});
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
			//做复杂了，又把gbField包装回了field :(
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
