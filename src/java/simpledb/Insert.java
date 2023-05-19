package simpledb;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;
    
    private OpIterator child;
    private TransactionId tid;
    private int tableId;
    private TupleDesc td;
    private boolean isCalled;

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
        // some code goes here
    	this.tid=t;
    	this.child=child;
    	this.tableId=tableId;
    	if(!child.getTupleDesc().equals(Database.getCatalog().getDatabaseFile(tableId).getTupleDesc())) {
    		throw new DbException("tupleDesc does not match!");
    	}
    	this.td=new TupleDesc(new Type[] {Type.INT_TYPE},new String [] {""});
    	isCalled=false;
    }

    public TupleDesc getTupleDesc() {//td是返回的tuple的td？
        // some code goes here
        return td;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
    	child.open();
    	super.open();
    }

    public void close() {
        // some code goes here
    	super.close();
    	//isCalled=false;// 你也别给我反复插了
    	child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
    	child.rewind();
    	//isCalled=false;
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
        // some code goes here
    	// insert DOES NOT need check to see if a particular tuple is a duplicate before inserting it.
    	// 报错-> return null if called more than once
    	if(isCalled)
    		return null;
    	
    	isCalled=true;
    	int count=0;
        while(child.hasNext()){
        	Tuple t=child.next();
        	try {
				Database.getBufferPool().insertTuple(tid, tableId, t);
				count++;
			} catch (DbException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (TransactionAbortedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        }
        Tuple ret=new Tuple(td);
        ret.setField(0, new IntField(count));
        
        return ret;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[] {child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
    	child=children[0];
    }
}
