package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;
    private OpIterator child;
    private TransactionId tid;
    private TupleDesc td;
    private boolean isCalled;
    
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
        // some code goes here
    	this.tid=t;
    	this.child=child;
    	this.td=new TupleDesc(new Type[] {Type.INT_TYPE},new String [] {"count"});
    	isCalled=false;
    }

    public TupleDesc getTupleDesc() {
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
    	//isCalled=false;// 插入可以重复插入，删除不能重复删
    	child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
    	child.rewind();
    	//isCalled=false;
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
        // some code goes here
    	// 与插入一样的报错
    	if(isCalled)
    		return null;
    	
    	isCalled=true;
        int count=0;
        while(child.hasNext()) {
        	Tuple t=child.next();
        	try {
				Database.getBufferPool().deleteTuple(tid, t);
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
