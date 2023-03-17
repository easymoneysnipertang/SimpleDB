package simpledb;

import java.util.*;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements OpIterator {
	//从指定table中的page中读取all of the tuples
	
    private static final long serialVersionUID = 1L;

    private TransactionId tid;
    private int tableId;
    private String tableAlias;//别名，可以为空
    //Iterator
//    private int cursor;
//    private Iterator<Tuple> it;
    
    //access tuples through the DbFile.iterator() method!
    //非叶节点，上层迭代器！
    //只需要拿回一个文件迭代器就行，其余的交给下面做！
    DbFileIterator itrator;
    
    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     *
     * @param tid
     *            The transaction this scan is running as a part of.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
    	//对一个表顺序扫描
        // some code goes here
    	this.tid=tid;
    	this.tableId=tableid;
    	if(tableAlias==null||tableAlias=="")
    		this.tableAlias="null";
    	else 
    		this.tableAlias=tableAlias;
//    	cursor=-1;
//    	it=null;
    }

    /**
     * @return
     *       return the table name of the table the operator scans. This should
     *       be the actual name of the table in the catalog of the database
     * */
    public String getTableName() {
        return Database.getCatalog().getTableName(tableId);
    }

    /**
     * @return Return the alias of the table this operator scans.
     * */
    public String getAlias()
    {
        // some code goes here
        return tableAlias;
    }

    /**
     * Reset the tableid, and tableAlias of this operator.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public void reset(int tableid, String tableAlias) {
        // some code goes here
    	if(tableAlias==null||tableAlias=="")
    		this.tableAlias="null";
    	else 
    		this.tableAlias=tableAlias;
    	this.tableId=tableid;
    }

    public SeqScan(TransactionId tid, int tableId) {
        this(tid, tableId, Database.getCatalog().getTableName(tableId));
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
//    	cursor=0;
//   	HeapPageId temp=new HeapPageId(tableId,cursor);//读table的cursor页
    	itrator=Database.getCatalog().getDatabaseFile(tableId).iterator(tid);//DbFile.iterator()
    	itrator.open();
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.  The alias and name should be separated with a "." character
     * (e.g., "alias.fieldName").
     *
     * @return the TupleDesc with field names from the underlying HeapFile,
     *         prefixed with the tableAlias string from the constructor.
     */
    public TupleDesc getTupleDesc() {//还需加前缀
    	//HeapFile里有TupleDesc
    	//TupleDesc需要typeAr和fieldAr去构造
    	TupleDesc temp=Database.getCatalog().getTupleDesc(tableId);
    	Type[] typeAr=new Type[temp.numFields()];
    	String[] fieldAr=new String[temp.numFields()]; 
    	for(int i=0;i<temp.numFields();i++) {
    		//typeAr直接得
    		typeAr[i]=temp.getFieldType(i);
    		//fieldAr得判断field是不是null，tableAlias已经预处理
    		StringBuilder toPrefix=new StringBuilder(this.tableAlias+".");
    		if(temp.getFieldName(i)==null||temp.getFieldName(i)=="")
    			toPrefix.append("null");
    		else
    			toPrefix.append(temp.getFieldName(i));
    		fieldAr[i]=toPrefix.toString();
    	}
        return new TupleDesc(typeAr,fieldAr);
    }

    public boolean hasNext() throws TransactionAbortedException, DbException {
        // some code goes here
    	if(itrator==null)
    		throw new IllegalStateException("unopen");
    	return itrator.hasNext();
    }

    public Tuple next() throws NoSuchElementException,
            TransactionAbortedException, DbException {
    	if(itrator==null)
    		throw new IllegalStateException("unopen");
        if(!hasNext())
        	throw new NoSuchElementException("no more");
        return itrator.next();
    }

    public void close() {
        // some code goes here
    	itrator.close();
    	itrator=null;
    	
    }

    public void rewind() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
    	if(itrator==null)
    		throw new IllegalStateException("unopen");
    	itrator.rewind();
    	
    }
}
