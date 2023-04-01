package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */

/**
 * 每个table对应一个HeapFile
 * HeapFile由一系列page组成，每个page又是固定量的tuple
 * 每个page是由一些列的slots组成，每个slot对应一个tuple
 * page都是HeapPage
 * 每个page都有一个header，header记录tuple是否有效 bits
 * 
 * @author 唐同学
 */
public class HeapFile implements DbFile {

	private File table;
	private TupleDesc tupleDesc;
	
    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
    	this.table=f;
    	this.tupleDesc=td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return table;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {//每个DbFile都有一个key->hashMap
        // some code goes here
        return table.getAbsoluteFile().hashCode();//就是page的tableId!
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        //构造一个page需要pageId和data[]
    	byte []data=new byte[BufferPool.getPageSize()];
    	int tableId=pid.getTableId();
    	int pageNo=pid.getPageNumber();
    	HeapPageId hpid=new HeapPageId(tableId,pageNo);
    	
    	//page在文件中有偏移量 random access
    	try {
			RandomAccessFile raf=new RandomAccessFile(table,"r");
			//做exercise6出现IndexOutOfBoudsException?
			//read函数偏移量是byte数组的偏移量！！前面初始化为空
			//不是数据流的偏移量，数据流得用seek！
			
			int offset=pageNo*BufferPool.getPageSize();//Number应该是从0开始？
			raf.seek(offset);
			int testRead=raf.read(data,0,BufferPool.getPageSize());
			
			if(testRead!=BufferPool.getPageSize())//没读对
				throw new IllegalArgumentException("page wrong!");
			
			raf.close();
			return new HeapPage(hpid,data);
			
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
    	throw new IllegalArgumentException("page does not exist in this file");
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
    	// 获取pageNumber
    	byte []data=new byte[BufferPool.getPageSize()];
    	int pageNo=page.getId().getPageNumber();
    	if(pageNo>numPages()) {
    		throw new IllegalArgumentException("page wrong！");
    	}
    	// 写入file
    	RandomAccessFile raf=new RandomAccessFile(table,"rw");
    	int offset=pageNo*BufferPool.getPageSize();
		raf.seek(offset);
		
		data=page.getPageData();
    	raf.write(data);
    	raf.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int) Math.floor((double)table.length()/BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    // This method will acquire a lock on the affected pages of the file, 
    // and may block until the lock can be acquired.
    // @return An ArrayList contain the pages that were modified
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
    	ArrayList<Page> ret=new ArrayList<Page>();
        // 查看file里面所有的page,寻找一个合适的插入tuple
    	for(int i=0;i<numPages();i++) {
    		// 用bufferPool获取page
    		// we should be able to add 504 tuples on an empty page. 报错↓
    		//HeapPageId pid=(HeapPageId) t.getRecordId().getPageId();// tuple插入后才有recordId！recordId就是用来记录这个的！
    		HeapPageId pid =new HeapPageId(this.getId(),i);
    		HeapPage page=(HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
    		if(page.getNumEmptySlots()!=0){// 该页有空位可以插入
    			page.insertTuple(t);
    			ret.add(page);
    			return ret;// 也别break了，直接return吧
    		}
    	}
    	// 报错->the next 512 additions should live on a new page
    	// 在file里追加一页
    	RandomAccessFile raf=new RandomAccessFile(table,"rw");
    	int offset=numPages()*BufferPool.getPageSize();// 从尾部追加
		raf.seek(offset);
		byte[] emptyPageData=HeapPage.createEmptyPageData();
    	raf.write(emptyPageData);
    	raf.close();
    	// 拿出新的一页做插入
    	HeapPageId pid=new HeapPageId(this.getId(),numPages()-1);
    	HeapPage page=(HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
    	page.insertTuple(t);
    	ret.add(page);
    	
    	return ret;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
    	ArrayList<Page> ret=new ArrayList<Page>();
        // 从页中删除tuple
    	HeapPageId pid=(HeapPageId) t.getRecordId().getPageId();//根据插入时的recordId做删除
		HeapPage page=(HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
    	page.deleteTuple(t);
    	ret.add(page);
		return ret;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
    	//iterate through through the tuples of each page in the HeapFile
    	//Do not load the entire table into memory
    	//用BufferPool.getPage()去获取pages
    	
    	//auxiliary class:(
    	
        // some code goes here
        return new HeapFileIterator(tid,this);
    }
    
    private class HeapFileIterator implements DbFileIterator{
    	//在一个文件里，一次读取一页，一页里面用Iterator<Tuple>，每次是返回一个tuple
    	private int cursor;
    	private TransactionId tid;
    	private HeapFile file;
    	private Iterator<Tuple> it;
    	
    	
		public HeapFileIterator(TransactionId tid, HeapFile file) {
			super();
			this.tid = tid;
			this.file = file;
		}

		@Override
		public void open() throws DbException, TransactionAbortedException {
			// TODO Auto-generated method stub
			cursor=0;
			HeapPageId temp=new HeapPageId(file.getId(),cursor);//tableId有，pageNo有
			it=((HeapPage)Database.getBufferPool().getPage(tid, temp, Permissions.READ_ONLY)).iterator();
		}

		@Override
		public boolean hasNext() throws DbException, TransactionAbortedException {
			// TODO Auto-generated method stub
			if(it==null)
				return false;//没有open
			if(!it.hasNext()) {//这一页的tuple已经访问完了
				if(cursor+1>=file.numPages())
					return false;
				else {
					cursor++;//下一页
					HeapPageId temp=new HeapPageId(file.getId(),cursor);
					it=((HeapPage)Database.getBufferPool().getPage(tid, temp, Permissions.READ_ONLY)).iterator();
					return it.hasNext();// deleteTest的时候报错，NoSuchElement 找到这来，确实需要再判断新开的一页是否还有next
				}
			}
			else 
				return true;
		}

		@Override
		public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
			// TODO Auto-generated method stub
			if(hasNext()) {
				return it.next();
			}
			throw new NoSuchElementException("no");//test中catch的是这个
			
		}

		@Override
		public void rewind() throws DbException, TransactionAbortedException {
			//Resets the iterator to the start.
			// TODO Auto-generated method stub
			cursor=0;
			HeapPageId temp=new HeapPageId(file.getId(),cursor);
			it=((HeapPage)Database.getBufferPool().getPage(tid, temp, Permissions.READ_ONLY)).iterator();
			
		}

		@Override
		public void close() {
			// TODO Auto-generated method stub
			cursor=0;
			it=null;
		}
    	
    }

}

