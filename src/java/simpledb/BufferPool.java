package simpledb;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */

/**
 * 一个table由多个page组成，
 * 每个page存储其中的一部分数据。
 * 当我们需要查询table中的数据时，
 * 数据库会在所有的page中查找符合条件的数据。
 * 
 * @author 唐同学
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;
    private final int numPages;
    private HashMap<Integer,Page> pages;//存放bufferPool中的page，key用pageId的hashCode()
    private LinkedList<PageId> pageOrder;
    // 锁管理器
    PageLockManager lockManager;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
    	this.numPages=numPages;
    	pages=new HashMap<Integer,Page>();
    	pageOrder=new LinkedList<PageId>();
    	lockManager=new PageLockManager();
    }
    
    public static int getPageSize() {
      return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        // some code goes here
    	// 先判断是什么锁
    	int lockType=perm==Permissions.READ_ONLY?PageLock.SHARED:PageLock.EXCLUSIVE;
    	boolean isAcquired=false;
    	// 循环获取锁
    	while(!isAcquired) {// 忙等待
    		isAcquired=lockManager.accuireLock(pid, tid, lockType);
    	}
    	
    	//page有自己独有的id(hashCode),page所属的table也有id(getTableId)
    	if(!pages.containsKey(pid.hashCode())) {//查询的page不在bufferPool中
    		if(pages.size()>=numPages) {// insufficient space
    			evictPage();
    		}
    		//从文件中读取page，用dbFile
    		//读取文件，catalog.getDatabaseFile()
    		DbFile temp=Database.getCatalog().getDatabaseFile(pid.getTableId());
    		Page page=temp.readPage(pid);
    		
    		//读入bufferPool
    		pages.put(pid.hashCode(), page);
    		pageOrder.add(pid);
    	}
        return pages.get(pid.hashCode());//lock? insufficient? not necessary for lab1?
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
    	lockManager.releaseLock(pid, tid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    	lockManager.completeTransaction(tid);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return lockManager.isHoldLock(p, tid);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // 先获取HeapFile
    	DbFile f=Database.getCatalog().getDatabaseFile(tableId);
    	// 插入tuple
    	ArrayList<Page> p=f.insertTuple(tid, t);
    	// makeDirty
    	for(Page page:p) {
    		page.markDirty(true, tid);
    		// 写报告时经提醒修改，插入删除要将它放入cache，需要判断空间
    		// 那有意义吗？f里面调用getPage不是已经将他放进缓存了吗
    		// 有可能并发处理？虽然它在缓存中，但过程中可能缓存又放入了page？
    		if(pages.size()>=numPages) {// insufficient space
    			evictPage();
    		}
    		pages.put(page.getId().hashCode(), page);// update
    		
    		pageOrder.remove(page.getId());// 最近进行了调用，LRU原则对他进行更新
    		pageOrder.add(page.getId());
    	}
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
    	// 找到table
        DbFile f=Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
        // 报错-> delete 504 tuples from the first page-> 重写了tuple的equals函数
        ArrayList<Page> p=f.deleteTuple(tid, t);
        for(Page page:p) {
        	page.markDirty(true, tid);
        	if(pages.size()>=numPages) {// insufficient space
    			evictPage();
    		}
        	pages.put(page.getId().hashCode(), page);
        	
        	pageOrder.remove(page.getId());// 最近进行了调用，LRU原则对它进行更新
    		pageOrder.add(page.getId());
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
    	for(Page p:pages.values()) {// 调用flushPage去做
    		flushPage(p.getId());
    	}
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
    	pages.remove(pid.hashCode());
    	pageOrder.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
    	Page p=pages.get(pid.hashCode());
    	// 判断该页是否为脏
    	TransactionId tid=p.isDirty();
    	if(tid!=null) {
    		// 把该页写入磁盘->找到file
    		Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(p);
    		p.markDirty(false, null);// 标记为不再脏
    	}
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
    	// cache-> LRU原则
    	PageId pid=pageOrder.getFirst();
    	try {
			flushPage(pid);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	discardPage(pid);
    }

}
