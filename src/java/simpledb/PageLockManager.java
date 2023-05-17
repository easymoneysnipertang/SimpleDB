package simpledb;

import java.util.concurrent.ConcurrentHashMap;

public class PageLockManager {
	// 用hashMap记录有锁的页
	/** 
	 * ConcurrentHashMap可以做到读取数据不加锁，
	 *并且其内部的结构可以让其在进行写操作的时候能够将锁的粒度保持地尽量地小，
	 *允许多个修改操作并发进行，其关键在于使用了锁分段技术。 
	*/
	ConcurrentHashMap<PageId,ConcurrentHashMap<TransactionId,PageLock>> lockedPages=new ConcurrentHashMap<>();
	
	// getPage(TransactionId tid, PageId pid, Permissions perm)
	public synchronized boolean accuireLock(PageId pid,TransactionId tid,int lockType) {
		// 判断当前页是否有锁
		if(!lockedPages.containsKey(pid)){
			PageLock lock=new PageLock(tid,lockType);
			// keep track of which locks each transaction holds
			ConcurrentHashMap<TransactionId,PageLock> pageLocks=new ConcurrentHashMap<>();
			pageLocks.put(tid, lock);
			// 一页上面可以同时被多个事务加锁
			lockedPages.put(pid, pageLocks);
			return true;
		}
		
		// 当前页上有锁
		ConcurrentHashMap<TransactionId,PageLock> pageLocks=lockedPages.get(pid);
		if(!pageLocks.containsKey(tid)) {// 当前事务未对page上锁
			if(lockType==PageLock.EXCLUSIVE)// 已经有锁，无法再上exclusive
				return false;
			// 看当前page上是否有exclusive
			if(pageLocks.size()>1) {// 多于一把锁，肯定是shared
				PageLock lock=new PageLock(tid,lockType);
				pageLocks.put(tid, lock);
				lockedPages.put(pid, pageLocks);
				return true;
			}
			else {// 就一把锁
				PageLock theOne = null;
				for(PageLock temp:pageLocks.values())
					theOne=temp;
				if(theOne.getType()==PageLock.EXCLUSIVE)// 如果是exclusive，没法
					return false;
				else {
					PageLock lock=new PageLock(tid,lockType);
					pageLocks.put(tid, lock);
					lockedPages.put(pid, pageLocks);
					return true;
				}
			}
		}
		
		else {// 当前事务对page持有锁
			PageLock lock=pageLocks.get(tid);
			if(lock.getType()==PageLock.SHARED) {// 原来上面有把读锁
				if(lockType==PageLock.SHARED)// 新请求还是读锁
					return true;
				else {// 新请求是写锁
					// If transaction t is the only transaction holding 
					// a shared lock on an object o, t may upgrade its
					// lock on o to an exclusive lock.
					if(pageLocks.size()==1) {
						lock.setType(PageLock.EXCLUSIVE);
						pageLocks.put(tid, lock);
						return true;
					}
					else {// 不止一个事务，其他事务对它还有读锁
						return false;// 不能改成写锁
					}
				}
			}
			else// 原来就有一把写锁，肯定行
				return true;
		}
	}

	// releasePage(TransactionId tid, PageId pid)
	public synchronized boolean releaseLock(PageId pid,TransactionId tid) {
//		if(lockedPages.containsKey(pid)) {
//			ConcurrentHashMap<TransactionId,PageLock> pageLocks=lockedPages.get(pid);
//			if(pageLocks.containsKey(tid)) {
//				pageLocks.remove(tid);// 释放当前事务对page的锁
//				if(pageLocks.size()==0)
//					lockedPages.remove(pid);// 当前页上已没有事务对其有锁
//				return true;
//			}
//		}
		if(isHoldLock(pid,tid)) {
			ConcurrentHashMap<TransactionId,PageLock> pageLocks=lockedPages.get(pid);
			pageLocks.remove(tid);// 释放当前事务对page的锁
			if(pageLocks.size()==0)
				lockedPages.remove(pid);// 当前页上已没有事务对其有锁
			return true;
		}
		
		return false;
	}
	
	// holdsLock(TransactionId tid, PageId p)
	public synchronized boolean isHoldLock(PageId pid,TransactionId tid) {
		if(lockedPages.containsKey(pid)) {
			ConcurrentHashMap<TransactionId,PageLock> pageLocks=lockedPages.get(pid);
			if(pageLocks.containsKey(tid)) 
				return true;
		}
		return false;
	}
	
	// transactionComplete(TransactionId tid)
	public synchronized void releaseAllLocks(TransactionId tid) {
		// 将tid锁定的页面全部释放
		for(PageId pid:lockedPages.keySet())
			releaseLock(pid,tid);
	}
	
}
