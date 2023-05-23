package simpledb;

import java.io.IOException;
import java.util.HashSet;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public class PageLockManager {
	// 用hashMap记录有锁的页
	/** 
	 * ConcurrentHashMap可以做到读取数据不加锁，
	 *并且其内部的结构可以让其在进行写操作的时候能够将锁的粒度保持地尽量地小，
	 *允许多个修改操作并发进行，其关键在于使用了锁分段技术。 
	*/
	ConcurrentHashMap<PageId,ConcurrentHashMap<TransactionId,PageLock>> lockedPages;
	ConcurrentHashMap<TransactionId, Set<TransactionId>> dependencyGraph;
	public PageLockManager() {
		lockedPages=new ConcurrentHashMap<>();
		//dependencyGraph=new ConcurrentHashMap<>();
	}
	
	private synchronized void addDependency(TransactionId tid1,TransactionId tid2) {
		if(tid1==tid2)return;
		Set<TransactionId> list=dependencyGraph.get(tid1);
		if(list==null||list.size()==0) {
			// 之前没有出度
			list=new HashSet<>();
		}
		list.add(tid2);
		dependencyGraph.put(tid1, list);
	}
	private synchronized void removeDependency(TransactionId tid) {
		dependencyGraph.remove(tid);
	}
	public synchronized boolean isExistCycle(TransactionId tid) {
		// DAG->拓扑排序
		// 从tid开始是否存在环
		Queue<TransactionId> que=new ConcurrentLinkedQueue<>();
		que.add(tid);
		Set<TransactionId> out=new HashSet<>();// 入度不为0的节点
		
		while(que.size()>0) {// bfs->队列非空
			TransactionId toRemove=que.poll();
			if(out.contains(toRemove))continue;
			// 用out记录下来从tid能搜索到的节点
			out.add(toRemove);
			Set<TransactionId> nowSet=dependencyGraph.get(toRemove);
			if(nowSet==null)continue;
			
			for(TransactionId nowId:nowSet) {// bfs
				que.add(nowId);
			}
		}
		
		ConcurrentHashMap<TransactionId,Integer> inDegree=new ConcurrentHashMap<>();
		for(TransactionId nowId:out) {// 初始化入度
			inDegree.put(nowId, 0);
		}
		for(TransactionId nowId:out) {
			// 计算每个节点的入度
			Set<TransactionId> outNodes=dependencyGraph.get(nowId);
			if(outNodes==null)continue;
			for(TransactionId outId:outNodes) {
				Integer temp=inDegree.get(outId);
				inDegree.put(outId, temp+1);
			}
		}
		
		while(true) {
			int count=0;
			for(TransactionId nowId:out) {
				if(inDegree.get(nowId)==null)continue;
				if(inDegree.get(nowId)==0) {// 入度为0
					Set<TransactionId> outNodes=dependencyGraph.get(nowId);
					// 移除该节点，后续节点入度减一
					if(outNodes==null)continue;
					for(TransactionId outId:outNodes) {
						Integer temp=inDegree.get(outId);
						inDegree.put(outId, temp-1);
					}
					inDegree.remove(nowId);
					count++;
				}
			}
			if(count==0) break;// 没有入度为0的节点了
		}
		
		if(inDegree.size()==0)return false;// 无环
		else return true;
	}
	// 当死锁产生，将自己以外的tid中止掉
	public synchronized void abortOthers(TransactionId tid) {
		Set<TransactionId> waiting=dependencyGraph.get(tid);
		if(waiting==null)return;
		for(TransactionId waitId:waiting) {
			try {
				Database.getBufferPool().transactionComplete(waitId, false);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	
	// getPage(TransactionId tid, PageId pid, Permissions perm)
	public synchronized boolean acquireLock(PageId pid,TransactionId tid,int lockType) {
		// 判断当前页是否有锁
		if(!lockedPages.containsKey(pid)){
			PageLock lock=new PageLock(tid,lockType);
			// keep track of which locks each transaction holds
			ConcurrentHashMap<TransactionId,PageLock> pageLocks=new ConcurrentHashMap<>();
			pageLocks.put(tid, lock);
			// 一页上面可以同时被多个事务加锁
			lockedPages.put(pid, pageLocks);
//			// 无依赖(只要往里放了锁，就解除依赖)
//			removeDependency(tid);
			return true;
		}
		
		// 当前页上有锁
		ConcurrentHashMap<TransactionId,PageLock> pageLocks=lockedPages.get(pid);
		if(!pageLocks.containsKey(tid)) {// 当前事务未对page上锁
			if(lockType==PageLock.EXCLUSIVE)// 已经有锁，无法再上exclusive
			{
//				for(TransactionId temp:pageLocks.keySet()) {
//					addDependency(tid,temp);// 有依赖(放不进去肯定有依赖)
//				}
				return false;
			}
			
			// 看当前page上是否有exclusive
			if(pageLocks.size()>1) {// 多于一把锁，肯定是shared
				PageLock lock=new PageLock(tid,lockType);
				pageLocks.put(tid, lock);
				lockedPages.put(pid, pageLocks);
				// 无依赖
//				removeDependency(tid);
				return true;
			}
			else {// 就一把锁
				PageLock theOne = null;
				for(PageLock temp:pageLocks.values())
					theOne=temp;
				if(theOne.getType()==PageLock.EXCLUSIVE)// 如果是exclusive，没法
				{
//					addDependency(tid,theOne.getTid());// 有依赖
					return false;
				}
				else {
					PageLock lock=new PageLock(tid,lockType);
					pageLocks.put(tid, lock);
					lockedPages.put(pid, pageLocks);
//					removeDependency(tid);// 无依赖
					return true;
				}
			}
		}
		
		else {// 当前事务对page持有锁
			PageLock lock=pageLocks.get(tid);
			if(lock.getType()==PageLock.SHARED) {// 原来上面有把读锁
				if(lockType==PageLock.SHARED)// 新请求还是读锁
					return true;// 没有依赖，且不用加锁
				else {// 新请求是写锁
					// If transaction t is the only transaction holding 
					// a shared lock on an object o, t may upgrade its
					// lock on o to an exclusive lock.
					if(pageLocks.size()==1) {
						lock.setType(PageLock.EXCLUSIVE);
						pageLocks.put(tid, lock);
//						removeDependency(tid);// 消除依赖
						return true;
					}
					else {// 不止一个事务，其他事务对它还有读锁
//						for(TransactionId temp:pageLocks.keySet()) {
//							addDependency(tid,temp);// 有依赖(放不进去肯定有依赖)
//						}
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
