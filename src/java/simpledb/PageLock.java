package simpledb;

import java.util.Objects;

// add by tang 2023.5.16

public class PageLock {
	// You will need to implement shared and exclusive locks
	public static final int SHARED=1;
	public static final int EXCLUSIVE=0;
	
	private TransactionId tid;
	private int type;
	
	public PageLock(TransactionId tid, int type) {
		super();
		this.tid = tid;
		this.type = type;
	}
	public TransactionId getTid() {
		return tid;
	}
	public void setTid(TransactionId tid) {
		this.tid = tid;
	}
	public int getType() {
		return type;
	}
	public void setType(int type) {
		this.type = type;
	}
	@Override
	public int hashCode() {
		return Objects.hash(tid, type);
	}
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		PageLock other = (PageLock) obj;
		return Objects.equals(tid, other.tid) && type == other.type;
	}
	
}
