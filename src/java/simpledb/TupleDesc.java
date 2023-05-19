package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

	private TDItem[] tdItems; //用来存放item的数组
	
    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        @Override
		public int hashCode() {
			return Objects.hash(fieldName, fieldType);
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			TDItem other = (TDItem) obj;
			return Objects.equals(fieldName, other.fieldName) && fieldType == other.fieldType;
		}

		/**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
    	//数组的iterator
        return (Iterator<TDItem>)Arrays.stream(tdItems).iterator();
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here
    	tdItems =new TDItem[typeAr.length];//构造数组对象
    	for(int i=0;i<typeAr.length;i++) {
    		//构造数组中每一个元素
    		tdItems[i]=new TDItem(typeAr[i],fieldAr[i]);
    	}
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
    	tdItems =new TDItem[typeAr.length];
    	for(int i=0;i<typeAr.length;i++) {
    		//匿名Item
    		tdItems[i]=new TDItem(typeAr[i],"");
    	}
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return tdItems.length;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
        if(i<0||i>tdItems.length) {
        	throw new NoSuchElementException("invalid");
        }
        return tdItems[i].fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
    	if(i<0||i>tdItems.length) {
        	throw new NoSuchElementException("invalid");
        }
        return tdItems[i].fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here
        for(int i=0;i<tdItems.length;i++)//遍历数组
        	if(tdItems[i].fieldName.equals(name))
        		return i;
        throw new  NoSuchElementException("not found");
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
    	int total=0;
       for(int i=0;i<tdItems.length;i++) {
    	   total+=tdItems[i].fieldType.getLen();
       }
       return total;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here
        //两个数组，用于构造新的TupleDesc
    	Type []typeAr=new Type[td1.numFields()+td2.numFields()];
        String []fieldAr=new String[td1.numFields()+td2.numFields()];
        //遍历td1，td2，填充两个数组
        for(int i=0;i<td1.numFields();i++) {
        	typeAr[i]=td1.tdItems[i].fieldType;
        	fieldAr[i]=td1.tdItems[i].fieldName;
        }
        for(int i=0;i<td2.numFields();i++) {
        	typeAr[i+td1.numFields()]=td2.tdItems[i].fieldType;
        	fieldAr[i+td1.numFields()]=td2.tdItems[i].fieldName;
        }
        //返回一个新构造的对象
        return new TupleDesc(typeAr,fieldAr);
    }

    @Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + Arrays.hashCode(tdItems);
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		TupleDesc other = (TupleDesc) obj;
		return Arrays.equals(tdItems, other.tdItems);
	}

//    public int hashCode() {
//        // If you want to use TupleDesc as keys for HashMap, implement this so
//        // that equal objects have equals hashCode() results
//        throw new UnsupportedOperationException("unimplemented");
//    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        StringBuilder temp=new StringBuilder();
        for(int i=0;i<tdItems.length-1;i++)
        	temp.append(tdItems[i].fieldType+"("+tdItems[i].fieldName+"),");
        temp.append(tdItems[tdItems.length-1].fieldType+"("+tdItems[tdItems.length-1].fieldName+")");//最后一项后面没有逗号
        return temp.toString();
    }
}
