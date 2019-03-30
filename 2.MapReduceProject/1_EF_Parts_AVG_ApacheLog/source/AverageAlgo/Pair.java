package bAverageAlgo;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class Pair implements Writable {

	
    private Integer key;
    private Integer value;
    
    

    public Pair() {
    	
	}

	public Pair(Integer key, Integer value) {
        this.key = key;
        this.value = value;
    }

    public Integer getKey() {
        return key;
    }

    public void setKey(Integer key) {
        this.key = key;
    }

    public Integer getValue() {
        return value;
    }

    public void setValue(Integer value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return new StringBuilder().append("[ ").append(key).append(" , ").append(value).append(" ]").toString();
    }

	@Override
	public void readFields(DataInput in) throws IOException {
		 key = in.readInt();
	    value = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt((int) key);
	    out.writeInt((int) value);		
	}

}