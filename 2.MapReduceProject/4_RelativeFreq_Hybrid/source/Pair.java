package dPart4;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class Pair implements Writable, WritableComparable<Pair> {

	private String key;
	private String value;

	public Pair() {

	}

	public Pair(String key, String value) {
		this.key = key;
		this.value = value;
	}

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	@Override
	public String toString() {
		return new StringBuilder().append("(").append(key).append(", ")
				.append(value).append(")").toString();
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		key = in.readUTF();
		value = in.readUTF();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(key);
		out.writeUTF(value);
	}

	@Override
	public int compareTo(Pair o) {
		int k = (key.compareTo(o.key));
		if (k != 0)
			return k;
		return value.compareTo(o.value);
	}
}
