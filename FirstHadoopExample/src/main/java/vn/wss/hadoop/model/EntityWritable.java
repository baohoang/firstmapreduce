package vn.wss.hadoop.model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.mahout.math.Varint;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;

public class EntityWritable implements WritableComparable<EntityWritable>,
		Cloneable {

	private long itemId;
	private long itemIdSimilar;
	private int numOfSimilar;
	private int numOfUsers;
	private int numOfItemSimilarUsers;

	// private int flag;

	public EntityWritable() {
	}

	public EntityWritable(long itemId, long itemIdSimilar, int numOfSimilar,
			int numOfUsers, int numOfItemSimilarUsers) {
		this.itemId = itemId;
		this.itemIdSimilar = itemIdSimilar;
		this.numOfSimilar = numOfSimilar;
		this.numOfUsers = numOfUsers;
		this.numOfItemSimilarUsers = numOfItemSimilarUsers;
		// this.flag = flag;
	}

	public EntityWritable(long itemId, long itemIdSimilar, int numOfSimilar) {
		this.itemId = itemId;
		this.itemIdSimilar = itemIdSimilar;
		this.numOfSimilar = numOfSimilar;
		// this.flag = flag;
	}

	public EntityWritable(long id, int count, int flag) {
		this.itemId = id;
		this.numOfUsers = count;
		// this.flag = flag;
	}

	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		Varint.writeSignedVarLong(itemId, out);
		Varint.writeSignedVarLong(itemIdSimilar, out);
		Varint.writeSignedVarInt(numOfSimilar, out);
		Varint.writeSignedVarInt(numOfUsers, out);
		Varint.writeSignedVarInt(numOfItemSimilarUsers, out);
		// Varint.writeSignedVarInt(flag, out);
	}

	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		itemId = Varint.readSignedVarLong(in);
		itemIdSimilar = Varint.readSignedVarLong(in);
		numOfSimilar = Varint.readSignedVarInt(in);
		numOfUsers = Varint.readSignedVarInt(in);
		numOfItemSimilarUsers = Varint.readSignedVarInt(in);
		// flag = Varint.readSignedVarInt(in);
	}

	public int compareTo(EntityWritable o) {
		// TODO Auto-generated method stub
		int r1 = compareLong(itemId, o.getItemId());
		if (r1 == 0) {
			int r2 = compareLong(itemIdSimilar, o.getItemIdSimilar());
			if (r2 == 0) {
				int r3 = compateInt(numOfSimilar, o.getNumOfSimilar());
				if (r3 == 0) {
					int r4 = compateInt(numOfItemSimilarUsers,
							o.getNumOfItemSimilarUsers());
					if (r4 == 0) {
						int r5 = compateInt(numOfUsers, o.getNumOfUsers());
						return r5;
					}
					return r4;
				}
				return r3;
			}
			return r2;
		}
		return r1;
	}

	private static int compareLong(long a, long b) {
		return a < b ? -1 : a > b ? 1 : 0;
	}

	private static int compateInt(int a, int b) {
		return a < b ? -1 : a > b ? 1 : 0;
	}

	@Override
	protected Object clone() throws CloneNotSupportedException {
		// TODO Auto-generated method stub
		return new EntityWritable(itemId, itemIdSimilar, numOfSimilar,
				numOfUsers, numOfItemSimilarUsers);
	}

	@Override
	public boolean equals(Object obj) {
		// TODO Auto-generated method stub
		if (obj instanceof EntityWritable) {
			EntityWritable e = (EntityWritable) obj;
			return itemId == e.getItemId()
					&& itemIdSimilar == e.getItemIdSimilar()
					&& numOfItemSimilarUsers == e.getNumOfItemSimilarUsers()
					&& e.getNumOfSimilar() == numOfSimilar
					&& e.getNumOfUsers() == numOfUsers;
		}
		return false;
	}

	@Override
	public int hashCode() {
		// TODO Auto-generated method stub
		return Longs.hashCode(itemId) + Longs.hashCode(itemIdSimilar)
				+ Ints.hashCode(numOfItemSimilarUsers)
				+ Ints.hashCode(numOfSimilar) + Ints.hashCode(numOfUsers);
	}

	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return super.toString();
	}

	public long getItemId() {
		return itemId;
	}

	public void setItemId(long itemId) {
		this.itemId = itemId;
	}

	public long getItemIdSimilar() {
		return itemIdSimilar;
	}

	public void setItemIdSimilar(long itemIdSimilar) {
		this.itemIdSimilar = itemIdSimilar;
	}

	public int getNumOfSimilar() {
		return numOfSimilar;
	}

	public void setNumOfSimilar(int numOfSimilar) {
		this.numOfSimilar = numOfSimilar;
	}

	public int getNumOfUsers() {
		return numOfUsers;
	}

	public void setNumOfUsers(int numOfUsers) {
		this.numOfUsers = numOfUsers;
	}

	public int getNumOfItemSimilarUsers() {
		return numOfItemSimilarUsers;
	}

	public void setNumOfItemSimilarUsers(int numOfItemSimilarUsers) {
		this.numOfItemSimilarUsers = numOfItemSimilarUsers;
	}

	public static EntityWritable fusion(EntityWritable i, EntityWritable ii) {
		EntityWritable res = new EntityWritable();
		res.setItemId(Math.max(i.getItemId(), ii.getItemId()));
		res.setItemIdSimilar(Math.max(i.getItemIdSimilar(),
				ii.getItemIdSimilar()));
		res.setNumOfItemSimilarUsers(Math.max(i.getNumOfItemSimilarUsers(),
				ii.getNumOfItemSimilarUsers()));
		res.setNumOfSimilar(Math.max(i.getNumOfSimilar(), ii.getNumOfSimilar()));
		res.setNumOfUsers(Math.max(i.getNumOfUsers(), ii.getNumOfUsers()));
		return res;
	}
}
