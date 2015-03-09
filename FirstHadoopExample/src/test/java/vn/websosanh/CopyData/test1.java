package vn.websosanh.CopyData;

import static org.junit.Assert.*;

import java.util.ArrayList;

import org.apache.cassandra.cli.CliParser.newColumnFamily_return;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.junit.Test;

public class test1 {

	@Test
	public void test() {
		// fail("Not yet implemented");
		ArrayList<Long> arr=new ArrayList<Long>();
		arr.add((long) 1);
		arr.add((long) 1);
		arr.add((long) 1);
		arr.add((long) 1);
		Vector vector=new RandomAccessSparseVector(arr.size());
		
	}

}
