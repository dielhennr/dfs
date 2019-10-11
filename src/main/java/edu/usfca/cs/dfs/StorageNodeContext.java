package edu.usfca.cs.dfs;

import java.util.ArrayList;

public class StorageNodeContext {

	private BloomFilter filter;
	private String hostname;
	private long timestamp;
	private long freeSpace;
	private int requests;

    StorageNodeContext replicaAssignment1;
    StorageNodeContext replicaAssignment2;

	public StorageNodeContext(String hostname) {
		this.filter = new BloomFilter(100000, 3);
		this.timestamp = System.currentTimeMillis();
		this.freeSpace = 0;
		this.requests = 0;
		this.hostname = hostname;
        replicaAssignment1 = null;
        replicaAssignment2 = null;
    }

	public String getHostName() {
		return this.hostname;
	}

	public long getFreeSpace() {
		return freeSpace;
	}

	public int getRequests() {
		return this.requests;
	}

	public void setFreeSpace(long freeSpace) {
		this.freeSpace = freeSpace;
	}


	public boolean mightBeThere(byte[] data) {
			if (filter.get(data)) {
				return true;
			}
		
		return false;
	}

	public void bumpRequests() {
		this.requests++;
	}

	public void put(byte[] data) {
		this.filter.put(data);
	}

	public void updateTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	public long getTimestamp() {
		return this.timestamp;
	}
}
