package edu.usfca.cs.dfs;

import io.netty.channel.ChannelHandlerContext;

public class StorageNodeContext {

    private ChannelHandlerContext ctx;
    private BloomFilter filter;
    private long timestamp;
    private long freeSpace;

    public StorageNodeContext(ChannelHandlerContext ctx) {
        this.ctx = ctx;
        this.filter = new BloomFilter(10000, 3);
        this.timestamp = System.currentTimeMillis();
        this.freeSpace = 0;
    }

	public ChannelHandlerContext getCtx() {
		return ctx;
	}
	
	public long getFreeSpace() {
		return freeSpace;
	}

	public void setFreeSpace(long freeSpace) {
		this.freeSpace = freeSpace;
	}

	public boolean mightBeThere(byte[] data) {
		return this.filter.get(data);
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
