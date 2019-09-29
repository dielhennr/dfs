package edu.usfca.cs.dfs;

import io.netty.channel.ChannelHandlerContext;

public class StorageNodeContext {

	private String hostname;
    private ChannelHandlerContext ctx;
    private BloomFilter filter;
    private long timestamp;

    public StorageNodeContext(ChannelHandlerContext ctx, String hostname) {
        this.ctx = ctx;
        this.hostname = hostname;
        this.filter = new BloomFilter(10000, 3);
        this.timestamp = 0;
    }

	public String getHostname() {
		return hostname;
	}

	public ChannelHandlerContext getCtx() {
		return ctx;
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
