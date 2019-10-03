package edu.usfca.cs.dfs;

import java.util.ArrayList;
import io.netty.channel.ChannelHandlerContext;

public class StorageNodeContext {

    private ChannelHandlerContext ctx;
    private ArrayList<BloomFilter> filters;
    private long timestamp;
    private long freeSpace;

    public StorageNodeContext(ChannelHandlerContext ctx) {
        this.ctx = ctx;
        this.filters = new ArrayList<BloomFilter>();
        this.filters.add(new BloomFilter(100000, 3));
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

  public void addFilter(BloomFilter filter) {
    this.filters.add(filter);
  }

	public boolean mightBeThere(byte[] data) {
    for (BloomFilter filter : filters) {
      if (filter.get(data)) {
        return true;
      }
    }
    return false;
	}
	
	public void put(byte[] data) {
		this.filters.get(0).put(data);
	}
    
	public void updateTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}
	public long getTimestamp() {
		return this.timestamp;
	}
}
