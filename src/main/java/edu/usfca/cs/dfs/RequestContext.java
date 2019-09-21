package edu.usfca.cs.dfs;

import io.netty.channel.ChannelHandlerContext;

public class RequestContext {


    private StorageMessages.StorageMessageWrapper request;
    private ChannelHandlerContext ctx;

    public RequestContext(ChannelHandlerContext ctx, StorageMessages.StorageMessageWrapper joinRequest) {
        this.ctx = ctx;
        this.request = joinRequest;
    }

    public StorageMessages.StorageMessageWrapper getRequest() {
        return request;
    }

    public ChannelHandlerContext getCtx() {
        return ctx;
    }
}
