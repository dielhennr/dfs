package edu.usfca.cs.dfs;

import io.netty.channel.ChannelHandlerContext;

public class JoinContext {


    private StorageMessages.JoinRequest joinRequest;
    private ChannelHandlerContext ctx;

    public JoinContext(ChannelHandlerContext ctx, StorageMessages.JoinRequest joinRequest) {
        this.ctx = ctx;
        this.joinRequest = joinRequest;
    }

    public StorageMessages.JoinRequest getJoinRequest() {
        return joinRequest;
    }

    public ChannelHandlerContext getCtx() {
        return ctx;
    }
}
