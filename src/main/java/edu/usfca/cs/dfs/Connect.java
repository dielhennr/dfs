package edu.usfca.cs.dfs;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

import edu.usfca.cs.dfs.net.MessagePipeline;
import java.net.InetAddress;
import java.net.UnknownHostException;

public class Connect {

	public EventLoopGroup workerGroup;
	public MessagePipeline pipeline;
	public Bootstrap bootstrap;
	public ChannelFuture cf;
	public Channel chan;
	public String host;
	
	public Connect(String host) {
		this.host = host;
	}
	
	public Channel connect() {
		
		EventLoopGroup workerGroup = new NioEventLoopGroup();
		MessagePipeline pipeline = new MessagePipeline();

		Bootstrap bootstrap = new Bootstrap()
								.group(workerGroup)
								.channel(NioSocketChannel.class)
								.option(ChannelOption.SO_KEEPALIVE, true)
								.handler(pipeline);

		ChannelFuture cf = bootstrap.connect(host, 4123);
		cf.syncUninterruptibly();
		
		Channel chan = cf.channel();
		
		return chan;
		
	}
	
	public void shutdown() {
		this.workerGroup.shutdownGracefully();
	}
	
}
