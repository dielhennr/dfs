package edu.usfca.cs.dfs;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import edu.usfca.cs.dfs.StorageMessages.StorageMessageWrapper;
import edu.usfca.cs.dfs.net.MessagePipeline;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.protobuf.ByteString;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

public class Client implements DFSNode {
	private static final Logger logger = LogManager.getLogger(Client.class);

	public Client() {
	}

	public static void main(String[] args) throws IOException {

		ArgumentMap arguments = new ArgumentMap(args);
		String controllerHost = arguments.getString("-h");
		Integer port = arguments.getInteger("-p", 13100);
		/* Default Chunk size to 16kb */
		Integer chunkSize = arguments.getInteger("-c", 16384);

		EventLoopGroup workerGroup = new NioEventLoopGroup();
		Client client = new Client();
		MessagePipeline pipeline = new MessagePipeline(client);

		Bootstrap bootstrap = new Bootstrap()
				.group(workerGroup)
				.channel(NioSocketChannel.class)
				.option(ChannelOption.SO_KEEPALIVE, true)
				.handler(pipeline);

		ChannelFuture cf = bootstrap.connect(controllerHost, port);
		cf.syncUninterruptibly();
		
		
		File file = new File(arguments.getString("-f"));

		StorageMessages.StoreRequest storeRequest = StorageMessages.StoreRequest.newBuilder().setFileName(file.getName())
				.setFileSize(file.getTotalSpace()).build();

		StorageMessages.StorageMessageWrapper msgWrapper = StorageMessages.StorageMessageWrapper.newBuilder()
				.setStoreRequest(storeRequest).build();

		Channel chan = cf.channel();
		ChannelFuture write = chan.writeAndFlush(msgWrapper);
		
		write.syncUninterruptibly();
		
		/*At this point we should get a response from controller telling us where to put this file*/
		
		/* Get number of chunks */
		long length = file.length();
		int chunks = (int) (length / chunkSize);
		
		/* Asynch writes and input stream */
        List<ChannelFuture> writes = new ArrayList<>();
        FileInputStream inputStream = new FileInputStream(file);
        
        byte[] messageBytes = new byte[chunkSize];
        /* Write a protobuf to the channel for each chunk */
        for (int i = 0; i < chunks; i++) {
        	messageBytes = inputStream.readNBytes(chunkSize);
			StorageMessages.StoreChunk storeChunk = StorageMessages.StoreChunk.newBuilder()
					.setFileName(file.getName())
					.setChunkId(i)
					.setData(ByteString.copyFrom(messageBytes))
					.build();
            writes.add(chan.write(storeChunk));
        }
        
		/* We will add one extra chunk for and leftover bytes */
		int leftover = (int) (length % chunkSize);
	
		/* If we have leftover bytes */
		if (leftover != 0) {
			/* Read them and write the protobuf */
			byte[] last = new byte[leftover];
			last = inputStream.readNBytes(leftover);
			StorageMessages.StoreChunk storeChunk = StorageMessages.StoreChunk.newBuilder()
					.setFileName(file.getName())
					.setChunkId(chunks)
					.setData(ByteString.copyFrom(last))
					.build();
			writes.add(chan.write(storeChunk));
		}
        
        chan.flush();
        
        for (ChannelFuture writeChunk : writes) {
            writeChunk.syncUninterruptibly();
        }
        
        inputStream.close();
        chan.close().syncUninterruptibly();

		/* Don't quit until we've disconnected: */
		System.out.println("Shutting down");
		workerGroup.shutdownGracefully();
	}

	@Override
	public void onMessage(ChannelHandlerContext ctx, StorageMessageWrapper message) {
		logger.info("Recieved permission to put file on " + message.getStoreResponse().getHostname());
	}

	public StorageMessages.StorageMessageWrapper buildStoreRequest(String filename, long fileSize) {

		StorageMessages.StoreRequest storeRequest = StorageMessages.StoreRequest.newBuilder().setFileName(filename)
				.setFileSize(fileSize).build();
		StorageMessages.StorageMessageWrapper msgWrapper = StorageMessages.StorageMessageWrapper.newBuilder()
				.setStoreRequest(storeRequest).build();

		return msgWrapper;
	}

}
