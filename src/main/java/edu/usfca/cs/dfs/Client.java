package edu.usfca.cs.dfs;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import com.google.protobuf.ByteString;

import edu.usfca.cs.dfs.StorageMessages.StorageMessageWrapper;
import edu.usfca.cs.dfs.net.MessagePipeline;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

public class Client implements DFSNode {

	/* Command Line Arguments */
	ArgumentMap arguments;

	/* Client logger */
	private static final Logger logger = LogManager.getLogger(Client.class);

	File file;

	/* Controller's hostname */
	String controllerHost;

	/* Port to connect through */
	Integer port;

	/* Chunk Size */
	Integer chunkSize;

    Bootstrap bootstrap;

	public Client(String[] args) {
		/* Command Line Arguments */
		this.arguments = new ArgumentMap(args);
		
		if (arguments.hasFlag("-f") && arguments.hasFlag("-h")) {
			file = new File(arguments.getString("-f"));
			controllerHost = arguments.getString("-h");
		} else {
			System.err.println("Usage: java -cp .... -h hostToContact -f fileToSend.\n"
					+ "-p port and -c <chunksize(int)>  are optional flags.");
			System.exit(1);
		}

		/* Default to port 13100 */
		port = arguments.getInteger("-p", 13100);

		/* Default Chunk size to 16kb */
		this.chunkSize = arguments.getInteger("-c", 16384);
	}

    private void addBootstrap(Bootstrap bootstrap) {
        this.bootstrap = bootstrap;
    }

	public static void main(String[] args) throws IOException {

		/* Create this node for interfacing in the pipeline */

		Client client = new Client(args);
		EventLoopGroup workerGroup = new NioEventLoopGroup();
		MessagePipeline pipeline = new MessagePipeline(client, client.chunkSize);

		Bootstrap bootstrap = new Bootstrap().group(workerGroup).channel(NioSocketChannel.class)
				.option(ChannelOption.SO_KEEPALIVE, true).handler(pipeline);

        client.addBootstrap(bootstrap);

		ChannelFuture cf = bootstrap.connect(client.controllerHost, client.port);
		cf.syncUninterruptibly();

		StorageMessages.StorageMessageWrapper msgWrapper = Client.buildStoreRequest(client.file.getName(),
				client.file.length());

		Channel chan = cf.channel();
		ChannelFuture write = chan.writeAndFlush(msgWrapper);
		write.syncUninterruptibly();
		if(cf.syncUninterruptibly().isSuccess()) {
			System.err.println("Synced on channel after writing to controller");
		}

		/* Don't quit until we've disconnected: */
		System.out.println("Shutting down");
	}

	@Override
	public void onMessage(ChannelHandlerContext ctx, StorageMessageWrapper message) {
		if (message.hasStoreResponse()) {
			logger.info("Recieved permission to put file on " + message.getStoreResponse().getHostname());
            StorageMessages.StorageMessageWrapper msgWrapper = Client.buildStoreRequest(this.file.getName(),
                    this.file.length());
            /*
            * At this point we should get a response from controller telling us where to
            * put this file.
            * 
            */
            ChannelFuture cf = bootstrap.connect(message.getStoreResponse().getHostname(), 13111).syncUninterruptibly();

            ChannelFuture write = cf.channel().writeAndFlush(msgWrapper).syncUninterruptibly();

            if (write.isSuccess() && write.isDone()) {
                logger.info("Sent store request to node " + message.getStoreResponse().getHostname());
            } 

            cf = bootstrap.connect(message.getStoreResponse().getHostname(), 13111).syncUninterruptibly();

            Path path = arguments.getPath("-f");

            try {
                ByteString data; 
                data = ByteString.copyFrom(Files.readAllBytes(path));
                StorageMessages.StoreChunk storeChunk = StorageMessages.StoreChunk.newBuilder().setChunkId(0).setFileName(path.getFileName().toString()).setData(data).build();
                StorageMessages.StorageMessageWrapper wrapper = StorageMessages.StorageMessageWrapper.newBuilder().setStoreChunk(storeChunk).build();
                ChannelFuture writeChunk = cf.channel().writeAndFlush(wrapper).syncUninterruptibly();

            } catch (IOException ioe) {
                logger.info("could not read file");
            }



            cf.channel().close().syncUninterruptibly();
        }



	}

    /**
     * Builds a store request protobuf
     *
     * @param filename
     * @param fileSize
     * @return
     */
	private static StorageMessages.StorageMessageWrapper buildStoreRequest(String filename, long fileSize) {

		StorageMessages.StoreRequest storeRequest = StorageMessages.StoreRequest
                                                    .newBuilder()
                                                    .setFileName(filename)
				                                    .setFileSize(fileSize)
                                                    .build();
		StorageMessages.StorageMessageWrapper msgWrapper = StorageMessages
                                                           .StorageMessageWrapper
                                                           .newBuilder()
				                                           .setStoreRequest(storeRequest)
                                                           .build();

		return msgWrapper;
	}

}
