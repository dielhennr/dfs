package edu.usfca.cs.dfs;

import com.google.protobuf.ByteString;
import edu.usfca.cs.dfs.StorageMessages.StorageMessageWrapper;
import edu.usfca.cs.dfs.net.MessagePipeline;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Client implements DFSNode {

	/* Command Line Arguments */
	ArgumentMap arguments;

	/* Client logger */
	private static final Logger logger = LogManager.getLogger(Client.class);

	/* File to send or query for */
	Path path;

	/* Controller's hostname */
	String controllerHost;

	/* Port to connect through */
	Integer port;

	/* Chunk Size */
	Integer chunkSize;

	/* Clients bootstrap for connecting to controller and storeagenodes */
	Bootstrap bootstrap;

	EventLoopGroup workerGroup;

	/**
	 * Constructs a client given the command line arguments
	 *
	 * @param args
	 */
	public Client(String[] args) {
		/* Command Line Arguments */
		this.arguments = new ArgumentMap(args);

		/* Check that user entered controller to connect to and file to send */
		if (arguments.hasFlag("-h")) {

			controllerHost = arguments.getString("-h");
		} else {
			System.err.println("Usage: java -cp .... -h hostToContact -f fileToSend.\n"
					+ "-p port and -c <chunksize(int)>  are optional flags.");
			System.exit(1);
		}
		if (arguments.hasFlag("-r")) {
			path = arguments.getPath("-r");
		}
		if (arguments.hasFlag("-f")) {
			path = arguments.getPath("-f");
		}

		/* Default to port 13100 */
		port = arguments.getInteger("-p", 13100);

		/* Default Chunk size to 16kb */
		this.chunkSize = arguments.getInteger("-c", 16384);

		workerGroup = new NioEventLoopGroup();
		MessagePipeline pipeline = new MessagePipeline(this);

		bootstrap = new Bootstrap().group(workerGroup).channel(NioSocketChannel.class)
				.option(ChannelOption.SO_KEEPALIVE, true).handler(pipeline);
	}

	public static void main(String[] args) throws IOException {

		/* Create this node for interfacing in the pipeline */

		Client client = new Client(args);

		ChannelFuture cf = client.bootstrap.connect(client.controllerHost, client.port);
		cf.syncUninterruptibly();
        
		StorageMessages.StorageMessageWrapper msgWrapper = null;
		if (client.arguments.hasFlag("-f")) {
			msgWrapper = Builders
				.buildStoreRequest(client.path.getFileName().toString(), client.chunkSize, "", "");
		}
		else if (client.arguments.hasFlag("-r")) {
			msgWrapper = Builders.buildRetrievalRequest(client.path.getFileName().toString());
		}
		Channel chan = cf.channel();
		ChannelFuture write = chan.writeAndFlush(msgWrapper);
		write.syncUninterruptibly();
		if (cf.syncUninterruptibly().isSuccess()) {
			System.err.println("Synced on channel after writing to controller");
		}

		/* Don't quit until we've disconnected: */
		System.out.println("Shutting down");
	}

	/** Client's inbound duties */
	@Override
	public void onMessage(ChannelHandlerContext ctx, StorageMessageWrapper message) {
		/*
		 * At this point we should get a response from controller telling us where to
		 * put this file.
		 */
		if (message.hasStoreResponse()) {
			logger.info("Recieved permission to put file on " + message.getStoreResponse().getHostname());
            String replica1 = message.getStoreResponse().getReplicaAssignments().getReplica1();
            String replica2 = message.getStoreResponse().getReplicaAssignments().getReplica2();
			logger.info("Replicating to " + replica1 + " and " + replica2);
			/*
			 * Build a store request to send to the storagenode so that it can change it's
			 * decoder
			 */
			StorageMessages.StorageMessageWrapper storeRequest = Builders
					.buildStoreRequest(
                                        message.getStoreResponse().getFileName(), 
                                        this.chunkSize, 
                                        replica1,
                                        replica2);

			logger.info("Sending chunks in size " + this.chunkSize + " to " + message.getStoreResponse().getHostname());

			/**
			 * Connect to StorageNode and start sending chunks
			 *
			 * <p>
			 * Not sure if these ports should be hard coded thats something you could change
			 * too Storeagenodes connect to controller through a port but i dont think
			 * client can send chunks through that port if its being used to listen. That's
			 * why I hard coded client to connect to a SN through 13111 and the SNs to
			 * listen on 13111. Meanwhile controller is listening on 13000 for heartbeats
			 * from SNs HeartBeatRunner.
			 */
			ChannelFuture cf = bootstrap.connect(message.getStoreResponse().getHostname(), 13111).syncUninterruptibly();

			/*
			 * Write the request to change the SNs decoder so that it can start receiving
			 * chunks
			 */
			ChannelFuture write = cf.channel().writeAndFlush(storeRequest).syncUninterruptibly();

			if (write.isSuccess() && write.isDone()) {
				logger.info("Sent store request to node " + message.getStoreResponse().getHostname());
			}

			/* Get number of chunks */
			long length = path.toFile().length();
			int chunks = (int) (length / this.chunkSize);

			/* We will add one extra chunk for leftover bytes */
			int leftover = (int) (length % this.chunkSize);

			/* Asynch writes and input stream */
			List<ChannelFuture> writes = new ArrayList<>();

			try (InputStream inputStream = Files.newInputStream(this.path)) {
				/* Write a protobuf to the channel for each chunk */
				byte[] data = new byte[this.chunkSize];
				for (int i = 0; i < chunks; i++) {
					data = inputStream.readNBytes(this.chunkSize);
					StorageMessageWrapper chunk = Builders.buildStoreChunk(path.getFileName().toString(), message.getStoreResponse().getHostname(), i,
							ByteString.copyFrom(data));
					writes.add(cf.channel().write(chunk));
				}

				/* If we have leftover bytes */
				data = new byte[leftover];
				if (leftover != 0) {
					data = inputStream.readNBytes(leftover);
					/* Read them and write the protobuf */
					StorageMessageWrapper chunk = Builders.buildStoreChunk(path.getFileName().toString(), message.getStoreResponse().getHostname(), chunks,
							ByteString.copyFrom(data));
					writes.add(cf.channel().write(chunk));
				}

				/* Flush the channel */
				cf.channel().flush();

				/* Sync on each write, their priority doesn't really matter */
				for (ChannelFuture writeChunk : writes) {
					writeChunk.syncUninterruptibly();
				}
				/*
				 * At this point all writes are done. Close the input stream and sync on the
				 * channelfuture
				 */
				inputStream.close();
				cf.syncUninterruptibly();
			} catch (IOException ioe) {
				logger.info("Could not read file " + this.path.getFileName().toString());
			}
			/* Close the channel */
			cf.channel().close().syncUninterruptibly();
			/* Shutdown the workerGroup */
			this.workerGroup.shutdownGracefully();
		}
		else if (message.hasRetrievalHosts()) {
			String[] possibleHosts = message.getRetrievalHosts().getHosts().split(" ");
			String fileName = message.getRetrievalHosts().getFileName();
			
			logger.info("Possible Hosts for file " + fileName + " ---> " + possibleHosts);
			
			for (String host : possibleHosts) {
				// Open connections for nodes and check if they have the file
			}
			
		}
	}

}
