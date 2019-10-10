package edu.usfca.cs.dfs;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;

import com.google.protobuf.ByteString;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.usfca.cs.dfs.StorageMessages.StorageMessageWrapper;
import edu.usfca.cs.dfs.net.MessagePipeline;
import edu.usfca.cs.dfs.net.ServerMessageRouter;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;

public class StorageNode implements DFSNode {

	private static final Logger logger = LogManager.getLogger(StorageNode.class);

	ServerMessageRouter messageRouter;

	private InetAddress ip;

	private String hostname;

	String controllerHostName;

	ArgumentMap arguments;

	ArrayList<Path> filePaths;

	ArrayList<String> replicaHosts;

	HashMap<String, ArrayList<Path>> nodeFileMap;
    static Bootstrap bootstrap;

	public StorageNode(String[] args) throws UnknownHostException {

		filePaths = new ArrayList<>();
		nodeFileMap = new HashMap<>();
		replicaHosts = new ArrayList<>();

		ip = InetAddress.getLocalHost();
		hostname = ip.getHostName();
		arguments = new ArgumentMap(args);
		if (arguments.hasFlag("-h")) {
			controllerHostName = arguments.getString("-h");
		} else {
			System.err.println("Usage: java -cp ..... -h controllerhostname");
			System.exit(1);
		}

		/* For log4j2 */
		System.setProperty("hostName", hostname);
		EventLoopGroup workerGroup = new NioEventLoopGroup();
		MessagePipeline pipeline = new MessagePipeline(this);

		bootstrap = new Bootstrap().group(workerGroup).channel(NioSocketChannel.class)
				.option(ChannelOption.SO_KEEPALIVE, true).handler(pipeline);
	};

	private String getHostname() {
		return hostname;
	}

	public static void main(String[] args) throws IOException {
		StorageNode storageNode = null;
		try {
			storageNode = new StorageNode(args);
		} catch (UnknownHostException e) {
			logger.error("Could not start storage node.");
			System.exit(1);
		}

        
		ChannelFuture cf = bootstrap.connect(storageNode.controllerHostName, 13100);
		cf.syncUninterruptibly();

		Channel chan = cf.channel();

		StorageMessages.StorageMessageWrapper msgWrapper = Builders.buildJoinRequest(storageNode.getHostname());

		ChannelFuture write = chan.writeAndFlush(msgWrapper);

		if (write.syncUninterruptibly().isSuccess()) {
			logger.info("Sent join request to " + storageNode.controllerHostName);
		} else {
			logger.info("Failed join request to " + storageNode.controllerHostName);
		}

		/*
		 * Have a thread start sending heartbeats to controller. Pass bootstrap to make
		 * connections
		 **/
		HeartBeatRunner heartBeat = new HeartBeatRunner(storageNode.getHostname(), storageNode.controllerHostName, bootstrap);
		Thread heartThread = new Thread(heartBeat);
		heartThread.start();
		logger.info("Started heartbeat thread.");
		storageNode.start();
	}

	/**
	 * Start listening for requests and inbound chunks
	 *
	 * @throws IOException
	 */
	public void start() throws IOException {
		/* Pass a reference of the controller to our message router */
		messageRouter = new ServerMessageRouter(this);
		messageRouter.listen(13111);
		System.out.println("Listening for connections on port 13100");
	}

	/* Storage node inbound duties */
	@Override
	public void onMessage(ChannelHandlerContext ctx, StorageMessageWrapper message) {
		if (message.hasStoreRequest()) {
			/**
			 * If we get a store request we need to change our decoder to fit the chunk size
			 */
			logger.info("Request to store " + message.getStoreRequest().getFileName() + " size: "
					+ message.getStoreRequest().getFileSize());
            logger.info("Replica Assignment 1: " + message.getStoreRequest().getReplicaAssignments().getReplica1());
            logger.info("Replica Assignment 2: " + message.getStoreRequest().getReplicaAssignments().getReplica2());
            replicaHosts.add(message.getStoreRequest().getReplicaAssignments().getReplica1());
            replicaHosts.add(message.getStoreRequest().getReplicaAssignments().getReplica2());
			ctx.pipeline().removeFirst();
			ctx.pipeline().addFirst(new LengthFieldBasedFrameDecoder(
					(int) message.getStoreRequest().getFileSize() + 1048576, 0, 4, 0, 4));

        } else if (message.hasStoreChunk()) {
            if (!message.getStoreChunk().getOriginHost().equals(this.hostname)) {
                logger.info("Recieved replica of " + message.getStoreChunk().getFileName() + " for " + message.getStoreChunk().getOriginHost());
            }
			/* Write that shit to disk, i've hard coded my bigdata directory change that */
			String fileName = message.getStoreChunk().getFileName();

			Path directoryPath = Paths.get("/bigdata/rdielhenn", message.getStoreChunk().getOriginHost(),fileName);

			if (!Files.exists(directoryPath)) {
				try {
					Files.createDirectory(directoryPath);
				} catch (IOException e) {
					logger.info("Problem creating path: " + directoryPath);
				}
				System.out.println("Directory created: " + directoryPath);
			}
            
            ByteString bytes = message.getStoreChunk().getData();

            try {
                /** 
                 * Store chunks in users specified home directory 
                 *
                 * Home -
                 *      file_chunks -
                 *          chunk0#AD12341FFC
                 *          chunk1#AD12341111
                 *          chunk2#12341FFC11
                 *
                 *  file_chunks will be named the name of the file whose chunks we are storing
                 *  we will also append a chunks chunkid, #, and the sha1sum of the given
                 *  file's bytes.
                 *
                 *  This will allow us to verify to correctness of the data on retrieval 
                 */
                Path path = Paths.get(directoryPath.toString(),
                        message.getStoreChunk().getFileName() 
                        + "_chunk" 
                        + message.getStoreChunk().getChunkId() 
                        + "#" 
                        + Checksum.SHAsum(bytes.toByteArray()));
				Files.write(path, message.getStoreChunk().getData().toByteArray());
                if (replicaHosts.size() == 2) {
                    ChannelFuture cf = bootstrap.connect(replicaHosts.get(0), 13111);
                    cf.syncUninterruptibly();
                    Channel chan = cf.channel();
                    chan.writeAndFlush(message).syncUninterruptibly();
                }
				if (!filePaths.contains(path)) {
					filePaths.add(path);
				}

			} catch (IOException | NoSuchAlgorithmException ioe) {

				logger.info("Could not write file");
			}
		}
	}
}
