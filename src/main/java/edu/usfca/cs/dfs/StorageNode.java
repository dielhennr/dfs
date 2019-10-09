package edu.usfca.cs.dfs;

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
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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

		StorageMessages.StorageMessageWrapper msgWrapper = StorageNode.buildJoinRequest(storageNode.getHostname());

		EventLoopGroup workerGroup = new NioEventLoopGroup();
		MessagePipeline pipeline = new MessagePipeline(storageNode);

		Bootstrap bootstrap = new Bootstrap().group(workerGroup).channel(NioSocketChannel.class)
				.option(ChannelOption.SO_KEEPALIVE, true).handler(pipeline);

		ChannelFuture cf = bootstrap.connect(storageNode.controllerHostName, 13100);
		cf.syncUninterruptibly();

		Channel chan = cf.channel();

		ChannelFuture write = chan.writeAndFlush(msgWrapper);

		if (write.syncUninterruptibly().isSuccess()) {
			logger.info("Sent join request to " + storageNode.controllerHostName);
		} else {
			logger.info("Failed join request to " + storageNode.controllerHostName);
			chan.close().syncUninterruptibly();
			workerGroup.shutdownGracefully();

			cf = bootstrap.connect(storageNode.controllerHostName, 13100);
			cf.syncUninterruptibly();

			chan = cf.channel();

			StorageMessages.StorageMessageWrapper replicaWrapper = StorageNode
					.buildReplicaRequest(storageNode.getHostname());

			write = chan.writeAndFlush(replicaWrapper);

			if (write.syncUninterruptibly().isSuccess()) {
				logger.info("Sent replica request " + storageNode.controllerHostName);
			} else {
				logger.info("Failed replica request to " + storageNode.controllerHostName);
				chan.close().syncUninterruptibly();
				workerGroup.shutdownGracefully();
			}

			System.exit(1);
		}

		chan.close().syncUninterruptibly();

		/*
		 * Have a thread start sending heartbeats to controller. Pass bootstrap to make
		 * connections
		 **/
		HeartBeatRunner heartBeat = new HeartBeatRunner(storageNode.getHostname(), storageNode.controllerHostName,
				bootstrap);
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
			ctx.pipeline().removeFirst();
			ctx.pipeline().addFirst(new LengthFieldBasedFrameDecoder(
					(int) message.getStoreRequest().getFileSize() + 1048576, 0, 4, 0, 4));

		} else if (message.hasStoreChunk()) {

			/* Write that shit to disk, i've hard coded my bigdata directory change that */
			String fileName = message.getStoreChunk().getFileName();

			Path directoryPath = Paths.get("/bigdata/rdielhenn", fileName);
			// message.getStoreChunk().getFileName() + "_chunk" +
			// message.getStoreChunk().getChunkId());

			if (!Files.exists(directoryPath)) {
				try {
					Files.createDirectory(directoryPath);
				} catch (IOException e) {
					logger.info("Problem creating path: " + directoryPath);
				}
				System.out.println("Directory created: " + directoryPath);
			}

			Path path = Paths.get("/bigdata/rdielhenn/", fileName,
					message.getStoreChunk().getFileName() + "_chunk" + message.getStoreChunk().getChunkId());

			System.out.println("Path is: " + path);
			try {
				Files.write(path, message.getStoreChunk().getData().toByteArray());
				if (!filePaths.contains(path)) {
					filePaths.add(path);
				}

			} catch (IOException ioe) {

				logger.info("Could not write file");
			}
		}
	}

	/**
	 * Build a join request protobuf with hostname/ip
	 *
	 * @param hostname
	 * @param ip
	 * @return the protobuf
	 */
	public static StorageMessages.StorageMessageWrapper buildJoinRequest(String hostname) {
		/* Store hostname in a JoinRequest protobuf */
		StorageMessages.JoinRequest joinRequest = StorageMessages.JoinRequest.newBuilder().setNodeName(hostname)
				.build();

		/* Wrapper */
		StorageMessages.StorageMessageWrapper msgWrapper = StorageMessages.StorageMessageWrapper.newBuilder()
				.setJoinRequest(joinRequest).build();

		return msgWrapper;
	}

	public static StorageMessages.StorageMessageWrapper buildReplicaRequest(String hostname) {
		StorageMessages.ReplicaRequest replicaRequest = StorageMessages.ReplicaRequest.newBuilder()
				.setHostname(hostname).build();

		StorageMessages.StorageMessageWrapper msgWrapper = StorageMessages.StorageMessageWrapper.newBuilder()
				.setReplicaRequest(replicaRequest).build();
		return msgWrapper;
	}

	/**
	 * Build a heartbeat protobuf
	 *
	 * @param hostname
	 * @param freeSpace
	 * @return the protobuf
	 */
	public static StorageMessages.StorageMessageWrapper buildHeartBeat(String hostname, long freeSpace) {

		StorageMessages.Heartbeat heartbeat = StorageMessages.Heartbeat.newBuilder().setFreeSpace(freeSpace)
				.setHostname(hostname).setTimestamp(System.currentTimeMillis()).build();

		StorageMessages.StorageMessageWrapper msgWrapper = StorageMessages.StorageMessageWrapper.newBuilder()
				.setHeartbeat(heartbeat).build();

		return msgWrapper;
	}
}
