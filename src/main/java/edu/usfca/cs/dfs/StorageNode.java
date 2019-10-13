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
import java.util.List;
import java.util.TreeSet;

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

public class StorageNode implements DFSNode {

	private static final Logger logger = LogManager.getLogger(StorageNode.class);

	ServerMessageRouter messageRouter;

	private InetAddress ip;

	String hostname;

	String controllerHostName;

	Path rootDirectory;

	ArgumentMap arguments;

	ArrayList<String> replicaHosts;

	static Bootstrap bootstrap;
	
	HashMap<String, TreeSet<ChunkWrapper>> chunkMap;   // Mapping filenames to the chunks
	HashMap<String, ArrayList<Path>> hostnameToChunks;	   // Mapping hostnames to Paths of chunks

	public StorageNode(String[] args) throws UnknownHostException {
		replicaHosts = new ArrayList<>();
		chunkMap = new HashMap<>();
		hostnameToChunks = new HashMap<>();
		ip = InetAddress.getLocalHost();
		hostname = ip.getHostName();
		arguments = new ArgumentMap(args);
		if (arguments.hasFlag("-h") && arguments.hasFlag("-r")) {
			rootDirectory = arguments.getPath("-r");
			controllerHostName = arguments.getString("-h");
		} else {
			System.err.println("Usage: java -cp ..... -h controllerhostname -r rootDirectory");
			System.exit(1);
		}

		/* For log4j2 */
		System.setProperty("hostName", hostname);
		EventLoopGroup workerGroup = new NioEventLoopGroup();
		MessagePipeline pipeline = new MessagePipeline(this);

		bootstrap = new Bootstrap().group(workerGroup).channel(NioSocketChannel.class)
				.option(ChannelOption.SO_KEEPALIVE, true).handler(pipeline);
	}

	public static void main(String[] args) throws IOException {
		StorageNode storageNode = null;
		try {
			storageNode = new StorageNode(args);
		} catch (UnknownHostException e) {
			logger.error("Could not start storage node.");
			System.exit(1);
		}

		ChannelFuture cf = bootstrap.connect(storageNode.controllerHostName, 13112);
		cf.syncUninterruptibly();

		Channel chan = cf.channel();

		StorageMessages.StorageMessageWrapper msgWrapper = Builders.buildJoinRequest(storageNode.hostname);

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
		HeartBeatRunner heartBeat = new HeartBeatRunner(storageNode.hostname, storageNode.controllerHostName,
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
		messageRouter.listen(13114);
		System.out.println("Listening for connections on port 13100");
	}

	/* Storage node inbound duties */
	@Override
	public void onMessage(ChannelHandlerContext ctx, StorageMessageWrapper message) {
		if (message.hasStoreRequest()) {
			
			/* Only accept first replica assignments for now */
			if (replicaHosts.isEmpty()) {
				replicaHosts.add(message.getStoreRequest().getReplicaAssignments().getReplica1());
				replicaHosts.add(message.getStoreRequest().getReplicaAssignments().getReplica2());
				logger.info("Replica Assignment 1: " + message.getStoreRequest().getReplicaAssignments().getReplica1());
				logger.info("Replica Assignment 2: " + message.getStoreRequest().getReplicaAssignments().getReplica2());
			} else {
				logger.info("Rejecting Assignment");
			}

			logger.info("Request to store " + message.getStoreRequest().getFileName() + " size: "
					+ message.getStoreRequest().getFileSize());
		} else if (message.hasStoreChunk()) {
			/* Write that shit to disk */
			hostnameToChunks.putIfAbsent(message.getStoreChunk().getOriginHost(), new ArrayList<Path>());
            chunkMap.putIfAbsent(message.getStoreChunk().getFileName(), new TreeSet<ChunkWrapper>(new ChunkWrapperComparator()));
			this.writeChunk(message);
			/* Send to replica assignments */
			if (replicaHosts.size() == 2 && message.getStoreChunk().getOriginHost().equals(this.hostname)) {
				/* Connect to first assignment and send chunk */
				ChannelFuture cf = bootstrap.connect(replicaHosts.get(0), 13114);
				cf.syncUninterruptibly();
				Channel chan = cf.channel();
				chan.writeAndFlush(message).syncUninterruptibly();
				cf.syncUninterruptibly();
				chan.close().syncUninterruptibly();

				/* Connect to second assignment and send chunk */
				cf = bootstrap.connect(replicaHosts.get(1), 13114);
				cf.syncUninterruptibly();
				chan = cf.channel();
				chan.writeAndFlush(message).syncUninterruptibly();
				cf.syncUninterruptibly();
				chan.close().syncUninterruptibly();
			}
		} else if (message.hasRetrieveFile()) {
            logger.info("Attempting to shoot chunks of " + message.getRetrieveFile().getFileName());
			Path filePath = Paths.get(rootDirectory.toString(), message.getRetrieveFile().getFileName());
			if (Files.exists(filePath)) {
				this.shootChunks(ctx, filePath);
			}
		}
	}

	/**
	 * Find all chunks // tokenize them check metadata // heal if neccessary // send
	 * chunks to client
	 *
	 * @param ctx      {@link ChannelHandlerContext}
	 * @param filePath {@link ChunkFinder} Produce a list of all chunks from root
	 *                 file directory
	 */
	public void shootChunks(ChannelHandlerContext ctx, Path filePath) {

	    /* Get chunks of the file requested */	
		TreeSet<ChunkWrapper> chunks = chunkMap.get(filePath.getFileName().toString());
        logger.info("File chunks path: " + filePath);
		/* Write the chunks back to client */
		if (chunks != null) {
            System.out.println("Chunk paths");
            chunks.stream().forEach(chunk -> System.out.println(chunk.getPath().getFileName().toString()));

			List<ChannelFuture> writes = new ArrayList<>();
			for (ChunkWrapper chunk : chunks) {
				Path chunkPath = chunk.getPath();
				ByteString data = null;
				String checksumCheck = null;

				/* Read the chunk and compute it's checksum */
				try {
					data = ByteString.copyFrom(Files.readAllBytes(chunkPath));
					checksumCheck = Checksum.SHAsum(data.toByteArray());
				} catch (IOException | NoSuchAlgorithmException ioe) {
					logger.info("Could not read chunk to send to client");
				}

				/*
				 * If the reads and checksum computation was succesful, write the chunk to
				 * client
				 */
				if (data != null && checksumCheck != null) {
					String[] fileTokens = chunkPath.getFileName().toString().split("#");
					String checksum = fileTokens[fileTokens.length - 1];

					logger.info("Checksum " + checksum);
					logger.info("Pathname " + chunkPath.toString());
					/* If checksums don't match send request to replica assignment for healing */
					if (!checksum.equals(checksumCheck)) {
						logger.info("Chunk " + chunkPath.toString() + "needs healing");
					} else {
						String filename = fileTokens[0];

						/* Build the chunks and write it to client */
						StorageMessages.StorageMessageWrapper chunkToSend = Builders.buildStoreChunk(filename,
								this.hostname, chunk.getChunkID(), chunk.getTotalChunks(), data);
						ChannelFuture write = ctx.pipeline().writeAndFlush(chunkToSend);
						writes.add(write);
					}

				}

			} 

			for (ChannelFuture write : writes) {
				write.syncUninterruptibly();
			}
            
		} else {
            logger.info("Could not find chunks");
        }
        ctx.channel().close();

	}

	/**
	 * Writes a StoreChunk to disk and appends its checksum to the filename The
	 * chunk will be stored under the root/home directory entered in the command
	 * line on storage node startup This is done with the -r flag
	 *
	 * Home - file_chunks - chunk0#AD12341FFC... chunk1#AD12341111...
	 * chunk2#12341FFC11...
	 *
	 * file_chunks will be named the name of the file whose chunks we are storing we
	 * will also append a chunks chunkid, #, and the sha1sum of the given file's
	 * bytes.
	 *
	 * This will allow us to verify to correctness of the data on retrieval
	 *
	 * @param message
	 */
	public void writeChunk(StorageMessages.StorageMessageWrapper message) {
		/*
		 * If this chunk is not being stored on its primary node, log the replication
		 * happening
		 */
		if (!message.getStoreChunk().getOriginHost().equals(this.hostname)
				&& message.getStoreChunk().getChunkId() == 0) {
			logger.info("Recieved replica of " + message.getStoreChunk().getFileName() + " for "
					+ message.getStoreChunk().getOriginHost());
		}

		String fileName = message.getStoreChunk().getFileName();

		Path chunkPath = Paths.get(rootDirectory.toString(), fileName);

		/*
		 * If we haven't stored a chunk of this file yet create a directory for the
		 * chunks to go into
		 */
		if (!Files.exists(chunkPath)) {
			try {
				Files.createDirectory(chunkPath);
			} catch (IOException e) {
				logger.info("Problem creating path: " + chunkPath);
			}
			logger.info("Created path: " + chunkPath);
		}

		try {

			byte[] data = message.getStoreChunk().getData().toByteArray();

			/**
			 * Store chunks in users specified home directory
			 */
            String checksum = Checksum.SHAsum(data);
			Path path = Paths.get(chunkPath.toString(),
					message.getStoreChunk().getFileName() + "_chunk" + message.getStoreChunk().getChunkId()							+ "#" + checksum);
            /* Add this path to the mapping of hosts to thier paths, we will use this to handle node failures and re-replication */
            synchronized(hostnameToChunks) {
                hostnameToChunks.get(message.getStoreChunk().getOriginHost()).add(path);
            }

            /* Add this chunk to it's files set */
            synchronized(chunkMap) {
                chunkMap.get(fileName).add(new ChunkWrapper(path, message.getStoreChunk().getChunkId()
                        , message.getStoreChunk().getTotalChunks(), checksum));
            }

			/* Write chunk to disk */
			Files.write(path, data);
		} catch (IOException | NoSuchAlgorithmException ioe) {
			logger.info("Could not write file/sha1sum not computed properly");
		}

	}
}
