package edu.usfca.cs.dfs;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

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

	public StorageNode(String[] args) throws UnknownHostException {
		replicaHosts = new ArrayList<>();

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
        
		ChannelFuture cf = bootstrap.connect(storageNode.controllerHostName, 13100);
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
		HeartBeatRunner heartBeat = new HeartBeatRunner(storageNode.hostname, storageNode.controllerHostName, bootstrap);
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

            /* Write that shit to disk*/
            this.writeChunk(message);

            /* Send to replica assignments */
            if (replicaHosts.size() == 2) {
                /* Connect to first assignment and send chunk*/
                ChannelFuture cf = bootstrap.connect(replicaHosts.get(0), 13111);
                cf.syncUninterruptibly();
                Channel chan = cf.channel();
                chan.writeAndFlush(message).syncUninterruptibly();
                cf.syncUninterruptibly();
                chan.close().syncUninterruptibly();

                /* Connect to second assignment and send chunk */
                cf = bootstrap.connect(replicaHosts.get(1), 13111);
                cf.syncUninterruptibly();
                chan = cf.channel();
                chan.writeAndFlush(message).syncUninterruptibly();
                cf.syncUninterruptibly();
                chan.close().syncUninterruptibly();
            }
		} else if (message.hasRetrieveFile()) {
            Path filePath = Paths.get(rootDirectory.toString(), message.getRetrieveFile().getFileName());
            if (Files.exists(filePath)) {
                this.shootChunks(ctx, filePath);
            }

        }
	}
    
    /**
     * 
     * @param ctx
     * @param filePath
     */
    public void shootChunks(ChannelHandlerContext ctx, Path filePath) {
    
        /** Get stream over directory's files */
        Stream<Path> chunks = null;
        try {
            chunks = Files.walk(filePath);
        } catch (IOException ioe) {
            logger.info("Could not send chunks back to client");
        }
        
        /* If we got a stream iterate over it and write the chunks back to client */
        if (chunks != null) {
            List<ChannelFuture> writes = new ArrayList<>();
            for (Object chunkPath : chunks.toArray()) {
                if (chunkPath instanceof Path) {
                    ByteString data = null;
                    String checksumCheck = null;

                    /* Read the chunk and compute it's checksum */
                    try {
                        data = ByteString.copyFrom(Files.readAllBytes( (Path) chunkPath ));
                        checksumCheck = Checksum.SHAsum(data.toByteArray());
                    } catch (IOException | NoSuchAlgorithmException ioe) {
                        logger.info("Could not read chunk to send to client");
                    }
                    
                    /* If the reads and checksum computation was succesful, write the chunk to client */
                    if (data != null && checksumCheck != null) {
                        String[] fileTokens = ((Path)chunkPath).toFile().toString().split("#"); 
                        String checksum = fileTokens[fileTokens.length - 1];
                        fileTokens = fileTokens[0].split("@");
                        logger.info("Tokens length " + fileTokens.length);
                        logger.info("Checksum " + checksum);
                        logger.info("Pathname " + ((Path) chunkPath).toString());
                        /* If checksums don't match send request to replica assignment for healing */
                        if (!checksum.equals(checksumCheck)) {
                            logger.info("Chunk " + chunkPath.toString() + "needs healing");
                        } else {
                            /* Get chunk metadata from path */
                            long totalChunks = Long.parseLong(fileTokens[fileTokens.length - 1]); 
                            long chunkID = Long.parseLong(fileTokens[fileTokens.length - 2]);
                            String filename = fileTokens[0];
                            
                            /* Build the chunks and write it to client */
                            StorageMessages.StorageMessageWrapper chunkToSend = Builders.buildStoreChunk(filename, this.hostname, chunkID, totalChunks, data);
                            ChannelFuture write = ctx.pipeline().writeAndFlush(chunkToSend);
                            writes.add(write);
                        }

                    }

                }

            }

            for (ChannelFuture write : writes) {
                write.syncUninterruptibly();
            }
            
        }

    }

    /**
      * Writes a StoreChunk to disk and appends its checksum to the filename
      * The chunk will be stored under the root/home directory entered in the command line on storage node startup
      * This is done with the -r flag
      *
      * Home -
      *          file_chunks -
      *              chunk0#AD12341FFC...
      *              chunk1#AD12341111...
      *              chunk2#12341FFC11...
      *
      *  file_chunks will be named the name of the file whose chunks we are storing
      *  we will also append a chunks chunkid, #, and the sha1sum of the given
      *  file's bytes.
      *
      *  This will allow us to verify to correctness of the data on retrieval 
      *
      * @param message
      */
    public void writeChunk(StorageMessages.StorageMessageWrapper message) {
        /* If this chunk is not being stored on its primary node, log the replication happening */
        if (!message.getStoreChunk().getOriginHost().equals(this.hostname) && message.getStoreChunk().getChunkId() == 0) {
            logger.info("Recieved replica of " + message.getStoreChunk().getFileName() + " for " + message.getStoreChunk().getOriginHost());
        }

        String fileName = message.getStoreChunk().getFileName();
        
        Path chunkPath = Paths.get(rootDirectory.toString(), fileName);
        
        /* If we haven't stored a chunk of this file yet create a directory for the chunks to go into */
        if (!Files.exists(chunkPath)) {
            try {
                Files.createDirectory(chunkPath);
            } catch (IOException e) {
                logger.info("Problem creating path: " + chunkPath);
            }
            logger.info("Created path: " + chunkPath);
        }
        
        ByteString bytes = message.getStoreChunk().getData();

        try {
            /** 
             * Store chunks in users specified home directory 
             */
            Path path = Paths.get(chunkPath.toString(),
                    message.getStoreChunk().getFileName() 
                    + "_chunk@" 
                    + message.getStoreChunk().getChunkId() 
                    + "@"
                    + message.getStoreChunk().getTotalChunks()
                    + "#" 
                    + Checksum.SHAsum(bytes.toByteArray()));

            /* Write chunk to disk */
            byte[] data = message.getStoreChunk().getData().toByteArray();

            Files.write(path, data);
        } catch (IOException | NoSuchAlgorithmException ioe) {
            logger.info("Could not write file/sha1sum not computed properly");
        }
    }
}
