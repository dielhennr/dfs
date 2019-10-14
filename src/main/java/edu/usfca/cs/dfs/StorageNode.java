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

public class StorageNode implements DFSNode {

	private static final Logger logger = LogManager.getLogger(StorageNode.class);

	ServerMessageRouter messageRouter;

	private InetAddress ip;

	String hostname;

	String controllerHostName;

	Path rootDirectory;

	ArgumentMap arguments;

	ArrayList<String> replicaHosts;

	Bootstrap bootstrap;

    ChannelHandlerContext clientCtx;
	
	HashMap<String, ArrayList<ChunkWrapper>> chunkMap;   // Mapping filenames to the chunks
	HashMap<String, ArrayList<Path>> hostnameToChunks;	   // Mapping hostnames to Paths of chunks

	public StorageNode(String[] args) throws UnknownHostException {
		replicaHosts = new ArrayList<>();
		chunkMap = new HashMap<>();
		hostnameToChunks = new HashMap<>();
		ip = InetAddress.getLocalHost();
        clientCtx = null;
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

		ChannelFuture cf = storageNode.bootstrap.connect(storageNode.controllerHostName, 13112);
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
				storageNode.bootstrap);
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
            StorageMessages.StoreRequest storeRequest = message.getStoreRequest();			
			/* Only accept first replica assignments for now */
			if (replicaHosts.isEmpty()) {
				replicaHosts.add(storeRequest.getReplicaAssignments().getReplica1());
				replicaHosts.add(storeRequest.getReplicaAssignments().getReplica2());
				logger.info("Replica Assignment 1: " + storeRequest.getReplicaAssignments().getReplica1());
				logger.info("Replica Assignment 2: " + storeRequest.getReplicaAssignments().getReplica2());
			} else {
				logger.info("Rejecting Assignment");
			}

			logger.info("Request to store " + storeRequest.getFileName() + " size: "
					+ storeRequest.getFileSize());
		} else if (message.hasStoreChunk()) {
			/* Write that shit to disk */
			hostnameToChunks.putIfAbsent(message.getStoreChunk().getOriginHost(), new ArrayList<Path>());
            chunkMap.putIfAbsent(message.getStoreChunk().getFileName(), new ArrayList<ChunkWrapper>());
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
            logger.info("Attempting to shoot chunks of " + message.getRetrieveFile().getFileName() + " to client");
			Path filePath = Paths.get(rootDirectory.toString(), message.getRetrieveFile().getFileName());
            clientCtx = ctx;
			if (Files.exists(filePath)) {
				this.shootChunks(filePath);
			}
		} else if (message.hasHealRequest()) {
            /** 
             * If we get a heal request we need to write back a healed chunk if we have it, 
             * otherwise we need  to send a request to the final location of the chunk 
             */
            StorageMessages.HealRequest healRequest = message.getHealRequest();
            StorageMessages.StorageMessageWrapper healResponse = null;

            /* Close the request channel */
            ctx.channel().close().syncUninterruptibly();

            if (healRequest.getIntermediateLocation().equals(this.hostname)) {
                logger.info("At Intermediate location: " + healRequest.getIntermediateLocation());
                /* If our chunk matches its checksum we can send it back to client, otherwise send request to the final replica location */
                if ( (healResponse = this.getChunkAsHealResponse(message.getHealRequest())) != null ) {
                    logger.info("Found valid chunk here for " + healRequest.getInitialLocation());
                    ChannelFuture cf = bootstrap.connect(healRequest.getInitialLocation(), 13114).syncUninterruptibly();
                    cf.channel().writeAndFlush(healResponse).syncUninterruptibly();
                } else {
                    logger.info("Did not find valid chunk here for " + healRequest.getInitialLocation());
                    logger.info("Requesting from " + healRequest.getFinalLocation());
                    ChannelFuture requestAgain = this.bootstrap.connect(healRequest.getFinalLocation(), 13114).syncUninterruptibly();
                    requestAgain.channel().writeAndFlush(message).syncUninterruptibly();
                }

            } else if (healRequest.getFinalLocation().equals(this.hostname)) {
                logger.info("At Final location: " + healRequest.getFinalLocation());
                /** 
                 * If our chunk matches its checksum we can send it back to intermediate location, 
                 * Otherwise all of our data is corrupted and we cannot retrieve it
                 */
                if ( (healResponse = this.getChunkAsHealResponse(message.getHealRequest())) != null) {
                    ChannelFuture cf = bootstrap.connect(healRequest.getIntermediateLocation(), 13114).syncUninterruptibly();
                    cf.channel().writeAndFlush(healResponse).syncUninterruptibly();
                } else {
                    logger.info("All data corrupted for " + healRequest.getFileName() + " chunk " + healRequest.getChunkId());
                }
            }
        } else if (message.hasHealResponse()) {
            logger.info("recieved healed chunk on " + this.hostname);
            StorageMessages.HealResponse healResponse = message.getHealResponse();
            ctx.channel().close().syncUninterruptibly();
            long totalChunks = 0;
            synchronized(chunkMap) {
                totalChunks = chunkMap.get(healResponse.getFileName()).size();
            }

            StorageMessages.StorageMessageWrapper chunk = Builders.buildStoreChunk( healResponse.getFileName(), 
                                                                                    healResponse.getPassTo(), 
                                                                                    healResponse.getChunkId(), 
                                                                                    totalChunks, 
                                                                                    healResponse.getData() );
            /* Shoot the healed chunk back to client if we are done healing */
            if (message.getHealResponse().getPassTo().equals(this.hostname)) {
                logger.info("Shooting healed chunk to client.");
                this.clientCtx.pipeline().writeAndFlush(chunk).syncUninterruptibly();
            } else {
                ChannelFuture cf = bootstrap.connect(message.getHealResponse().getPassTo(), 13114).syncUninterruptibly();
                cf.channel().writeAndFlush(message).syncUninterruptibly();
            }

            /* Intermediate node and primary node write healed chunk to disk */
            this.writeChunk(chunk);
        }
	}
    
    /**
     * Attempts to find a healed chunk in the filesystem for the request
     *
     * @param healRequestWrapper
     * @return healedchunk
     */
    public StorageMessages.StorageMessageWrapper getChunkAsHealResponse(StorageMessages.HealRequest healRequest)  {
        StorageMessages.StorageMessageWrapper healedChunk = null; 
        String filename = healRequest.getFileName();
        int chunkID = (int) healRequest.getChunkId();
        logger.info("Request to heal " + healRequest.getFileName() + " chunk " + healRequest.getChunkId());
        logger.info("Origin location " + healRequest.getInitialLocation());

        synchronized (chunkMap) {
            for (ChunkWrapper chunk : chunkMap.get(filename)) {
                if (chunk.chunkID == chunkID) {
                    try {
                        byte[] data = Files.readAllBytes(chunk.getPath());
                        if (chunk.checksum.equals(Checksum.SHAsum(data))) {
                            String passTo = null;

                            if (healRequest.getFinalLocation().equals(this.hostname)) {
                                passTo = healRequest.getInitialLocation();
                            }

                            StorageMessages.HealResponse validChunk = StorageMessages.HealResponse
                                                            .newBuilder()
                                                            .setFileName(filename)
                                                            .setChunkId(chunkID)
                                                            .setPassTo(passTo)
                                                            .setData(ByteString.copyFrom(data))
                                                            .build();

                            healedChunk = StorageMessages.StorageMessageWrapper.newBuilder().setHealResponse(validChunk).build();
                               
                        }
                    } catch (IOException | NoSuchAlgorithmException ioe) {
                        logger.info("Could not read chunk for heal request");
                    }
                    break;
                }
            }
        }
        
        return healedChunk;

    }

	/**
	 * Find all chunks // tokenize them check metadata // heal if neccessary // send
	 * chunks to client
	 *
	 * @param filePath {@link ChunkFinder} Produce a list of all chunks from root
	 *                 file directory
	 */
	public void shootChunks(Path filePath) {

	    /* Get chunks of the file requested */	
		ArrayList<ChunkWrapper> chunks = chunkMap.get(filePath.getFileName().toString());
		/* Write the chunks back to client */
		if (chunks != null) {
            
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
				 * client or request a healed chunk depending on wether or not checksums match
				 */
				if (data != null && checksumCheck != null) {
					String[] fileTokens = chunkPath.getFileName().toString().split("#");
					String checksum = fileTokens[fileTokens.length - 1];

					/* If checksums don't match, send request to first replica assignment for healing */
					if (!checksum.equals(checksumCheck)) {

                        /**
                         * If our the checksum of a chunk no longer matches we will send a request
                         * To our first replica assignment to send back a valid chunk
                         */
						logger.info("Chunk " + chunkPath.toString() + "needs healing");
                        try {
                            /* Delete the corrupted chunk */
                            Files.deleteIfExists(chunkPath);
                        } catch (IOException ioe) {
                            logger.info("Could not delete corrupted chunk");
                        }

                        /* Build the heal request and send it the our first replica assignment with the 
                         * hostname of the last replica assignment  
                         **/
                        ChannelFuture healRequest = this.bootstrap.connect(replicaHosts.get(0), 13114);
                        healRequest.syncUninterruptibly();
                        ChannelFuture write = healRequest.channel().writeAndFlush(
                            Builders.buildHealRequest(chunk.getFileName(), chunk.getChunkID(), this.hostname, 
                                replicaHosts.get(0), replicaHosts.get(1)));
                        write.syncUninterruptibly();
					} else {
						/* Build the store chunks and write it to client */
						StorageMessages.StorageMessageWrapper chunkToSend = Builders.buildStoreChunk(chunk.getFileName(),
								this.hostname, chunk.getChunkID(), chunk.getTotalChunks(), data);
						this.clientCtx.pipeline().writeAndFlush(chunkToSend);
					}
				}
			} 
		} else {
            logger.info("Could not find chunks");
        }
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
                chunkMap.get(fileName).add(new ChunkWrapper(path, fileName, message.getStoreChunk().getChunkId()
                        , message.getStoreChunk().getTotalChunks(), checksum));
            }

			/* Write chunk to disk */
			Files.write(path, data);
		} catch (IOException | NoSuchAlgorithmException ioe) {
			logger.info("Could not write file/sha1sum not computed properly");
		}
	}
}
