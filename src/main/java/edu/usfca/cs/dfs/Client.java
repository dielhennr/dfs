package edu.usfca.cs.dfs;

import com.google.protobuf.ByteString;
import edu.usfca.cs.dfs.StorageMessages.NodeState;
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
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
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

  Integer totalRetrievalChunks;

  /* Clients bootstrap for connecting to controller and storeagenodes */
  Bootstrap bootstrap;

  EventLoopGroup workerGroup;

  SortedSet<StorageMessages.StoreChunk> retrievalSet;

  /**
   * Constructs a client given the command line arguments
   *
   * @param args
   */
  public Client(String[] args) {
    /* Command Line Arguments */
    this.arguments = new ArgumentMap(args);

    /* Check that user entered controller to connect to and file to send */
    if (arguments.hasFlag("-h")
        && (((arguments.hasFlag("-r") || arguments.hasFlag("-f")) || arguments.hasFlag("-l")))) {
      controllerHost = arguments.getString("-h");
    } else {
      System.err.println(
          "Usage: java -cp .... -h hostToContact -[fr] fileToSend/Retrieve.\n"
              + "-p port and -c <chunksize(int)>  are optional flags.");
      System.exit(1);
    }

    if (arguments.hasFlag("-r")) {
      path = arguments.getPath("-r");
      retrievalSet =
          Collections.synchronizedSortedSet(
              new TreeSet<StorageMessages.StoreChunk>(new StoreChunkComparator()));
    }
    if (arguments.hasFlag("-f")) {
      path = arguments.getPath("-f");
    }

    /* Default to port 13100 */
    port = arguments.getInteger("-p", 13112);

    /* Default Chunk size to 50mb */
    this.chunkSize = arguments.getInteger("-c", 50000000);

    this.totalRetrievalChunks = 0;

    workerGroup = new NioEventLoopGroup();
    MessagePipeline pipeline = new MessagePipeline(this);

    bootstrap =
        new Bootstrap()
            .group(workerGroup)
            .channel(NioSocketChannel.class)
            .option(ChannelOption.SO_KEEPALIVE, true)
            .handler(pipeline);
  }

  public static void main(String[] args) throws IOException {
    /* Create this node for interfacing in the pipeline */

    Client client = new Client(args);

    ChannelFuture cf = client.bootstrap.connect(client.controllerHost, client.port);
    cf.syncUninterruptibly();

    StorageMessages.StorageMessageWrapper msgWrapper = null;
    if (client.arguments.hasFlag("-f")) {
      msgWrapper =
          Builders.buildStoreRequest(client.path.getFileName().toString(), client.chunkSize);
    } else if (client.arguments.hasFlag("-r")) {
      msgWrapper = Builders.buildRetrievalRequest(client.path.getFileName().toString());
    } else if (client.arguments.hasFlag("-l")) {
      msgWrapper = Builders.buildPrintRequest(null);
    }
    Channel chan = cf.channel();
    ChannelFuture write = chan.writeAndFlush(msgWrapper);
    write.syncUninterruptibly();
    if (cf.syncUninterruptibly().isSuccess()) {
      System.err.println("Synced on channel after writing to controller");
    }
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

      /*
       * Build a store request to send to the storagenode so that it can change it's
       * decoder
       */
      StorageMessages.StorageMessageWrapper storeRequest =
          Builders.buildStoreRequest(message.getStoreResponse().getFileName(), this.chunkSize);

      logger.info(
          "Sending chunks in size "
              + this.chunkSize
              + " to "
              + message.getStoreResponse().getHostname());

      /**
       * Connect to StorageNode and start sending chunks
       *
       * <p>Not sure if these ports should be hard coded thats something you could change too
       * Storeagenodes connect to controller through a port but i dont think client can send chunks
       * through that port if its being used to listen. That's why I hard coded client to connect to
       * a SN through 13111 and the SNs to listen on 13111. Meanwhile controller is listening on
       * 13000 for heartbeats from SNs HeartBeatRunner.
       */
      ChannelFuture cf =
          bootstrap.connect(message.getStoreResponse().getHostname(), 13114).syncUninterruptibly();

      /*
       * Write the request to overwrite previous data under this filename
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

      int totalChunks = leftover == 0 ? chunks : chunks + 1;

      /* Asynch writes and input stream */
      List<ChannelFuture> writes = new ArrayList<>();

      try (InputStream inputStream = Files.newInputStream(this.path)) {
        /* Write a protobuf to the channel for each chunk */
        byte[] data = new byte[this.chunkSize];
        for (int i = 0; i < chunks; i++) {
          data = inputStream.readNBytes(this.chunkSize);
          StorageMessageWrapper chunk =
              Builders.buildStoreChunk(
                  path.getFileName().toString(),
                  message.getStoreResponse().getHostname(),
                  i,
                  totalChunks,
                  ByteString.copyFrom(data));
          writes.add(cf.channel().write(chunk));
        }

        /* If we have leftover bytes */
        data = new byte[leftover];
        if (leftover != 0) {
          data = inputStream.readNBytes(leftover);
          /* Read them and write the protobuf */
          StorageMessageWrapper chunk =
              Builders.buildStoreChunk(
                  path.getFileName().toString(),
                  message.getStoreResponse().getHostname(),
                  chunks,
                  totalChunks,
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
      System.err.println("Shutting down");
    } else if (message.hasRetrievalHosts()) {
      String[] possibleHosts = message.getRetrievalHosts().getHosts().split(" ");
      String fileName = message.getRetrievalHosts().getFileName();

      logger.info(
          "Possible Hosts for file " + fileName + " ---> " + Arrays.toString(possibleHosts));
      /* Sending retrieval requests to notes with the file we want */
      for (String host : possibleHosts) {
        // Open connections for nodes and check if they have the file
        ChannelFuture cf = bootstrap.connect(host, 13114);
        cf.syncUninterruptibly();
        Channel chan = cf.channel();
        ChannelFuture write =
            chan.writeAndFlush(Builders.buildRetrievalRequest(path.toFile().toString()));
        write.syncUninterruptibly();
      }
    } else if (message.hasStoreChunk()) {
      /* Add chunk to our set sorted by chunkID */
      if (message.getStoreChunk().getChunkId() == 0) {
        this.totalRetrievalChunks = (int) message.getStoreChunk().getTotalChunks();
      }

      retrievalSet.add(message.getStoreChunk());
      logger.info("Recieved retrieval chunk. So far we retrieved " + retrievalSet.size());
      logger.info(
          "This chunks says that there are "
              + message.getStoreChunk().getTotalChunks()
              + " total chunks");
      if (retrievalSet.size() == this.totalRetrievalChunks) {
        logger.info("Done with retrieval");
        this.stitchChunks();
        ctx.channel().close().syncUninterruptibly();
      }
    } else if (message.hasPrintRequest()) {
      StorageMessages.PrintNodesRequest printRequest = message.getPrintRequest();

      List<NodeState> nodeStateList = printRequest.getNodesList();

      for (StorageMessages.NodeState node : nodeStateList) {
        long freeSpace = node.getDiskSpace();
        String nodename = node.getNodeName();
        int requests = node.getRequests();

        System.out.println("Node Name: " + nodename + " --->");
        System.out.println("Total requests: " + requests);
        System.out.println("Free Disk Space: " + freeSpace);
        System.out.println("------------------------------");
      }

      ctx.channel().close().syncUninterruptibly();
      this.workerGroup.shutdownGracefully();
    }
  }

  /** Stitch together retrieved chunks */
  public void stitchChunks() {
    String cwd = System.getProperty("user.dir");
    logger.info("cwd: " + cwd);
    Path path = Paths.get(cwd, retrievalSet.first().getFileName() + "_retrieval");
    logger.info("file: " + retrievalSet.first().getFileName());

    for (StorageMessages.StoreChunk chunk : retrievalSet) {
      try {
        Files.write(
            path,
            chunk.getData().toByteArray(),
            StandardOpenOption.APPEND,
            StandardOpenOption.CREATE);
      } catch (IOException ioe) {
        logger.info("Could not stitch chunks together");
      }
    }
    workerGroup.shutdownGracefully();
  }
}
