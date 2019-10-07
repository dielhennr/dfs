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
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
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

  Path path;

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
      path = arguments.getPath("-f");
      controllerHost = arguments.getString("-h");
    } else {
      System.err.println(
          "Usage: java -cp .... -h hostToContact -f fileToSend.\n"
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
    MessagePipeline pipeline = new MessagePipeline(client);

    Bootstrap bootstrap =
        new Bootstrap()
            .group(workerGroup)
            .channel(NioSocketChannel.class)
            .option(ChannelOption.SO_KEEPALIVE, true)
            .handler(pipeline);

    client.addBootstrap(bootstrap);

    ChannelFuture cf = bootstrap.connect(client.controllerHost, client.port);
    cf.syncUninterruptibly();

    StorageMessages.StorageMessageWrapper msgWrapper =
        Client.buildStoreRequest(client.path.getFileName().toString(), client.chunkSize);

    Channel chan = cf.channel();
    ChannelFuture write = chan.writeAndFlush(msgWrapper);
    write.syncUninterruptibly();
    if (cf.syncUninterruptibly().isSuccess()) {
      System.err.println("Synced on channel after writing to controller");
    }

    /* Don't quit until we've disconnected: */
    System.out.println("Shutting down");
  }

  @Override
  public void onMessage(ChannelHandlerContext ctx, StorageMessageWrapper message) {
    if (message.hasStoreResponse()) {
      logger.info("Recieved permission to put file on " + message.getStoreResponse().getHostname());
      StorageMessages.StorageMessageWrapper msgWrapper =
          Client.buildStoreRequest(this.path.getFileName().toString(), this.chunkSize);
      logger.info("Chunk size: " + this.chunkSize);
      /*
       * At this point we should get a response from controller telling us where to
       * put this file.
       *
       */
      ChannelFuture cf =
          bootstrap.connect(message.getStoreResponse().getHostname(), 13111).syncUninterruptibly();

      ChannelFuture write = cf.channel().writeAndFlush(msgWrapper).syncUninterruptibly();

      if (write.isSuccess() && write.isDone()) {
        logger.info("Sent store request to node " + message.getStoreResponse().getHostname());
      }

      /* Get number of chunks */
      long length = path.toFile().length();
      int chunks = (int) (length / this.chunkSize);

      /* Asynch writes and input stream */
      List<ChannelFuture> writes = new ArrayList<>();
      try (FileInputStream inputStream = new FileInputStream(path.toFile())) {
        byte[] messageBytes = new byte[this.chunkSize];
        /* Write a protobuf to the channel for each chunk */
        for (int i = 0; i < chunks; i++) {
          messageBytes = inputStream.readNBytes(this.chunkSize);
          StorageMessages.StoreChunk storeChunk =
              StorageMessages.StoreChunk.newBuilder()
                  .setFileName(path.getFileName().toString())
                  .setChunkId(i)
                  .setData(ByteString.copyFrom(messageBytes))
                  .build();
          StorageMessages.StorageMessageWrapper wrap =
              StorageMessages.StorageMessageWrapper.newBuilder().setStoreChunk(storeChunk).build();
          writes.add(cf.channel().writeAndFlush(wrap));
        }

        /* We will add one extra chunk for leftover bytes */
        int leftover = (int) (length % this.chunkSize);

        /* If we have leftover bytes */
        if (leftover != 0) {
          /* Read them and write the protobuf */
          byte[] last = new byte[leftover];
          last = inputStream.readNBytes(leftover);
          StorageMessages.StoreChunk storeChunk =
              StorageMessages.StoreChunk.newBuilder()
                  .setFileName(path.getFileName().toString())
                  .setChunkId(chunks)
                  .setData(ByteString.copyFrom(last))
                  .build();
          StorageMessages.StorageMessageWrapper wrap =
              StorageMessages.StorageMessageWrapper.newBuilder().setStoreChunk(storeChunk).build();
          writes.add(cf.channel().writeAndFlush(wrap));
        }

        for (ChannelFuture writeChunk : writes) {
          writeChunk.syncUninterruptibly();
        }

        inputStream.close();
        cf.syncUninterruptibly();
      } catch (FileNotFoundException e1) {
        logger.info("File not found: %s", this.path.getFileName().toString());
      } catch (IOException e2) {
        e2.printStackTrace();
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
  private static StorageMessages.StorageMessageWrapper buildStoreRequest(
      String filename, long fileSize) {

    StorageMessages.StoreRequest storeRequest =
        StorageMessages.StoreRequest.newBuilder()
            .setFileName(filename)
            .setFileSize(fileSize)
            .build();
    StorageMessages.StorageMessageWrapper msgWrapper =
        StorageMessages.StorageMessageWrapper.newBuilder().setStoreRequest(storeRequest).build();

    return msgWrapper;
  }
}
