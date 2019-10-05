package edu.usfca.cs.dfs;

import java.io.File;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;

/**
 * Runnable object that sends heartbeats to the Controller every 5 seconds Uses
 * the bootstrap to open a channel, write to it, and then close it
 */
public class HeartBeatRunner implements Runnable {
	/* Heartbeat MetaData */
	String hostname;
	String controllerHost;
	int requests;
	File f;
	Bootstrap bootstrap;

	private static final Logger logger = LogManager.getLogger(StorageNode.class);

	public HeartBeatRunner(String hostname, String controllerHost, Bootstrap bootstrap) {
		f = new File("/bigdata");
		this.hostname = hostname;
		this.controllerHost = controllerHost;
		this.requests = 0;
		this.bootstrap = bootstrap;
	}

    public void bumpRequests() {
        this.requests++;
    }

	@Override
	public void run() {

		while (true) {

			long freeSpace = f.getFreeSpace();

			StorageMessages.StorageMessageWrapper msgWrapper = StorageNode.buildHeartBeat(hostname, freeSpace,
					requests);

			ChannelFuture cf = this.bootstrap.connect(controllerHost, 13100);
			cf.syncUninterruptibly();

			Channel chan = cf.channel();

			ChannelFuture write = chan.writeAndFlush(msgWrapper);
			write.syncUninterruptibly();

			chan.close().syncUninterruptibly();

			logger.debug("Sent heartbeat to " + controllerHost);
			try {
				Thread.sleep(5000);
			} catch (InterruptedException e) {
				logger.debug("Interrupted when sleeping after heartbeat.");
			}

		}
	}
}
