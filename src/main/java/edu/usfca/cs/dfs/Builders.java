package edu.usfca.cs.dfs;

import com.google.protobuf.ByteString;

public class Builders {

	/**
	 * Build a join request protobuf with hostname/ip
	 *
	 * @param hostname
	 * @param ip
	 * @return the protobuf
	 */
	public static StorageMessages.StorageMessageWrapper buildJoinRequest(String hostname) {
		/* Store hostname in a JoinRequest protobuf */
		StorageMessages.JoinRequest joinRequest = StorageMessages.JoinRequest
                                        .newBuilder()
                                        .setNodeName(hostname)
				                        .build();

		/* Wrapper */
		StorageMessages.StorageMessageWrapper msgWrapper = StorageMessages.StorageMessageWrapper
                                                .newBuilder()
				                                .setJoinRequest(joinRequest)
                                                .build();

		return msgWrapper;
	}

	/**
	 * Build a heartbeat protobuf
	 *
	 * @param hostname
	 * @param freeSpace
	 * @return the protobuf
	 */
	public static StorageMessages.StorageMessageWrapper buildHeartBeat(String hostname, 
                                                                            long freeSpace) {

		StorageMessages.Heartbeat heartbeat = StorageMessages.Heartbeat
                                        .newBuilder()
                                        .setFreeSpace(freeSpace)
				                        .setHostname(hostname)
                                        .setTimestamp(System.currentTimeMillis())
                                        .build();

		StorageMessages.StorageMessageWrapper msgWrapper = StorageMessages.StorageMessageWrapper
                                                    .newBuilder()
                                                    .setHeartbeat(heartbeat)
                                                    .build();

		return msgWrapper;
	}

	/**
	 * Builds a store request protobuf
	 * {@link edu.usfca.cs.dfs.StorageMessages.StoreRequest}
	 * {@link edu.usfca.cs.dfs.StorageMessages.StorageMessageWrapper}
	 *
	 * @param filename
	 * @param fileSize
     * @param replicaHost1
     * @param replicaHost2
     *
	 * @return store request for a storagenode
	 */
	public static StorageMessages.StorageMessageWrapper buildStoreRequest(String filename, long fileSize, String replicaHost1, String replicaHost2) {
        StorageMessages.ReplicaAssignments replicaAssignments = StorageMessages.ReplicaAssignments.newBuilder().setReplica1(replicaHost1).setReplica2(replicaHost2).build();
		StorageMessages.StoreRequest storeRequest = StorageMessages.StoreRequest.newBuilder().setFileName(filename)
				.setFileSize(fileSize).setReplicaAssignments(replicaAssignments).build();

		return StorageMessages.StorageMessageWrapper.newBuilder().setStoreRequest(storeRequest).build();
	}

	/**
	 * Builds a store chunk protobuf
	 * {@link edu.usfca.cs.dfs.StorageMessages.StoreChunk}
	 * {@link edu.usfca.cs.dfs.StorageMessages.StorageMessageWrapper}
	 *
	 * @param fileName
	 * @param id
	 * @param data
	 * @return
	 */
	public static StorageMessages.StorageMessageWrapper buildStoreChunk(String fileName, String primary, int id, ByteString data) {

		StorageMessages.StoreChunk storeChunk = StorageMessages.StoreChunk.newBuilder().setFileName(fileName).setOriginHost(primary)
				.setChunkId(id).setData(data).build();

		return StorageMessages.StorageMessageWrapper.newBuilder().setStoreChunk(storeChunk).build();
	}

	/**
	 * Builds a store response protobuf
	 * {@link edu.usfca.cs.dfs.StorageMessages.StoreResponse}
	 * {@link edu.usfca.cs.dfs.StorageMessages.StorageMessageWrapper}
	 *
	 * @param hostname
	 * @return msgWrapper
	 */
	public static StorageMessages.StorageMessageWrapper buildStoreResponse(String fileName, String hostname, String replicaHost1, String replicaHost2) {
    
        StorageMessages.ReplicaAssignments replicaAssignments = StorageMessages.ReplicaAssignments
                                                .newBuilder()
                                                .setReplica1(replicaHost1)
                                                .setReplica2(replicaHost2)
                                                .build();
        
		StorageMessages.StoreResponse storeRequest = StorageMessages.StoreResponse
                                            .newBuilder()
                                            .setHostname(hostname)
				                            .setFileName(fileName)
                                            .setReplicaAssignments(replicaAssignments)
                                            .build();
		StorageMessages.StorageMessageWrapper msgWrapper = StorageMessages.StorageMessageWrapper
                                                    .newBuilder()
				                                    .setStoreResponse(storeRequest)
                                                    .build();

		return msgWrapper;
	}
}