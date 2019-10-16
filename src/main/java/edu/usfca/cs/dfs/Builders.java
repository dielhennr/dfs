package edu.usfca.cs.dfs;

import java.util.ArrayList;

import com.google.protobuf.ByteString;

public class Builders {

	/**
	 * Build a join request protobuf with hostname/ip
	 *
	 * @param hostname
	 * @param ip
	 * @return join request
	 */
	public static StorageMessages.StorageMessageWrapper buildJoinRequest(String hostname) {
		/* Store hostname in a JoinRequest protobuf */
		StorageMessages.JoinRequest joinRequest = StorageMessages.JoinRequest.newBuilder().setNodeName(hostname)
				.build();

		return StorageMessages.StorageMessageWrapper.newBuilder().setJoinRequest(joinRequest).build();
	}

	/**
	 * Builds a heal request
	 *
	 * @param fileName       - Filename of chunk that needs healing
	 * @param chunkId        - chunk that needs healing
	 * @param originLocation - First host sending this request
	 * @param intermediate   - originLocation's first replication assignment
	 * @param finalLocation  - originLocation's second replication assignment
	 * @return heal request
	 */
	public static StorageMessages.StorageMessageWrapper buildHealRequest(String fileName, long chunkId,
			String originLocation, String intermediate, String finalLocation) {
		/* Store hostname in a JoinRequest protobuf */
		StorageMessages.HealRequest healRequest = StorageMessages.HealRequest.newBuilder().setFileName(fileName)
				.setChunkId(chunkId).setInitialLocation(originLocation).setIntermediateLocation(intermediate)
				.setFinalLocation(finalLocation).build();

		return StorageMessages.StorageMessageWrapper.newBuilder().setHealRequest(healRequest).build();
	}

	/**
	 * Builds a heal response with a healed chunk (or failure bool?)
	 *
	 * @param filename - Filename of this chunk
	 * @param chunkId  - Chunk of file being sent
	 * @param passTo   - additional hosts to pass the response to
	 * @param data     - data to send
	 * @return
	 */
	public static StorageMessages.StorageMessageWrapper buildHealResponse(String filename, long chunkId, String passTo,
			ByteString data) {
		/* Store hostname in a JoinRequest protobuf */
		StorageMessages.HealResponse joinRequest = StorageMessages.HealResponse.newBuilder().setFileName(filename)
				.setChunkId(chunkId).setPassTo(passTo).setData(data).build();

		return StorageMessages.StorageMessageWrapper.newBuilder().setHealResponse(joinRequest).build();
	}

	/**
	 * Build a heartbeat protobuf
	 *
	 * @param hostname
	 * @param freeSpace
	 * @return heartbeat
	 */
	public static StorageMessages.StorageMessageWrapper buildHeartBeat(String hostname, long freeSpace) {

		StorageMessages.Heartbeat heartbeat = StorageMessages.Heartbeat.newBuilder().setFreeSpace(freeSpace)
				.setHostname(hostname).setTimestamp(System.currentTimeMillis()).build();

		return StorageMessages.StorageMessageWrapper.newBuilder().setHeartbeat(heartbeat).build();
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
	 * @return store request
	 */
	public static StorageMessages.StorageMessageWrapper buildStoreRequest(String filename, long fileSize,
			String replicaHost1, String replicaHost2) {
		StorageMessages.ReplicaAssignments replicaAssignments = StorageMessages.ReplicaAssignments.newBuilder()
				.setReplica1(replicaHost1).setReplica2(replicaHost2).build();

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
	 * @return store chunk
	 */
	public static StorageMessages.StorageMessageWrapper buildStoreChunk(String fileName, String primary, long id,
			long totalChunks, ByteString data) {

		StorageMessages.StoreChunk storeChunk = StorageMessages.StoreChunk.newBuilder().setFileName(fileName)
				.setOriginHost(primary).setChunkId(id).setTotalChunks(totalChunks).setData(data).build();

		return StorageMessages.StorageMessageWrapper.newBuilder().setStoreChunk(storeChunk).build();
	}

	/**
	 * Builds a store response protobuf
	 * {@link edu.usfca.cs.dfs.StorageMessages.ReplicaAssignments}
	 * {@link edu.usfca.cs.dfs.StorageMessages.StoreResponse}
	 * {@link edu.usfca.cs.dfs.StorageMessages.StorageMessageWrapper}
	 *
	 * @param hostname
	 * @return store response
	 */
	public static StorageMessages.StorageMessageWrapper buildStoreResponse(String fileName, String hostname,
			String replicaHost1, String replicaHost2) {

		StorageMessages.ReplicaAssignments replicaAssignments = StorageMessages.ReplicaAssignments.newBuilder()
				.setReplica1(replicaHost1).setReplica2(replicaHost2).build();

		StorageMessages.StoreResponse storeRequest = StorageMessages.StoreResponse.newBuilder().setHostname(hostname)
				.setFileName(fileName).setReplicaAssignments(replicaAssignments).build();

		return StorageMessages.StorageMessageWrapper.newBuilder().setStoreResponse(storeRequest).build();
	}

	/**
	 * 
	 * @param fileName
	 * @return
	 */
	public static StorageMessages.StorageMessageWrapper buildRetrievalRequest(String fileName) {
		StorageMessages.RetrieveFile retrievalRequest = StorageMessages.RetrieveFile.newBuilder().setFileName(fileName)
				.build();

		return StorageMessages.StorageMessageWrapper.newBuilder().setRetrieveFile(retrievalRequest).build();
	}

	/**
	 * 
	 * @param hosts
	 * @param fileName
	 * @return
	 */
	public static StorageMessages.StorageMessageWrapper buildPossibleRetrievalHosts(String hosts, String fileName) {
		StorageMessages.PossibleRetrievalHosts hostsResponse = StorageMessages.PossibleRetrievalHosts.newBuilder()
				.setHosts(hosts).setFileName(fileName).build();

		return StorageMessages.StorageMessageWrapper.newBuilder().setRetrievalHosts(hostsResponse).build();
	}

	public static StorageMessages.StorageMessageWrapper buildMergeReplicasOnFailure(String downNodeHost,
			String downNodeReplicaAssignment2) {

		StorageMessages.MergeReplicasOnFailure replicaRequest = StorageMessages.MergeReplicasOnFailure.newBuilder()
				.setDownNodeHostName(downNodeHost).setReplicaAssignment2FromDownNode(downNodeReplicaAssignment2)
				.build();

		return StorageMessages.StorageMessageWrapper.newBuilder().setMergeReplicasOnFailure(replicaRequest).build();
	}

	public static StorageMessages.StorageMessageWrapper buildPrintRequest(ArrayList<StorageNodeContext> listOfNodes) {
		StorageMessages.PrintNodesRequest printRequest;
		if (listOfNodes == null) {
			printRequest = StorageMessages.PrintNodesRequest.newBuilder().build();
		} else {
			ArrayList<StorageMessages.NodeState> nodeList = new ArrayList<>();
			StorageMessages.NodeState nodeState;
			for (StorageNodeContext node : listOfNodes) {
				nodeState = StorageMessages.NodeState.newBuilder().setDiskSpace(node.getFreeSpace())
						.setNodeName(node.getHostName()).setRequests(node.getRequests()).build();
				nodeList.add(nodeState);

			}
			printRequest = StorageMessages.PrintNodesRequest.newBuilder().addAllNodes(nodeList).build();

		}

		return StorageMessages.StorageMessageWrapper.newBuilder().setPrintRequest(printRequest).build();

	}

	/**
	 * 
	 * @param downNode
	 * @param newAssignment
	 * @return rereplicate to new node request
	 */
	public static StorageMessages.StorageMessageWrapper buildReplicateToNewAssignment(String downNode,
			String newAssignment) {
		StorageMessages.ReplicateToNewAssignment replicateRequest = StorageMessages.ReplicateToNewAssignment
				.newBuilder().setDownNode(downNode).setNewAssignment(newAssignment).build();

		return StorageMessages.StorageMessageWrapper.newBuilder().setReplicateToNewAssignment(replicateRequest).build();
	}

	/**
	 * 
	 * @param downNode
	 * @return deleteRequest
	 */
	public static StorageMessages.StorageMessageWrapper buildDeleteData(String downNode) {
		StorageMessages.DeleteData deleteRequest = StorageMessages.DeleteData.newBuilder().setDownNode(downNode)
				.build();

		return StorageMessages.StorageMessageWrapper.newBuilder().setDeleteData(deleteRequest).build();
	}

	public static StorageMessages.StorageMessageWrapper buildSimplyMerge(String owner, String passedFrom) {
		StorageMessages.SimplyMerge simplyMerge = StorageMessages.SimplyMerge.newBuilder().setOwner(owner)
				.setOwnershipPassedFrom(passedFrom).build();

		return StorageMessages.StorageMessageWrapper.newBuilder().setSimplyMerge(simplyMerge).build();
	}
}
