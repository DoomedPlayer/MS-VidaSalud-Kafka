package com.BinarySeint.vsKafka.dto;

import java.util.List;

public class ConsumerGroupDetailResponse {

    private String groupId;
    private String state;
    private String coordinator;
    private List<MemberInfo> members;
    private List<PartitionOffsetInfo> offsets;

    public ConsumerGroupDetailResponse() {}

    public ConsumerGroupDetailResponse(String groupId, String state, String coordinator,
                                       List<MemberInfo> members, List<PartitionOffsetInfo> offsets) {
        this.groupId = groupId;
        this.state = state;
        this.coordinator = coordinator;
        this.members = members;
        this.offsets = offsets;
    }

    public String getGroupId() { return groupId; }
    public void setGroupId(String groupId) { this.groupId = groupId; }

    public String getState() { return state; }
    public void setState(String state) { this.state = state; }

    public String getCoordinator() { return coordinator; }
    public void setCoordinator(String coordinator) { this.coordinator = coordinator; }

    public List<MemberInfo> getMembers() { return members; }
    public void setMembers(List<MemberInfo> members) { this.members = members; }

    public List<PartitionOffsetInfo> getOffsets() { return offsets; }
    public void setOffsets(List<PartitionOffsetInfo> offsets) { this.offsets = offsets; }

    public static class MemberInfo {
        private String memberId;
        private String clientId;
        private String host;
        private List<String> assignedPartitions;

        public MemberInfo(String memberId, String clientId, String host, List<String> assignedPartitions) {
            this.memberId = memberId;
            this.clientId = clientId;
            this.host = host;
            this.assignedPartitions = assignedPartitions;
        }

        public String getMemberId() { return memberId; }
        public String getClientId() { return clientId; }
        public String getHost() { return host; }
        public List<String> getAssignedPartitions() { return assignedPartitions; }
    }

    public static class PartitionOffsetInfo {
        private String topic;
        private int partition;
        private long committedOffset;
        private long logEndOffset;
        private long lag;

        public PartitionOffsetInfo(String topic, int partition, long committedOffset, long logEndOffset, long lag) {
            this.topic = topic;
            this.partition = partition;
            this.committedOffset = committedOffset;
            this.logEndOffset = logEndOffset;
            this.lag = lag;
        }

        public String getTopic() { return topic; }
        public int getPartition() { return partition; }
        public long getCommittedOffset() { return committedOffset; }
        public long getLogEndOffset() { return logEndOffset; }
        public long getLag() { return lag; }
    }
}