package com.BinarySeint.vsKafka.dto;

import java.util.Map;

public class CreateTopicRequest {

    private String name;
    private Integer partitions;
    private Short replicationFactor;
    private String cleanupPolicy; 
    private Long retentionMs;    
    private Map<String, String> extraConfigs;

    public CreateTopicRequest() {}

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    public Integer getPartitions() { return partitions; }
    public void setPartitions(Integer partitions) { this.partitions = partitions; }

    public Short getReplicationFactor() { return replicationFactor; }
    public void setReplicationFactor(Short replicationFactor) { this.replicationFactor = replicationFactor; }

    public String getCleanupPolicy() { return cleanupPolicy; }
    public void setCleanupPolicy(String cleanupPolicy) { this.cleanupPolicy = cleanupPolicy; }

    public Long getRetentionMs() { return retentionMs; }
    public void setRetentionMs(Long retentionMs) { this.retentionMs = retentionMs; }

    public Map<String, String> getExtraConfigs() { return extraConfigs; }
    public void setExtraConfigs(Map<String, String> extraConfigs) { this.extraConfigs = extraConfigs; }
}