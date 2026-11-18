package com.BinarySeint.vsKafka.dto;

import java.util.Map;

public class TopicDetailResponse {

    private String name;
    private int partitions;
    private int replicationFactor;
    private Map<String, String> configs;

    public TopicDetailResponse() {}

    public TopicDetailResponse(String name, int partitions, int replicationFactor, Map<String, String> configs) {
        this.name = name;
        this.partitions = partitions;
        this.replicationFactor = replicationFactor;
        this.configs = configs;
    }

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    public int getPartitions() { return partitions; }
    public void setPartitions(int partitions) { this.partitions = partitions; }

    public int getReplicationFactor() { return replicationFactor; }
    public void setReplicationFactor(int replicationFactor) { this.replicationFactor = replicationFactor; }

    public Map<String, String> getConfigs() { return configs; }
    public void setConfigs(Map<String, String> configs) { this.configs = configs; }
}