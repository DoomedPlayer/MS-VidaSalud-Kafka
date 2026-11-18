package com.BinarySeint.vsKafka.dto;

import java.util.Map;

public class UpdateTopicConfigRequest {

    private Integer newPartitionCount; 
    private Map<String, String> configs; 

    public UpdateTopicConfigRequest() {}

    public Integer getNewPartitionCount() { return newPartitionCount; }
    public void setNewPartitionCount(Integer newPartitionCount) { this.newPartitionCount = newPartitionCount; }

    public Map<String, String> getConfigs() { return configs; }
    public void setConfigs(Map<String, String> configs) { this.configs = configs; }
}