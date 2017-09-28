package com.xxx.demons.kafkamessage.util;

import com.google.gson.annotations.SerializedName;

import java.util.Map;

public class ZookeeperVO {
    @SerializedName("listener_security_protocol_map")
    public Map listenerSecurityProtocolMap;
    @SerializedName("endpoints")
    public String[] endpoints;
    @SerializedName("jmx_port")
    public Integer jmxPort;
    @SerializedName("host")
    public String host;

    @SerializedName("timestamp")
    public String timestamp;
    @SerializedName("port")
    public Integer port;

    @SerializedName("version")
    public Integer version;
}