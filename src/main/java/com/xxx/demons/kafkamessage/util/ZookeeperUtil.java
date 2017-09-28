package com.xxx.demons.kafkamessage.util;

import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;

import java.nio.charset.Charset;
import java.util.List;
import java.util.stream.Collectors;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ZookeeperUtil {
    public static String buildChildPath(String parentPath, String id) {
        return parentPath + "/" + id;
    }

    /**
     * create parent path on zookeeper example: "/vertx_cluster"
     */
    public static void createParentPath(CuratorFramework client, String path) {
        try {
            String result = client.create()
                    .creatingParentContainersIfNeeded()
                    .withMode(CreateMode.PERSISTENT)
                    .withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE)
                    .forPath(path);
            log.debug("zookeeper parent node {} create result: {}", path, result);
            System.out.println("zookeeper parent node {} create result:" + result);
        } catch (KeeperException.NodeExistsException e) {
            log.warn("create parent node {} failed! {} ", path, e.getMessage());
        } catch (Exception e) {
            log.error("create parent node {} failed! ", path, e);
            throw new RuntimeException(e.getMessage());
        }
    }


    /**
     * create a child path on zookeeper example: "/vertx_cluster/id", path's data is this machine
     * join ip
     */
    public static void createChildPath(CuratorFramework client, String path, String data) {
        try {
            String result = client.create()
                    .withMode(CreateMode.EPHEMERAL)
                    .withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE)
                    .forPath(path, data.getBytes(Charset.defaultCharset()));
            log.debug("zookeeper create child node data {} result: {}", data, result);
        } catch (KeeperException.NodeExistsException e) {
            log.warn("create child node {} failed! {} ", path, e.getMessage());
        } catch (Exception e) {
            log.error("create child node {} failed! ", path, e);
            throw new RuntimeException(e.getMessage());
        }
    }


    /**
     * Get child data by path example path: "/vertx_cluster/id" example data: 127.0.0.1:15701
     */
    public static String getChildData(CuratorFramework client, String path) {
        try {
            return new String(client.getData().forPath(path), Charset.defaultCharset());
        } catch (Exception e) {
            log.error("", e);
            throw new RuntimeException(e.getMessage());
        }
    }

    /**
     * fetch children's data by parent path
     */
    public static List<String> fetchChildrenData(CuratorFramework client, String path) {
        try {
            List<String> joins = client.getChildren().forPath(path)
                    .stream()
                    .map(id -> ZookeeperUtil.getChildData(client, buildChildPath(path, id)))
                    .collect(Collectors.toList());
            log.debug("Fetch children's data form zookeeper path : {} {}", path, joins.stream().collect(Collectors.joining(",")));
            return joins;
        } catch (Exception e) {
            log.error("", e);
            throw new RuntimeException(e.getMessage());
        }
    }
}
