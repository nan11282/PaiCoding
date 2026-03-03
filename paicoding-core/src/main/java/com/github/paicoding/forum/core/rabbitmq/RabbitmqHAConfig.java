package com.github.paicoding.forum.core.rabbitmq;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * RabbitMQ 高可用配置 - 适配本地 Docker 集群 * @date 2025/10/13
 */
@Data
@Component
@ConfigurationProperties(prefix = "rabbitmq")
public class RabbitmqHAConfig {
    
    // ========== 单节点配置（兼容旧代码） ==========
    private String host = "127.0.0.1";
    private Integer port = 5672;
    private String username = "guest";  // Docker 默认用户
    private String passport = "guest";  // Docker 默认密码
    private String virtualhost = "/";
    private Boolean switchFlag = false;
    private Integer poolSize = 5;
    
    // ========== 高可用集群配置 ==========
    /**
     * 集群地址，格式: host1:port1,host2:port2,host3:port3
     * 本地 Docker 集群: localhost:5672,localhost:5673,localhost:5674
     */
    private String addresses;
    
    /**
     * 连接超时时间（毫秒）
     */
    private Integer connectionTimeout = 15000;
    
    /**
     * 心跳间隔（秒）
     */
    private Integer requestedHeartbeat = 30;
    
    /**
     * 发布确认类型: none, simple, correlated
     */
    private String publisherConfirmType = "correlated";
    
    /**
     * 是否开启发布返回
     */
    private Boolean publisherReturns = true;
    
    /**
     * 是否使用集群模式
     */
    public boolean isClusterMode() {
        return addresses != null && !addresses.trim().isEmpty();
    }
    
    /**
     * 获取连接实例（根据配置自动选择单节点或集群）
     */
    public RabbitmqConnection createConnection() {
        if (isClusterMode()) {
            return new RabbitmqConnection(addresses, username, passport, virtualhost);
        } else {
            return new RabbitmqConnection(host, port, username, passport, virtualhost);
        }
    }
    
    /**
     * 获取开关状态（兼容旧字段名）
     */
    public Boolean getSwitchFlag() {
        return switchFlag != null ? switchFlag : false;
    }
}
