package com.github.paicoding.forum.core.rabbitmq;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

/**
 * Quorum 队列管理器 - 应用启动时自动声明 Quorum 队列 * @date 2025/10/13
 */
@Slf4j
@Component
public class QuorumQueueManager implements CommandLineRunner {

    @Autowired
    private RabbitmqHAConfig rabbitmqConfig;

    /**
     * 应用启动时执行，声明所有需要的 Quorum 队列
     */
    @Override
    public void run(String... args) throws Exception {
        if (!rabbitmqConfig.getSwitchFlag()) {
            log.info("RabbitMQ 开关已关闭，跳过队列声明");
            return;
        }

        log.info("开始声明 Quorum 队列...");
        
        // 声明项目中需要的队列
        declareQuorumQueues();
        
        log.info("Quorum 队列声明完成");
    }

    /**
     * 声明所有 Quorum 队列
     */
    private void declareQuorumQueues() {
        RabbitmqConnection rabbitmqConnection = null;
        try {
            rabbitmqConnection = rabbitmqConfig.createConnection();
            Connection connection = rabbitmqConnection.getConnection();
            
            if (connection == null || !connection.isOpen()) {
                log.error("RabbitMQ 连接失败，无法声明队列");
                return;
            }

            try (Channel channel = connection.createChannel()) {
                // 项目中常用的队列名称，根据实际业务调整
                String[] queueNames = {
                    "article.create",      // 文章创建队列
                    "article.update",      // 文章更新队列
                    "user.register",       // 用户注册队列
                    "comment.create",      // 评论创建队列
                    "notification.send",   // 通知发送队列
                    "email.send",          // 邮件发送队列
                    "search.index"         // 搜索索引队列
                };

                for (String queueName : queueNames) {
                    declareQuorumQueue(channel, queueName);
                }
                
                // 声明死信队列（普通队列即可）
                declareDLXQueue(channel);
            }
            
        } catch (Exception e) {
            log.error("声明 Quorum 队列失败", e);
        } finally {
            if (rabbitmqConnection != null) {
                rabbitmqConnection.close();
            }
        }
    }

    /**
     * 声明单个 Quorum 队列
     */
    private void declareQuorumQueue(Channel channel, String queueName) {
        try {
            Map<String, Object> args = new HashMap<>();
            args.put("x-queue-type", "quorum");                    // 队列类型：Quorum
            args.put("x-quorum-initial-group-size", 3);            // 初始副本数：3（匹配集群节点数）
            args.put("x-dead-letter-exchange", "dlx.exchange");    // 死信交换机
            args.put("x-dead-letter-routing-key", "dlx.routing");  // 死信路由键
            
            // 声明队列（如果已存在且参数一致，则忽略）
            channel.queueDeclare(queueName, true, false, false, args);
            log.info("Quorum 队列声明成功: {}", queueName);
            
        } catch (Exception e) {
            log.error("声明 Quorum 队列失败: {}", queueName, e);
        }
    }

    /**
     * 声明死信队列和交换机
     */
    private void declareDLXQueue(Channel channel) {
        try {
            // 声明死信交换机
            channel.exchangeDeclare("dlx.exchange", "direct", true);
            
            // 声明死信队列（普通队列）
            channel.queueDeclare("dlx.queue", true, false, false, null);
            
            // 绑定死信队列到死信交换机
            channel.queueBind("dlx.queue", "dlx.exchange", "dlx.routing");
            
            log.info("死信队列声明成功: dlx.queue");
            
        } catch (Exception e) {
            log.error("声明死信队列失败", e);
        }
    }

    /**
     * 手动声明自定义 Quorum 队列的工具方法
     */
    public void declareCustomQuorumQueue(String queueName, Map<String, Object> additionalArgs) {
        RabbitmqConnection rabbitmqConnection = null;
        try {
            rabbitmqConnection = rabbitmqConfig.createConnection();
            Connection connection = rabbitmqConnection.getConnection();
            
            try (Channel channel = connection.createChannel()) {
                Map<String, Object> args = new HashMap<>();
                args.put("x-queue-type", "quorum");
                args.put("x-quorum-initial-group-size", 3);
                
                // 合并自定义参数
                if (additionalArgs != null) {
                    args.putAll(additionalArgs);
                }
                
                channel.queueDeclare(queueName, true, false, false, args);
                log.info("自定义 Quorum 队列声明成功: {}", queueName);
            }
            
        } catch (Exception e) {
            log.error("声明自定义 Quorum 队列失败: {}", queueName, e);
        } finally {
            if (rabbitmqConnection != null) {
                rabbitmqConnection.close();
            }
        }
    }
}
