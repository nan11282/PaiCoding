package com.github.paicoding.forum.core.rabbitmq;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.MessageProperties;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Quorum 队列消息发送器 * @date 2025/10/13
 */
@Slf4j
@Component
public class QuorumMessageSender {

    @Autowired
    private RabbitmqHAConfig rabbitmqConfig;

    /**
     * 发送消息到 Quorum 队列（带确认）
     */
    public boolean sendMessage(String queueName, String message) {
        if (!rabbitmqConfig.getSwitchFlag()) {
            log.warn("RabbitMQ 开关已关闭，消息未发送: {}", message);
            return false;
        }

        RabbitmqConnection rabbitmqConnection = null;
        try {
            rabbitmqConnection = rabbitmqConfig.createConnection();
            Connection connection = rabbitmqConnection.getConnection();
            
            if (connection == null || !connection.isOpen()) {
                log.error("RabbitMQ 连接失败，消息发送失败");
                return false;
            }

            try (Channel channel = connection.createChannel()) {
                // 开启发布确认
                channel.confirmSelect();
                
                // 发送持久化消息
                channel.basicPublish(
                    "",                                    // 交换机（使用默认）
                    queueName,                            // 路由键（队列名）
                    MessageProperties.PERSISTENT_TEXT_PLAIN, // 消息持久化
                    message.getBytes("UTF-8")             // 消息内容
                );
                
                // 等待确认（超时5秒）
                if (channel.waitForConfirms(5000)) {
                    log.info("消息发送成功到 Quorum 队列 [{}]: {}", queueName, message);
                    return true;
                } else {
                    log.error("消息发送确认超时，队列: {}", queueName);
                    return false;
                }
            }
            
        } catch (Exception e) {
            log.error("发送消息到 Quorum 队列失败，队列: {}, 消息: {}", queueName, message, e);
            return false;
        } finally {
            if (rabbitmqConnection != null) {
                rabbitmqConnection.close();
            }
        }
    }

    /**
     * 批量发送消息到 Quorum 队列
     */
    public boolean sendBatchMessages(String queueName, String[] messages) {
        if (!rabbitmqConfig.getSwitchFlag()) {
            log.warn("RabbitMQ 开关已关闭，批量消息未发送");
            return false;
        }

        RabbitmqConnection rabbitmqConnection = null;
        try {
            rabbitmqConnection = rabbitmqConfig.createConnection();
            Connection connection = rabbitmqConnection.getConnection();
            
            try (Channel channel = connection.createChannel()) {
                // 开启发布确认
                channel.confirmSelect();
                
                // 批量发送
                for (String message : messages) {
                    channel.basicPublish(
                        "", 
                        queueName, 
                        MessageProperties.PERSISTENT_TEXT_PLAIN, 
                        message.getBytes("UTF-8")
                    );
                }
                
                // 等待所有消息确认
                if (channel.waitForConfirms(10000)) {
                    log.info("批量消息发送成功到 Quorum 队列 [{}]，数量: {}", queueName, messages.length);
                    return true;
                } else {
                    log.error("批量消息发送确认超时，队列: {}", queueName);
                    return false;
                }
            }
            
        } catch (Exception e) {
            log.error("批量发送消息到 Quorum 队列失败，队列: {}", queueName, e);
            return false;
        } finally {
            if (rabbitmqConnection != null) {
                rabbitmqConnection.close();
            }
        }
    }

    /**
     * 发送 JSON 消息（常用于对象序列化）
     */
    public boolean sendJsonMessage(String queueName, Object messageObject) {
        try {
            // 这里可以集成 Jackson 或 Gson 进行序列化
            String jsonMessage = messageObject.toString(); // 简化处理，实际项目中应使用 JSON 库
            return sendMessage(queueName, jsonMessage);
        } catch (Exception e) {
            log.error("发送 JSON 消息失败", e);
            return false;
        }
    }
}
