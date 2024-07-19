package com.scz.demo.listener;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.connection.stream.Record;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.stream.StreamListener;
import org.springframework.stereotype.Component;

/**
 * @Description
 * @Author scz
 * @CreateDate 2024/7/19 上午11:10
 * @Version 1.0
 **/

@Slf4j
@Component
public class MsgListener implements StreamListener {

    private RedisTemplate<String, Object> redisTemplate;


    public MsgListener(RedisTemplate<String, Object> redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    @Override
    public void onMessage(Record message) {
        log.info("Received message: id:{} data:{}", message.getId(), message.getValue());
        try {
            Thread.sleep(50);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        //TODO GET redis or jdbc data
        redisTemplate.opsForValue().getAndDelete("key");
        log.info("Processed message end");
    }
}
