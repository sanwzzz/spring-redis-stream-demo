package com.scz.demo;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.jsontype.impl.LaissezFaireSubTypeValidator;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.datatype.jsr310.deser.LocalDateDeserializer;
import com.fasterxml.jackson.datatype.jsr310.deser.LocalDateTimeDeserializer;
import com.fasterxml.jackson.datatype.jsr310.ser.LocalDateSerializer;
import com.fasterxml.jackson.datatype.jsr310.ser.LocalDateTimeSerializer;
import com.scz.demo.listener.MsgListener;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StreamOperations;
import org.springframework.data.redis.serializer.Jackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.data.redis.stream.StreamMessageListenerContainer;
import org.springframework.data.redis.stream.Subscription;

import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

@Slf4j
@SpringBootApplication
public class SpringRedisStreamDemoApplication implements InitializingBean {

    static final String streamKey = "stream-key";
    static final String consumerGroupName = "defGroup";


    public static void main(String[] args) {
        SpringApplication.run(SpringRedisStreamDemoApplication.class, args);
    }


    @Autowired
    RedisTemplate<String, Object> redisTemplate;

    @Autowired
    MsgListener msgListener;


    @Override
    public void afterPropertiesSet() throws Exception {
        new Thread(() -> {
            int cnt = 0;
            log.info("Start send message...");
            while (true) {
                try {
                    for (int i = 0; i < 100; i++) {
                        redisTemplate.opsForStream().add(MapRecord.create(streamKey, Map.of("key", "value", "time", LocalDateTime.now())));
                    }
                    log.info("Send message... {}", ++cnt);
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();
    }


    @Bean(initMethod = "start", destroyMethod = "stop", value = "streamContainer")
    public StreamMessageListenerContainer streamContainer(
            RedisConnectionFactory connectionFactory) {
        log.info("Load StreamMessageListenerContainer Start...");

        ObjectMapper om = new ObjectMapper();
        om.setVisibility(PropertyAccessor.ALL, JsonAutoDetect.Visibility.ANY);
        om.enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL);

        JavaTimeModule timeModule = new JavaTimeModule();
        timeModule.addDeserializer(LocalDate.class, new LocalDateDeserializer(DateTimeFormatter.ofPattern("yyyy-MM-dd")));
        timeModule.addDeserializer(LocalDateTime.class, new LocalDateTimeDeserializer(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")));
        timeModule.addSerializer(LocalDate.class, new LocalDateSerializer(DateTimeFormatter.ofPattern("yyyy-MM-dd")));
        timeModule.addSerializer(LocalDateTime.class, new LocalDateTimeSerializer(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")));

        om.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        om.registerModule(timeModule);

        Jackson2JsonRedisSerializer<Object> jackson2JsonRedisSerializer = new Jackson2JsonRedisSerializer<>(om, Object.class);

        StreamMessageListenerContainer.StreamMessageListenerContainerOptions options = StreamMessageListenerContainer.StreamMessageListenerContainerOptions
                .builder()
                .pollTimeout(Duration.ofMillis(3000))
//                .batchSize(1) // 批量抓取消息
                .keySerializer(new StringRedisSerializer())
                .hashKeySerializer(new StringRedisSerializer())
                .hashValueSerializer(jackson2JsonRedisSerializer)
//                .executor(Executors.newSingleThreadExecutor())
                .build();

        StreamMessageListenerContainer container = StreamMessageListenerContainer.create(connectionFactory, options);

        StreamOperations streamOperations = redisTemplate.opsForStream();

        //init stream
        if(!this.redisTemplate.hasKey(streamKey)) {
            ObjectRecord<String, Map<String, String>> record = ObjectRecord.create(streamKey, Collections.singletonMap("test", "test"));
            RecordId recordId = streamOperations.add(record);
            streamOperations.delete(streamKey, recordId);
        }


        //init group
        StreamInfo.XInfoGroups groups = streamOperations.groups(streamKey);
        Optional<StreamInfo.XInfoGroup> first = groups.stream().filter(v -> Objects.equals(consumerGroupName, v.groupName())).findFirst();
        if (!first.isPresent()) {
            streamOperations.createGroup(streamKey, ReadOffset.from("0"), consumerGroupName);
        }


        Subscription receive = container.receive(
                Consumer.from(consumerGroupName, UUID.randomUUID().toString()),
                StreamOffset.create(streamKey, ReadOffset.lastConsumed()),
                new MsgListener(redisTemplate)
        );
        container.start();

        //
//        container.stop();
//        receive.cancel();

        log.info("Load StreamMessageListenerContainer End...");
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("StreamMessageListenerContainer Shutdown initiated...");
            container.stop();
            receive.cancel();
            log.info("StreamMessageListenerContainer Shutdown completed.");
        }));

        return container;
    }
}
