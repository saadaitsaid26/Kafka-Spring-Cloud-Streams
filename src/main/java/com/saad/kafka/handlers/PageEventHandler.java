package com.saad.kafka.handlers;

import com.saad.kafka.events.PageEvent;
import java.util.Date;
import java.util.Random;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class PageEventHandler {

    private static final Logger log = LoggerFactory.getLogger(PageEventHandler.class);
    private static final Random RANDOM = new Random();

    @Bean
    public Consumer<PageEvent> pageEventConsumer() {
        return input -> log.info("****************** {}", input);
    }

    @Bean
    public Supplier<PageEvent> pageEventSupplier() {
        return () -> {
            String page = Math.random() > 0.5 ? "P1" : "P2";
            String user = Math.random() > 0.5 ? "U1" : "U2";
            long duration = 10 + RANDOM.nextInt(10000);
            return new PageEvent(page, user, new Date(), duration);
        };
    }
}