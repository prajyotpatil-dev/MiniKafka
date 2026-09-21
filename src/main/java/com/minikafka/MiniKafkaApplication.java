package com.minikafka;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Entry point application class for MiniKafka.
 */
@SpringBootApplication
public class MiniKafkaApplication {

    public static void main(String[] args) {
        SpringApplication.run(MiniKafkaApplication.class, args);
    }
}
