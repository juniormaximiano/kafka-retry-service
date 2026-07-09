package com.juno.kafkaretryservice;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.retry.annotation.EnableRetry;


@SpringBootApplication
@EnableRetry
public class KafkaRetryServiceApplication {

	public static void main(String[] args) {
		SpringApplication.run(KafkaRetryServiceApplication.class, args);
	}

}
