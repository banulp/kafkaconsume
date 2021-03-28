package com.banulp.kafkaconsume;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.Map;

@Service
public class KafkaConsumeMessage {

    @Autowired
    private RestTemplate restTemplate;

    private ObjectMapper mapper = new ObjectMapper();

    @KafkaListener(topics = "dg-events", groupId = "foo")
    public void listenGroupFoo(String message) {
        System.out.println("Received Message in group foo: " + message);

        String fooResourceUrl
                = "https://api.telegram.org/bot718410719:AAH1woztS1Vma2iTdI7e_I47t4MQxGJ-Kgs/sendmessage?chat_id=-577997719&text=";

        try {
            Map map = mapper.readValue(message, Map.class);

            ResponseEntity<String> response1
                    = restTemplate.getForEntity(fooResourceUrl + map.get("region") + "-" + map.get("title"), String.class);

            ResponseEntity<String> response2
                    = restTemplate.getForEntity(fooResourceUrl + map.get("id"), String.class);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }

    }
}
