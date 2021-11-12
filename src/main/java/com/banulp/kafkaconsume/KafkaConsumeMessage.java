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
//        System.out.println("Received Message in group foo: " + message);

        String fooResourceUrl
                = "https://api.telegram.org/bot718410719:AAH1woztS1Vma2iTdI7e_I47t4MQxGJ-Kgs/sendmessage?chat_id=-577997719&text=";

//        https://api.telegram.org/bot2105340863:AAFhExWwNhPOCjjiNnVF6ibE_1us7HDgszA/sendmessage?chat_id=63447517&text=%EC%83%88%EB%A1%AD%EA%B2%8C%ED%95%98%EC%86%8C%EC%84%9C


        try {
            Map map = mapper.readValue(message, Map.class);

            ResponseEntity<String> response
                    = restTemplate.getForEntity(fooResourceUrl + map.get("region") + "-" + map.get("title") + " " + "https://www.daangn.com/articles/" + map.get("id"), String.class);

        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }

    }
}
