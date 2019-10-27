package com.wqy.springbootes;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.annotation.RestController;

@SpringBootApplication
@RestController
public class SpringbootEsApplication {

    public static void main(String[] args) {
        SpringApplication.run(SpringbootEsApplication.class, args);
    }

}
