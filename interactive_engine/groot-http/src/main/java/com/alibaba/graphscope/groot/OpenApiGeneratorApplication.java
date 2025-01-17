package com.alibaba.graphscope.groot;

import com.fasterxml.jackson.databind.Module;

import org.openapitools.jackson.nullable.JsonNullableModule;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.server.WebServerFactoryCustomizer;
import org.springframework.boot.web.servlet.server.ConfigurableServletWebServerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.FullyQualifiedAnnotationBeanNameGenerator;
import org.springframework.core.env.Environment;

@SpringBootApplication(nameGenerator = FullyQualifiedAnnotationBeanNameGenerator.class)
@ComponentScan(
        basePackages = {
            "com.alibaba.graphscope.groot",
            "com.alibaba.graphscope.groot.service.api",
            "org.openapitools.configuration"
        },
        nameGenerator = FullyQualifiedAnnotationBeanNameGenerator.class)
public class OpenApiGeneratorApplication {
    @Autowired private Environment env;

    public static void main(String[] args) {
        SpringApplication.run(OpenApiGeneratorApplication.class, args);
    }

    @Bean
    public WebServerFactoryCustomizer<ConfigurableServletWebServerFactory> customizer() {
        return server -> {
            String port = env.getProperty("frontend.service.httpPort", "8080");
            server.setPort(Integer.parseInt(port));
        };
    }

    @Bean(name = "com.alibaba.graphscope.groot.OpenApiGeneratorApplication.jsonNullableModule")
    public Module jsonNullableModule() {
        return new JsonNullableModule();
    }
}
