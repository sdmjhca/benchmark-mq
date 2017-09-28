package com.xxx.bootstrap;

import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.boot.Banner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportResource;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.Resource;

import io.vertx.core.Vertx;

@Configuration
@EnableAutoConfiguration
@ComponentScan
@ImportResource("classpath:beans.xml")
@Component
@Scope(BeanDefinition.SCOPE_SINGLETON)
public class EntryPoint {
    @Resource
    private Vertx vertx;
    @Resource
    private ConfigurableApplicationContext context;

    private static ConfigurableApplicationContext gContext;

    public static void main(String... args) {
        SpringApplication app = new SpringApplication(EntryPoint.class);
        app.setBannerMode(Banner.Mode.OFF);
        app.run();
    }

    @PostConstruct
    private void init() {
        gContext = context;
    }

    public static <T> T getBean(Class<T> cls) {
        return gContext.getBean(cls);
    }

    public static String getEnv(String name) {
        return gContext.getEnvironment().getProperty(name);
    }

    @PreDestroy
    public void destroy() {
        vertx.close();
    }
}
