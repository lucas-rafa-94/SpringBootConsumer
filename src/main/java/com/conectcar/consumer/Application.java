package com.conectcar.consumer;


import com.conectcar.helper.GetProperties;
import com.conectcar.helper.JmsConnectionFactory;
import com.conectcar.helper.JmsServerConnection;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.support.SpringBootServletInitializer;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.EnableRetry;
import org.springframework.retry.annotation.Retryable;
import org.springframework.web.WebApplicationInitializer;

import javax.jms.*;
import javax.naming.Context;
import javax.naming.InitialContext;
import java.util.HashMap;
import java.util.Properties;


@ComponentScan
@EnableRetry
@SpringBootApplication
public class Application extends SpringBootServletInitializer implements WebApplicationInitializer {
    private static final Log log = LogFactory.getFactory().getInstance(Application.class);

    private static Properties env =null;
    private static final String CONECTCAR_EXTERNAL_CLIENT = "conectcar-external-client";
    public static Context context;

    private static JmsConnectionFactory jmsConnectionFactory = null;
    private static JmsServerConnection jmsServerConnection = null;
    private static HashMap<String, JmsServerConnection> jmsServerProperites = GetProperties.getServerProperties();
    private static  HashMap<String, JmsConnectionFactory> jmsProperites = GetProperties.getProperties();


    public static ConnectionFactory connectionFactory()  {

        ConnectionFactory connectionFactory = null;

        env = new Properties();
        env.put(Context.INITIAL_CONTEXT_FACTORY, jmsServerConnection.getContextFactory());
        env.put(Context.PROVIDER_URL, jmsServerConnection.getUrl());
        env.put(Context.SECURITY_PRINCIPAL, jmsConnectionFactory.getUsername());
        env.put(Context.SECURITY_CREDENTIALS,jmsConnectionFactory.getPassword());
        env.put("jboss.naming.client.ejb.context", true);
        env.put(Context.URL_PKG_PREFIXES, "org.jboss.ejb.client.naming");
        env.put("invocation.timeout", 3000);
        try {
            context = new InitialContext(env);
            connectionFactory =
                    (ConnectionFactory) context.lookup(jmsConnectionFactory.getJmsFactory());
        }catch (Exception e){
            e.printStackTrace();
        }
        return connectionFactory;
    }

    @Retryable(
            value = { Exception.class },
            maxAttempts = 2,
            backoff = @Backoff(delay = 5000))
    public static void startConnectionFactory(String fluxo) throws Exception{

        System.out.println("Conectando " + fluxo);
        //log.debug("Conectando " + fluxo);

        ConnectionFactory connectionFactory = connectionFactory();
        Connection connection = null;

        Destination destination = (Destination) context.lookup(jmsConnectionFactory.getJmsQueue());

        connection = connectionFactory.createConnection(System.getProperty("username", jmsConnectionFactory.getUsername()), System.getProperty("password", jmsConnectionFactory.getPassword()));
        //connection.setClientID("test-client" + String.format("%1$d", System.currentTimeMillis()));
        connection.setClientID(fluxo);


        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        TopicSubscriber topicSubscriber = session.createDurableSubscriber((Topic) destination, CONECTCAR_EXTERNAL_CLIENT);

        if(fluxo.equals("PassagemProcessadaRemoteOSA31012")){
            topicSubscriber.setMessageListener(new ConsumerConectCarPP());
        }else if (fluxo.equals("RequisitaImagemRemoteOSA31012")){
            topicSubscriber.setMessageListener(new ConsumerConectCarRI());
        }else{
            topicSubscriber.setMessageListener(new ConsumerConectCarFC());
        }

    }

    public static void main(String[] args) throws Exception {
        SpringApplication.run(Application.class, args);


        Runnable passagemProcessadaConsumer = () -> {
            try {
                jmsConnectionFactory = jmsProperites.get("PassagemProcessadaRemoteOSA31012");
                jmsServerConnection = jmsServerProperites.get("OSA3");
                startConnectionFactory("PassagemProcessadaRemoteOSA31012");
            }catch (Exception e){

                e.printStackTrace();
            }

        };


        Runnable requisitaImagemConsumer = () -> {
            try {
                jmsConnectionFactory = jmsProperites.get("RequisitaImagemRemoteOSA31012");
                jmsServerConnection = jmsServerProperites.get("OSA3");
                startConnectionFactory("RequisitaImagemRemoteOSA31012");
            }catch (Exception e){
                e.printStackTrace();
            }

        };

        Runnable falhaConsumer = () -> {
            try {
                jmsConnectionFactory = jmsProperites.get("FalhaComunicacaoRemoteOSA31012");
                jmsServerConnection = jmsServerProperites.get("OSA3");
                startConnectionFactory("FalhaComunicacaoRemoteOSA31012");
            }catch (Exception e){
                e.printStackTrace();
            }

        };

        passagemProcessadaConsumer.run();
        requisitaImagemConsumer.run();
        falhaConsumer.run();


    }
    @Override
    protected SpringApplicationBuilder configure(SpringApplicationBuilder builder) {
        return builder.sources(Application.class);
    }
}
