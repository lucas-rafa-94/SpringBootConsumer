package com.conectcar.helper;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.jms.*;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import java.util.HashMap;
import java.util.Hashtable;


public class SendMessageFactory {

    private static final Log log = LogFactory.getFactory().getInstance(SendMessageFactory.class);

    private static  HashMap<String, JmsServerConnection> jmsServerProperites = GetProperties.getServerProperties();
    private static  HashMap<String, JmsConnectionFactory> jmsProperites = GetProperties.getProperties();
    private static  JmsServerConnection jmsServerConnection = null;
    private static  JmsConnectionFactory jmsConnectionFactory = null;
    private static QueueConnectionFactory qconFactory;
    private static QueueConnection qcon;
    private static  QueueSession qsession;
    private static QueueSender qsender;
    private static Queue queue;
    private static BytesMessage msg;

    public static void init(Context ctx, String queueName) throws NamingException, JMSException {

        qconFactory = (QueueConnectionFactory) ctx.lookup(jmsConnectionFactory.getJmsFactory());
        qcon = qconFactory.createQueueConnection();
        qsession = qcon.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
        queue = (Queue) ctx.lookup(queueName);
        qsender = qsession.createSender(queue);
        msg = qsession.createBytesMessage();
        qcon.start();
    }

    public static  void send(BytesMessage message) throws JMSException {
        qsender.send(message);
    }

    public static  void close() throws JMSException {
        qsender.close();
        qsession.close();
        qcon.close();
    }
    private static InitialContext getInitialContext(String url)
            throws NamingException
    {
        Hashtable env = new Hashtable();
        env.put(Context.INITIAL_CONTEXT_FACTORY, jmsServerConnection.getContextFactory());
        env.put(Context.PROVIDER_URL, url);
        return new InitialContext(env);
    }

    public static void sendToQueue(BytesMessage bytesMessage, String fluxo) {

        try {
            jmsServerConnection = jmsServerProperites.get("Local");
            jmsConnectionFactory = jmsProperites.get(fluxo);
            InitialContext ic = getInitialContext(jmsServerConnection.getUrl());
            init(ic, jmsConnectionFactory.getJmsQueue());
            send(bytesMessage);
            close();

        } catch (JMSException | NamingException e) {
            log.error("sendToQueue", e);
        } catch (Exception e){
            log.error("sendToQueue", e);
        }

    }
}
