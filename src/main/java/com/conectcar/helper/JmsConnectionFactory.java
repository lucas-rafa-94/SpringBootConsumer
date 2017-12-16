package com.conectcar.helper;


public class JmsConnectionFactory {

    private String username, password, jmsQueue, jmsFactory;

    public JmsConnectionFactory(String username, String password, String jmsQueue, String jmsFactory) {
        super();
        this.username = username;
        this.password = password;
        this.jmsQueue = jmsQueue;
        this.jmsFactory = jmsFactory;
    }

    public JmsConnectionFactory() {
        super();
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getJmsQueue() {
        return jmsQueue;
    }

    public void setJmsQueue(String jmsQueue) {
        this.jmsQueue = jmsQueue;
    }

    public String getJmsFactory() {
        return jmsFactory;
    }

    public void setJmsFactory(String jmsFactory) {
        this.jmsFactory = jmsFactory;
    }

}
