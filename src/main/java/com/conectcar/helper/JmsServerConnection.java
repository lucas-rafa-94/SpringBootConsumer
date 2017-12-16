package com.conectcar.helper;
public class JmsServerConnection {

    private String url, contextFactory;

    public JmsServerConnection(String url, String contextFactory) {
        super();
        this.url = url;
        this.contextFactory = contextFactory;

    }

    public JmsServerConnection() {
        super();
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getContextFactory() {
        return contextFactory;
    }

    public void setContextFactory(String contextFactory) {
        this.contextFactory = contextFactory;
    }



}

