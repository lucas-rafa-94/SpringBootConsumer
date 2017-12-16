package com.conectcar.helper;


import com.google.gson.Gson;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.HashMap;

public class GetProperties {

    public static HashMap<String, JmsConnectionFactory> getProperties() {

        HashMap<String, JmsConnectionFactory> jmsProperites = new HashMap<>();

        try {
           // String domainHome = System.getenv("DOMAIN_HOME");
           // FileReader fileReader = new FileReader(domainHome + "/config/app/o2MinJndiConnection.properties");
            FileReader fileReader = new FileReader( "/Users/lucasdossantos/Desktop/deploy/o2MinJndiConnection.properties");
            BufferedReader br = new BufferedReader(fileReader);
            String linha = br.readLine();
            String resultado = "";

            Gson gson = new Gson();

            while (linha != null) {
                jmsProperites.put(linha.split("=")[0].trim(),
                        gson.fromJson(linha.split("=")[1].trim(), JmsConnectionFactory.class));
                resultado += linha;
                linha = br.readLine();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return jmsProperites;

    }


    public static HashMap<String, JmsServerConnection> getServerProperties() {

        HashMap<String, JmsServerConnection> jmsServerProperites = new HashMap<>();

        try {
            //String domainHome = System.getenv("DOMAIN_HOME");
            //FileReader fileReader = new FileReader(domainHome + "/config/app/o2MinJndiConnection.properties");
            FileReader fileReader = new FileReader( "/Users/lucasdossantos/Desktop/deploy/o2MinConnectionServerJms.properties");
            BufferedReader br = new BufferedReader(fileReader);
            String linha = br.readLine();
            String resultado = "";

            Gson gson = new Gson();

            while (linha != null) {
                jmsServerProperites.put(linha.split("=")[0].trim(),
                        gson.fromJson(linha.split("=")[1].trim(), JmsServerConnection.class));
                resultado += linha;
                linha = br.readLine();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return jmsServerProperites;
    }
}
