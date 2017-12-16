package com.conectcar.consumer;

import com.conectcar.helper.SendMessageFactory;

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;

public class ConsumerConectCarFC implements MessageListener {
    public void onMessage(Message message) {
        if (message instanceof BytesMessage) {
            try {
                SendMessageFactory.sendToQueue((BytesMessage) message, "FalhaComunicacaoLocalOSA31012");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
