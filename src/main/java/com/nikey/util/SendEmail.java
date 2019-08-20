package com.nikey.util;

import java.util.Properties;
import java.util.concurrent.Callable;

import javax.mail.Authenticator;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

public class SendEmail {

    private static int port = 465; // SSL port
    private static final String SSL_FACTORY = "javax.net.ssl.SSLSocketFactory";

    // Recipient's email ID needs to be mentioned.
    private static final String toLocal = PropUtil.getString("send_to");

    // Sender's email ID needs to be mentioned
    private static final String from = PropUtil.getString("send_from");

    // Assuming you are sending email from localhost
    private static final String host = PropUtil.getString("send_host");

    private static final String password = PropUtil.getString("mail_password");

    // Get system properties
    private static final Properties properties = new Properties();

    static {
        properties.put("mail.smtp.auth", "true");
        properties.setProperty("mail.smtp.host", host);
        properties.put("mail.smtp.socketFactory.class", SSL_FACTORY); // 使用JSSE的SSLsocketfactory来取代默认的socketfactory
        properties.put("mail.smtp.socketFactory.fallback", "false"); // 只处理SSL的连接,对于非SSL的连接不做处理
        properties.put("mail.smtp.port", port);
        properties.put("mail.smtp.socketFactory.port", port);
    }

    // Get the default Session object.
    private static final Session session = Session.getInstance(properties,
            new Authenticator() {
                @Override
                protected PasswordAuthentication getPasswordAuthentication() {
                    return new PasswordAuthentication(from, password);
                }
            });

    public static void main(String[] args) {
        sendMail("mail_test", "hello, world");
    }

    public static void sendMail(final String subject, final String text) {
        Callable<String> task = new Callable<String>() {

            @Override
            public String call() throws Exception {
                try {
                    // Create a default MimeMessage object.
                    MimeMessage message = new MimeMessage(session);

                    // Set From: header field of the header.
                    message.setFrom(new InternetAddress(from));

                    // Set To: header field of the header.
                    String[] toStr = toLocal.split(",");
                    for (String to : toStr) {
                        message.addRecipient(Message.RecipientType.TO,
                                new InternetAddress(to));
                    }

                    // Set Subject: header field
                    message.setSubject(subject);

                    // Now set the actual message
                    message.setText(text);

                    // Send message
                    Transport transport = session.getTransport("smtps");
                    transport.connect(host, from, password);
                    transport.sendMessage(message, message.getAllRecipients());
                    transport.close();
                } catch (MessagingException mex) {
                    mex.printStackTrace();
                    LogJsonUtil.errorJsonFileRecord(
                            SendEmail.class.getSimpleName(), mex.getMessage(),
                            "subject: " + subject + "\ntext: " + text);
                }
                return "done";
            }

        };

        JobControllUtil.submitJob(task, "subject: " + subject + "\ntext: "
                + text, SendEmail.class.getSimpleName());
    }

}
