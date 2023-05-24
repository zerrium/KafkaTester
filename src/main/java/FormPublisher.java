import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.swing.*;
import javax.swing.event.DocumentEvent;
import javax.swing.event.DocumentListener;
import java.util.Properties;

public class FormPublisher {
    private JPanel jPanel1;
    private JTextArea textArea1;
    private JTextField textField1;
    private JButton sendButton;
    private static final String topicName = "Tes";
    private static Producer<String, String> producer;

    public FormPublisher() {
        sendButton.addActionListener(e -> {
            producer.send(new ProducerRecord<>(topicName, textField1.getText(), textArea1.getText()));
            textArea1.setText("");
            textField1.setText("");
            sendButton.setEnabled(false);
        });
        textField1.getDocument().addDocumentListener(new DocumentListener() {
            @Override
            public void insertUpdate(DocumentEvent e) {
                sendButton.setEnabled(!textArea1.getText().isEmpty() && !textField1.getText().isEmpty());
            }

            @Override
            public void removeUpdate(DocumentEvent e) {
                sendButton.setEnabled(!textArea1.getText().isEmpty() && !textField1.getText().isEmpty());
            }

            @Override
            public void changedUpdate(DocumentEvent e) {
                sendButton.setEnabled(!textArea1.getText().isEmpty() && !textField1.getText().isEmpty());
            }
        });
        textArea1.getDocument().addDocumentListener(new DocumentListener() {
            @Override
            public void insertUpdate(DocumentEvent e) {
                sendButton.setEnabled(!textArea1.getText().isEmpty() && !textField1.getText().isEmpty());
            }

            @Override
            public void removeUpdate(DocumentEvent e) {
                sendButton.setEnabled(!textArea1.getText().isEmpty() && !textField1.getText().isEmpty());
            }

            @Override
            public void changedUpdate(DocumentEvent e) {
                sendButton.setEnabled(!textArea1.getText().isEmpty() && !textField1.getText().isEmpty());
            }
        });
    }

    public static void main(String[] args) {
        initKafka();
        JFrame frame = new JFrame("Publisher Kafka");
        frame.setContentPane(new FormPublisher().jPanel1);
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        frame.pack();
        frame.setVisible(true);
    }

    private static void initKafka() {
        // create instance for properties to access producer configs
        Properties props = new Properties();

        //Assign localhost id
        props.put("bootstrap.servers", "localhost:9092");

        //Set acknowledgements for producer requests.
        props.put("acks", "all");

        //If the request fails, the producer can automatically retry,
        props.put("retries", 0);

        //Specify buffer size in config
        props.put("batch.size", 16384);

        //Reduce the no of requests less than 0
        props.put("linger.ms", 1);

        //The buffer.memory controls the total amount of memory available to the producer for buffering.
        props.put("buffer.memory", 33554432);

        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        producer = new KafkaProducer<>(props);
    }
}
