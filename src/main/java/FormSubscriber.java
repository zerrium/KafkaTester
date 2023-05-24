import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import javax.swing.*;
import javax.swing.table.DefaultTableModel;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;
import java.util.List;
import java.util.Properties;

public class FormSubscriber {
    private JPanel jPanel1;
    private JTable table1;
    private static Timer timer = new Timer(100, new ActionListener() {
        @Override
        public void actionPerformed(ActionEvent e) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(50));
            for (ConsumerRecord<String, String> record : records) {
                dtm.insertRow(0, new Object[]{sdf.format(new Date()), record.key(), record.value()});
            }
        }
    });
    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm.SSS");
    private static final String topicName = "Tes";
    private static KafkaConsumer<String, String> consumer;
    private static final DefaultTableModel dtm = new DefaultTableModel(new Object[]{"", "", ""}, 1);

    public static void main(String[] args) {
        initKafka();
        SwingUtilities.invokeLater(() -> {
            JFrame frame = new JFrame("Subscriber Kafka");
            frame.setContentPane(new FormSubscriber().jPanel1);
            frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
            frame.pack();
            frame.setVisible(true);
            timer.start();
        });
    }

    private static void initKafka() {
        Properties props = new Properties();

        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "test");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        consumer = new KafkaConsumer<>(props);

        //Kafka Consumer subscribes list of topics here.
        consumer.subscribe(List.of(topicName));
    }

    private void createUIComponents() {
        table1 = new JTable(dtm);
        table1.getColumnModel().getColumn(0).setPreferredWidth(50);
        table1.getColumnModel().getColumn(1).setPreferredWidth(50);
        table1.getColumnModel().getColumn(2).setPreferredWidth(280);
    }
}
