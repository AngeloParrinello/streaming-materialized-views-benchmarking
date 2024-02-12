package it.agilelab.thesis.nexmark.flink;

import com.j256.simplejmx.client.JmxClient;

import javax.management.JMException;
import javax.management.ObjectName;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

public final class FlinkJMXClient {

    private final String hostName;
    private final int port;

    private FlinkJMXClient(final String hostName, final int port) {
        this.hostName = hostName;
        this.port = port;
    }

    public static void main(final String[] args) throws JMException {
        JmxClient client = new JmxClient("localhost", 1099);
        Set<ObjectName> objectNames = client.getBeanNames();
        // ObjectName objectName = objectNames.stream().filter(name -> name.toString().contains("org.apache.flink.taskmanager.job.task.operator.numRecordsOut:job_id=c47d9d5d5800ee94338f1588b7cb80e3")).findFirst().get();
        // System.out.println(client.getAttribute(objectName, "Count"));

        var executor = Executors.newSingleThreadScheduledExecutor();
        Pattern pattern = Pattern.compile("io.confluent.ksql.metrics:type=consumer-metrics,key=bids,id=_confluent-ksql-nexmark_query_CTAS_NEXMARK_Q1");
        AtomicBoolean isZero = new AtomicBoolean(false);

        /*executor.schedule(() -> {
            try {
                if (isZero.get()) {
                    List<ObjectName> list = objectNames.stream().filter(name -> name.toString().contains(pattern.pattern())).collect(Collectors.toList());
                    list.forEach(name -> {
                        try {
                            System.out.println("FINISHED");
                            System.out.println("Attribute: " + name);
                            System.out.println(client.getAttribute(name, name.getCanonicalName()));
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    });
                    // stop the executor
                    executor.shutdownNow();
                } else {
                    objectNames.stream()
                            .filter(name -> name.toString().contains(pattern.pattern()))
                            .forEach(name -> {
                                System.out.println(name);
                                try {
                                    Arrays.stream(client.getAttributesInfo(name)).forEach(attribute -> {
                                        System.out.println(attribute.getName());
                                        if (attribute.getName().equals("consumer-messages-per-sec")) {
                                            try {
                                                if (client.getAttribute(name, attribute.getName()).equals(0.0)) {
                                                    isZero.set(true);
                                                }
                                            } catch (Exception e) {
                                                throw new RuntimeException(e);
                                            }
                                        }
                                        try {
                                            System.out.println(client.getAttribute(name, attribute.getName()));
                                        } catch (Exception e) {
                                            throw new RuntimeException(e);
                                        }
                                    });
                                } catch (JMException e) {
                                    throw new RuntimeException(e);
                                }
                            });
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, 1, java.util.concurrent.TimeUnit.SECONDS);*/



        /*for (ObjectName name : objectNames) {
            MBeanAttributeInfo[] attributes = client.getAttributesInfo(name);
            MBeanOperationInfo[] operations = client.getOperationsInfo(name);



            try {
                FileWriter myWriter = new FileWriter("flink_jmx.txt", true);
                System.out.println("Name: " + name);
                myWriter.write("Name: " + name + "\n");
                System.out.println("Attributes: " + attributes.length);
                System.out.println("Operations: " + operations.length);
                myWriter.write("Attributes: " + attributes.length + "\n");
                myWriter.write("Operations: " + operations.length + "\n");
                for (MBeanAttributeInfo attribute : attributes) {

                    System.out.println("Attribute: " + attribute.getName());
                    myWriter.write("Attribute: " + attribute.getName() + "\n");
                }
                for (MBeanOperationInfo operation : operations) {
                    System.out.println("Operation: " + operation.getName());
                    myWriter.write("Operation: " + operation.getName() + "\n");
                }
                myWriter.close();
            } catch (IOException e) {
                System.out.println("An error occurred.");
                e.printStackTrace();
            }
        }*/
    }
}
