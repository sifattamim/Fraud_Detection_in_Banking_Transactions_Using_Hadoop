package com.pack;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import scala.Tuple2;

public class CCFraudDetectionSystem {

    public static void main(String[] args) throws Exception {

        // Check if required arguments are passed
        if (args.length < 5) {
            System.err.println("Usage: CCFraudDetectionSystem <broker> <topic> <groupId> <zipCodeCsvPath> <hbaseMaster>");
            System.exit(1);
        }

        String broker = args[0];
        String topic = args[1];
        String groupId = args[2];
        String zipCodeCsvPath = args[3];
        String hbaseMaster = args[4];

        // Set up Spark Streaming context (1-second batch interval)
        SparkConf conf = new SparkConf().setAppName("CreditCardFraudDetection");
        JavaStreamingContext streamingContext = new JavaStreamingContext(conf, Durations.seconds(1));
        streamingContext.sparkContext().setLogLevel("WARN");

        // Kafka topic set
        Set<String> topics = new HashSet<>(Collections.singletonList(topic));

        // Kafka Consumer configuration
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
        kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        kafkaParams.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        kafkaParams.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Create Kafka DStream
        JavaInputDStream<ConsumerRecord<String, JsonNode>> kafkaStream = KafkaUtils.createDirectStream(
                streamingContext,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(topics, kafkaParams)
        );

        // Parse each Kafka message into TransactionData using Jackson
        JavaDStream<TransactionData> transactionStream = kafkaStream.map(record -> {
            ObjectMapper mapper = new ObjectMapper();
            return mapper.treeToValue(record.value(), TransactionData.class);
        });

        // Load Distance Utility (singleton pattern)
        DistanceUtility distanceUtil = DistanceUtility.GetInstance(zipCodeCsvPath);

        // Fraud detection logic
        transactionStream.foreachRDD((JavaRDD<TransactionData> rdd) -> {
            JavaPairRDD<TransactionData, String> statusRDD = rdd.mapToPair((PairFunction<TransactionData, TransactionData, String>) txn -> {

                // Fetch last transaction info from HBase lookup
                TransactionLookupRecord lastRecord = HbaseClient.getTransactionLookupRecord(txn, hbaseMaster);

                // Calculate time difference and speed
                double distance = distanceUtil.getDistanceViaZipCode(
                        txn.getPostcode().toString(),
                        lastRecord.getPostcode().toString()
                );

                SimpleDateFormat sdf = new SimpleDateFormat("dd-MM-yyyy HH:mm:ss");
                long timeDiffSec = (sdf.parse(txn.getTransactionDate()).getTime() - sdf.parse(lastRecord.getTransactionDate()).getTime()) / 1000;
                double speed = (timeDiffSec > 0) ? distance / timeDiffSec : Double.MAX_VALUE;

                // Define fraud rules
                boolean isFraud = lastRecord.getScore() < 200 ||
                                  txn.getAmount() > lastRecord.getUcl() ||
                                  speed > 0.25;

                String status = isFraud ? "FRAUD" : "GENUINE";

                // Insert transaction into HBase
                HbaseClient.InsertTransactionIntoDB(txn, hbaseMaster, status);

                return new Tuple2<>(txn, status);
            });

            // Print results
            statusRDD.collect().forEach(entry -> {
                System.out.println(entry._1 + " --> " + entry._2);
            });
        });

        streamingContext.start();
        streamingContext.awaitTermination();
    }
}
