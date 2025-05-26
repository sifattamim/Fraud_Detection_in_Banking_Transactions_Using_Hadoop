package com.pack;

import java.io.IOException;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseClient {

    private static Connection hBaseConnection;
    private static final String MASTER_TABLE = "card_transactions_master";
    private static final String LOOKUP_TABLE = "card_transaction_lookup";
    private static final String COLUMN_FAMILY_CF = "cf";
    private static final String COLUMN_FAMILY_CARD = "cardDetail";
    private static final String COLUMN_FAMILY_TRANSACTION = "transactionDetail";

    public static Connection getConnection(String hBaseMaster) throws IOException {
        if (hBaseConnection == null || hBaseConnection.isClosed()) {
            Configuration config = HBaseConfiguration.create();
            config.setInt("timeout", 1200);
            config.set("hbase.master", hBaseMaster + ":60000");
            config.set("hbase.zookeeper.quorum", hBaseMaster);
            config.set("hbase.zookeeper.property.clientPort", "2181");
            config.set("zookeeper.znode.parent", "/hbase");

            hBaseConnection = ConnectionFactory.createConnection(config);
        }
        return hBaseConnection;
    }

    public static TransactionLookupRecord fetchLookupRecord(TransactionData tx, String hBaseMaster) throws IOException {
        try (Connection connection = getConnection(hBaseMaster);
             Table table = connection.getTable(TableName.valueOf(LOOKUP_TABLE))) {

            Get get = new Get(Bytes.toBytes(tx.getCardId().toString()));
            Result result = table.get(get);

            TransactionLookupRecord record = new TransactionLookupRecord(tx.getCardId());

            byte[] val;
            val = result.getValue(Bytes.toBytes(COLUMN_FAMILY_CF), Bytes.toBytes("ucl"));
            if (val != null) record.setUcl(Double.parseDouble(Bytes.toString(val)));

            val = result.getValue(Bytes.toBytes(COLUMN_FAMILY_CF), Bytes.toBytes("score"));
            if (val != null) record.setScore(Integer.parseInt(Bytes.toString(val)));

            val = result.getValue(Bytes.toBytes(COLUMN_FAMILY_CF), Bytes.toBytes("postcode"));
            if (val != null) record.setPostcode(Integer.parseInt(Bytes.toString(val)));

            val = result.getValue(Bytes.toBytes(COLUMN_FAMILY_CF), Bytes.toBytes("transaction_dt"));
            if (val != null) record.setTransactionDate(Bytes.toString(val));

            return record;

        } catch (Exception e) {
            System.err.println("Error fetching transaction lookup record: " + e.getMessage());
            e.printStackTrace();
        }
        return null;
    }

    public static void insertTransaction(TransactionData tx, String hBaseMaster, String status) throws IOException {
        try (Connection connection = getConnection(hBaseMaster);
             Table masterTable = connection.getTable(TableName.valueOf(MASTER_TABLE))) {

            String guid = UUID.randomUUID().toString().replace("-", "");
            Put put = new Put(Bytes.toBytes(guid));

            put.addColumn(Bytes.toBytes(COLUMN_FAMILY_CARD), Bytes.toBytes("card_id"), Bytes.toBytes(tx.getCardId().toString()));
            put.addColumn(Bytes.toBytes(COLUMN_FAMILY_CARD), Bytes.toBytes("member_id"), Bytes.toBytes(tx.getMemberId().toString()));
            put.addColumn(Bytes.toBytes(COLUMN_FAMILY_TRANSACTION), Bytes.toBytes("amount"), Bytes.toBytes(tx.getAmount().toString()));
            put.addColumn(Bytes.toBytes(COLUMN_FAMILY_TRANSACTION), Bytes.toBytes("postcode"), Bytes.toBytes(tx.getPostcode().toString()));
            put.addColumn(Bytes.toBytes(COLUMN_FAMILY_TRANSACTION), Bytes.toBytes("pos_id"), Bytes.toBytes(tx.getPosId().toString()));
            put.addColumn(Bytes.toBytes(COLUMN_FAMILY_TRANSACTION), Bytes.toBytes("transaction_dt"), Bytes.toBytes(tx.getTransactionDate()));
            put.addColumn(Bytes.toBytes(COLUMN_FAMILY_TRANSACTION), Bytes.toBytes("status"), Bytes.toBytes(status));

            masterTable.put(put);

            if ("GENUINE".equalsIgnoreCase(status)) {
                try (Table lookupTable = connection.getTable(TableName.valueOf(LOOKUP_TABLE))) {
                    Put lookupPut = new Put(Bytes.toBytes(tx.getCardId().toString()));
                    lookupPut.addColumn(Bytes.toBytes(COLUMN_FAMILY_CF), Bytes.toBytes("postcode"), Bytes.toBytes(tx.getPostcode().toString()));
                    lookupPut.addColumn(Bytes.toBytes(COLUMN_FAMILY_CF), Bytes.toBytes("transaction_dt"), Bytes.toBytes(tx.getTransactionDate()));
                    lookupTable.put(lookupPut);
                }
            }

        } catch (Exception e) {
            System.err.println("Error inserting transaction: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
