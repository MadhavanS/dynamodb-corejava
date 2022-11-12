package io.mad.ddb;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;

import java.net.URI;

public class DynamodbEnhancedClientCRUD {
    private final static String tblName = "MusicBean";

    public static void main(String[] args) {
        DynamoDbClient ddb = DynamoDbClient.builder()
                .endpointOverride(URI.create("http://localhost:8000"))
                .credentialsProvider(StaticCredentialsProvider
                        .create(AwsBasicCredentials.create("key", "secretkey")))
                .region(Region.US_WEST_2)
                .build();

        DynamoDbEnhancedClient client = DynamoDbEnhancedClient.builder()
                .dynamoDbClient(ddb)
                .build();
        System.out.println(client);

        long tableExist = ddb.listTables().tableNames().stream().filter(tbl -> tbl.equals(tblName))
                .count();

        if(tableExist == 0)
            createTable(client);
        putRecord(client);
    }

    private static DynamoDbTable<MusicBean> createTable(DynamoDbEnhancedClient client) {
        DynamoDbTable<MusicBean> customerTable = client.table(tblName, TableSchema.fromBean(MusicBean.class));
        // Create the table
        customerTable.createTable(builder -> builder
                .provisionedThroughput(b -> b
                        .readCapacityUnits(10L)
                        .writeCapacityUnits(10L)
                        .build())
        );
        return customerTable;
    }

    public static void putRecord(DynamoDbEnhancedClient enhancedClient) {
        MusicBean musicRecord = null;
        try {
            DynamoDbTable<MusicBean> musicTable = enhancedClient.table(tblName, TableSchema.fromBean(MusicBean.class));

            // Populate the Table.
            musicRecord = new MusicBean();
            musicRecord.setArtist("Westlife");
            musicRecord.setAlbumID("id104");
            musicRecord.setAlbum("Coast to Coast");

            // Put the customer data into an Amazon DynamoDB table.
            musicTable.putItem(musicRecord);

        } catch (DynamoDbException e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }
        System.out.println("Customer data added to the table with id " + musicRecord.getAlbumID());
    }
}
