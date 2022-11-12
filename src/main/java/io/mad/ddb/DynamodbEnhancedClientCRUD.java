package io.mad.ddb;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.Key;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.enhanced.dynamodb.model.GetItemEnhancedRequest;
import software.amazon.awssdk.enhanced.dynamodb.model.QueryConditional;
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
        addRecord(client);
//        getRecord(client, "id101", "Westlife");
        queryRecord(client, "id103");
//        updateRecord(client, "id101");
//        deleteRecord(client, "id101");
    }

    private static DynamoDbTable<MusicBean> createTable(DynamoDbEnhancedClient client) {
        DynamoDbTable<MusicBean> musicTable = client.table(tblName, TableSchema.fromBean(MusicBean.class));
        // Create the table
        musicTable.createTable(builder -> builder
                .provisionedThroughput(b -> b
                        .readCapacityUnits(10L)
                        .writeCapacityUnits(10L)
                        .build())
        );
        return musicTable;
    }

    public static void addRecord(DynamoDbEnhancedClient enhancedClient) {
        MusicBean musicRecord = null;
        try {
            DynamoDbTable<MusicBean> musicTable = enhancedClient.table(tblName, TableSchema.fromBean(MusicBean.class));

            // Populate the Table.
            musicRecord = new MusicBean();
            musicRecord.setArtist("Westlife");
            musicRecord.setAlbumID("id104");
            musicRecord.setAlbum("Turnaround");

            // Insert record into an Amazon DynamoDB table.
            musicTable.putItem(musicRecord);

        } catch (DynamoDbException e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }
        System.out.println("Customer data added to the table with id " + musicRecord.getAlbumID());
    }

    public static String getRecord(DynamoDbEnhancedClient enhancedClient, String keyVal, String sortVal) {
        MusicBean result = null;

        try {
            DynamoDbTable<MusicBean> musicTable = enhancedClient.table(tblName, TableSchema.fromBean(MusicBean.class));
            Key key = Key.builder()
                    .partitionValue(keyVal)
                    .sortValue(sortVal) // If DynamoDbSortKey is defined in Bean, then this step is mandatory
                    .build();

            // Get the item by using the key.
            result = musicTable.getItem(
                    (GetItemEnhancedRequest.Builder requestBuilder) -> requestBuilder.key(key));
            System.out.println("******* The Album name is " + result.getAlbum());

        } catch (DynamoDbException e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }
        return result.getAlbumID();
    }

    public static String updateRecord(DynamoDbEnhancedClient enhancedClient, String keyVal) {
        try {
            DynamoDbTable<MusicBean> musicTable = enhancedClient.table(tblName, TableSchema.fromBean(MusicBean.class));
            Key key = Key.builder()
                    .partitionValue(keyVal)
                    .build();

            // Get the item by using the key and update.
            MusicBean item = musicTable.getItem(record -> record.key(key));
            item.setAlbum("Westlife");
            musicTable.updateItem(item);
            return item.getAlbum();

        } catch (DynamoDbException e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }
        return "";
    }

    public static String deleteRecord(DynamoDbEnhancedClient enhancedClient, String keyVal) {
        try {
            DynamoDbTable<MusicBean> musicTable = enhancedClient.table(tblName, TableSchema.fromBean(MusicBean.class));
            Key key = Key.builder()
                    .partitionValue(keyVal)
                    .build();

            // Get the item by using the key and update the email value.
            MusicBean item = musicTable.getItem(record -> record.key(key));
            musicTable.deleteItem(item);
            return item.getAlbum();

        } catch (DynamoDbException e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }
        return "";
    }

    public static String queryRecord(DynamoDbEnhancedClient enhancedClient, String recordId) {
        try{
            DynamoDbTable<MusicBean> mappedTable = enhancedClient.table(tblName, TableSchema.fromBean(MusicBean.class));
            QueryConditional queryConditional = QueryConditional.keyEqualTo(Key.builder()
                    .partitionValue(recordId)
                    .build());

            mappedTable.query(queryConditional)
                    .items().stream()
                    .findAny().ifPresent(bean -> System.out.println("QueryRecord - AlbumID: " + bean.getAlbum()));

        } catch (DynamoDbException e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }
        return "";
    }
}
