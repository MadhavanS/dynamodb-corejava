package io.mad.ddb;

import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbBean;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbPartitionKey;

@DynamoDbBean
public class MusicBean {
    private String albumID;
    private String album;
    private String artist;

    @DynamoDbPartitionKey
    public String getAlbumID() { return albumID; }

    public void setAlbumID(String albumID) { this.albumID = albumID; }


    public String getAlbum() { return album; }
    public void setAlbum(String album) { this.album = album; }


    public String getArtist() { return artist; }
    public void setArtist(String artist) { this.artist = artist; }
}

