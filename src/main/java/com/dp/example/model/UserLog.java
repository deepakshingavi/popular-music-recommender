package com.dp.example.model;

import java.sql.Timestamp;
import java.util.Objects;

public class UserLog {

    private String userId;
    private Timestamp loginTs;
    private String artistId;
    private String aritstName;
    private String trackId;
    private String trackName;

    //Spark's Encoders needs a empty constructor along with setter methods
    public UserLog(){

    }

    public UserLog(String userId, Timestamp loginTs, String artistId, String aritstName, String trackId, String trackName) {
        this.userId = userId;
        this.loginTs = loginTs;
        this.artistId = artistId;
        this.aritstName = aritstName;
        this.trackId = trackId;
        this.trackName = trackName;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public Timestamp getLoginTs() {
        return loginTs;
    }

    public void setLoginTs(Timestamp loginTs) {
        this.loginTs = loginTs;
    }

    public String getAritstName() {
        return aritstName;
    }

    public void setAritstName(String aritstName) {
        this.aritstName = aritstName;
    }

    public String getTrackId() {
        return trackId;
    }

    public void setTrackId(String trackId) {
        this.trackId = trackId;
    }

    public String getTrackName() {
        return trackName;
    }

    public void setTrackName(String trackName) {
        this.trackName = trackName;
    }

    public String getArtistId() {
        return artistId;
    }

    public void setArtistId(String artistId) {
        this.artistId = artistId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        UserLog userLog = (UserLog) o;
        return Objects.equals(userId, userLog.userId) &&
                Objects.equals(loginTs, userLog.loginTs) &&
                Objects.equals(artistId, userLog.artistId) &&
                Objects.equals(aritstName, userLog.aritstName) &&
                Objects.equals(trackId, userLog.trackId) &&
                Objects.equals(trackName, userLog.trackName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(userId, loginTs, artistId, aritstName, trackId, trackName);
    }


    @Override
    public String toString() {
        return "UserLog{" +
                "userId='" + userId + '\'' +
                ", loginTs=" + loginTs +
                ", artistId='" + artistId + '\'' +
                ", aritstName='" + aritstName + '\'' +
                ", trackId='" + trackId + '\'' +
                ", trackName='" + trackName + '\'' +
                '}';
    }
}
