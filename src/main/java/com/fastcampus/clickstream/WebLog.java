package com.fastcampus.clickstream;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;

public class WebLog {
    // Log Generator에서 넣어준 7가지 데이터를 저장 (ip주소, 타임스탬프정보, http메소드, url, 응답코드, 응답시간, 세션아이디)
    private String ipAddr;
    private Long timestamp;
    private String method;
    private String url;
    private String responseCode;
    private String responseTime;
    private String sessionId;

    // 생성자를 통해 값을 넣어준다
    public WebLog(String ipAddr, Long timestamp, String method, String url, String responseCode, String responseTime, String sessionId) {
        this.ipAddr = ipAddr;
        this.timestamp = timestamp;
        this.method = method;
        this.url = url;
        this.responseCode = responseCode;
        this.responseTime = responseTime;
        this.sessionId = sessionId;
    }

    // getter, setter 추가
    public String getIpAddr() {
        return ipAddr;
    }

    public void setIpAddr(String ipAddr) {
        this.ipAddr = ipAddr;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public String getMethod() {
        return method;
    }

    public void setMethod(String method) {
        this.method = method;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getResponseCode() {
        return responseCode;
    }

    public void setResponseCode(String responseCode) {
        this.responseCode = responseCode;
    }

    public String getResponseTime() {
        return responseTime;
    }

    public void setResponseTime(String responseTime) {
        this.responseTime = responseTime;
    }

    public String getSessionId() {
        return sessionId;
    }

    public void setSessionId(String sessionId) {
        this.sessionId = sessionId;
    }

    @Override
    public String toString() {
        OffsetDateTime offsetDateTime = OffsetDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneId.systemDefault());
        // kafka부터 들어오는 정보들을 출력함
        return String.format("WebLog(ipAddr=%s, timestamp=%s, method=%s, responseCode=%s, responseTime=%s, sessionId=%s",
                ipAddr, offsetDateTime, method, responseCode, responseTime, sessionId);
    }
}
