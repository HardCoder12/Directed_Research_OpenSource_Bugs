package com.cloud.ucs.structure;


public class UcsCookie {
    private final String cookie;
    private final Long startTime;

    public UcsCookie(String cookie, Long startTime) {
        this.cookie = cookie;
        this.startTime = startTime;
    }

    public String getCookie() {
        return cookie;
    }

    public Long getStartTime() {
        return startTime;
    }
    

}
