package org.lightcouch;

public class RevInfo {
    private String rev;
    private String status;

    public RevInfo() {
    }

    public RevInfo(String rev, String status) {
        this.rev = rev;
        this.status = status;
    }

    public String getRev() {
        return rev;
    }

    public String getStatus() {
        return status;
    }
}
