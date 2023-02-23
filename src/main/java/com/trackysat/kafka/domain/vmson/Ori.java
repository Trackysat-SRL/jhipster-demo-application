package com.trackysat.kafka.domain.vmson;

public class Ori {

    String uid;
    String cla;
    String ver;
    String typ;
    Inf inf;

    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    public String getCla() {
        return cla;
    }

    public void setCla(String cla) {
        this.cla = cla;
    }

    public String getVer() {
        return ver;
    }

    public void setVer(String ver) {
        this.ver = ver;
    }

    public String getTyp() {
        return typ;
    }

    public void setTyp(String typ) {
        this.typ = typ;
    }

    public Inf getInf() {
        return inf;
    }

    public void setInf(Inf inf) {
        this.inf = inf;
    }
}
