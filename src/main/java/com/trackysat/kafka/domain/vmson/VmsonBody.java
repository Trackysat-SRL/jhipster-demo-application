package com.trackysat.kafka.domain.vmson;

import java.util.List;

public class VmsonBody {

    String uid;
    String ver;

    Des des;
    List<Con> con;
    Ets ets;
    Ori ori;

    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    public String getVer() {
        return ver;
    }

    public void setVer(String ver) {
        this.ver = ver;
    }

    public Des getDes() {
        return des;
    }

    public void setDes(Des des) {
        this.des = des;
    }

    public List<Con> getCon() {
        return con;
    }

    public void setCon(List<Con> con) {
        this.con = con;
    }

    public Ets getEts() {
        return ets;
    }

    public void setEts(Ets ets) {
        this.ets = ets;
    }

    public Ori getOri() {
        return ori;
    }

    public void setOri(Ori ori) {
        this.ori = ori;
    }
}
