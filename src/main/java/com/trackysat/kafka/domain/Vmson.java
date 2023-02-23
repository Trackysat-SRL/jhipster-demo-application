package com.trackysat.kafka.domain;

import java.time.Instant;
import java.util.List;

public class Vmson {

    VmsonBody vmson;

    public VmsonBody getVmson() {
        return vmson;
    }

    public void setVmson(VmsonBody vmson) {
        this.vmson = vmson;
    }
}

class VmsonBody {

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

class Des {

    String uid;
    String cla;
    String ver;
    String typ;

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
}

class Ets {

    String uid;
    Instant tst;
    String eds;

    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    public Instant getTst() {
        return tst;
    }

    public void setTst(Instant tst) {
        this.tst = tst;
    }

    public String getEds() {
        return eds;
    }

    public void setEds(String eds) {
        this.eds = eds;
    }
}

class Ori {

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

class Inf {

    String deviceId;

    public String getDeviceId() {
        return deviceId;
    }

    public void setDeviceId(String deviceId) {
        this.deviceId = deviceId;
    }
}

class Con {

    Ets ets;
    Sat sat;
    List<Sen> sen;

    public Ets getEts() {
        return ets;
    }

    public void setEts(Ets ets) {
        this.ets = ets;
    }

    public Sat getSat() {
        return sat;
    }

    public void setSat(Sat sat) {
        this.sat = sat;
    }

    public List<Sen> getSen() {
        return sen;
    }

    public void setSen(List<Sen> sen) {
        this.sen = sen;
    }
}

class Sat {

    Integer snr; //: 9,
    Double lon; //: 17.2638,
    Integer spe; //: 0,
    String fix; //: "FIX3D",
    Integer dir; //: 0,
    Integer alt; //: 69,
    String typ; //: "GPS",
    Double lat; //: 40.9584,
    String sig; //: "24-1"

    public Integer getSnr() {
        return snr;
    }

    public void setSnr(Integer snr) {
        this.snr = snr;
    }

    public Double getLon() {
        return lon;
    }

    public void setLon(Double lon) {
        this.lon = lon;
    }

    public Integer getSpe() {
        return spe;
    }

    public void setSpe(Integer spe) {
        this.spe = spe;
    }

    public String getFix() {
        return fix;
    }

    public void setFix(String fix) {
        this.fix = fix;
    }

    public Integer getDir() {
        return dir;
    }

    public void setDir(Integer dir) {
        this.dir = dir;
    }

    public Integer getAlt() {
        return alt;
    }

    public void setAlt(Integer alt) {
        this.alt = alt;
    }

    public String getTyp() {
        return typ;
    }

    public void setTyp(String typ) {
        this.typ = typ;
    }

    public Double getLat() {
        return lat;
    }

    public void setLat(Double lat) {
        this.lat = lat;
    }

    public String getSig() {
        return sig;
    }

    public void setSig(String sig) {
        this.sig = sig;
    }
}

class Sen {

    String val; //: "true",
    String sid; //: "1",
    String mis; //: "bool",
    String iid; //: "KeyStatus",
    String typ; //: "POWER",
    String src; //: "DEVICE"

    public String getVal() {
        return val;
    }

    public void setVal(String val) {
        this.val = val;
    }

    public String getSid() {
        return sid;
    }

    public void setSid(String sid) {
        this.sid = sid;
    }

    public String getMis() {
        return mis;
    }

    public void setMis(String mis) {
        this.mis = mis;
    }

    public String getIid() {
        return iid;
    }

    public void setIid(String iid) {
        this.iid = iid;
    }

    public String getTyp() {
        return typ;
    }

    public void setTyp(String typ) {
        this.typ = typ;
    }

    public String getSrc() {
        return src;
    }

    public void setSrc(String src) {
        this.src = src;
    }
}
