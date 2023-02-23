package com.trackysat.kafka.domain.vmson;

public class Sat {

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
