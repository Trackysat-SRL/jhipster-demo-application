package com.trackysat.kafka.domain.vmson;

import java.util.List;

public class Con {

    Ets ets;
    Sat sat;
    List<Sen> sen;
    Gsm gsm;

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

    public Gsm getGsm() {
        return gsm;
    }

    public void setGsm(Gsm gsm) {
        this.gsm = gsm;
    }
}
