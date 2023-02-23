package com.trackysat.kafka.domain.vmson;

import java.time.Instant;

public class Ets {

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
