package com.trackysat.kafka.domain;

import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.datastax.oss.driver.api.mapper.annotations.NamingStrategy;
import com.datastax.oss.driver.api.mapper.annotations.PartitionKey;
import com.datastax.oss.driver.api.mapper.entity.naming.NamingConvention;
import com.trackysat.kafka.domain.vmson.Inf;
import java.io.Serializable;
import java.time.Instant;
import java.util.Objects;

/**
 * A Device from vmson.
 */
@Entity
@NamingStrategy(convention = NamingConvention.SNAKE_CASE_INSENSITIVE)
public class Device implements Serializable {

    private static final long serialVersionUID = 1L;

    @PartitionKey
    String uid;

    Instant createdDate = Instant.now();

    String cla;
    String ver;
    String typ;

    String companyname;

    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    public Instant getCreatedDate() {
        return createdDate;
    }

    public void setCreatedDate(Instant createdDate) {
        this.createdDate = createdDate;
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

    public String getCompanyname() {
        return companyname;
    }

    public void setCompanyname(String companyname) {
        this.companyname = companyname;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Device)) return false;
        Device devices = (Device) o;
        return Objects.equals(getUid(), devices.getUid());
    }

    @Override
    public int hashCode() {
        // see https://vladmihalcea.com/how-to-implement-equals-and-hashcode-using-the-jpa-entity-identifier/
        return getClass().hashCode();
    }

    // prettier-ignore
    @Override
    public String toString() {
        return "Devices{" +
            "uid='" + uid + '\'' +
            ", createdDate=" + createdDate +
            ", cla='" + cla + '\'' +
            ", ver='" + ver + '\'' +
            ", typ='" + typ + '\'' +
            ", companyname='" + companyname + '\'' +
            '}';
    }
}
