package com.trackysat.kafka.domain.aggregations;

import java.util.List;
import java.util.Objects;

public class SensorStatsDTO {

    String sid;
    String measureUnit; //mis
    String name; // iid
    String type; // typ
    String source; //src
    List<SensorValDTO> values;

    public String getSid() {
        return sid;
    }

    public void setSid(String sid) {
        this.sid = sid;
    }

    public String getMeasureUnit() {
        return measureUnit;
    }

    public void setMeasureUnit(String measureUnit) {
        this.measureUnit = measureUnit;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public List<SensorValDTO> getValues() {
        return values;
    }

    public void setValues(List<SensorValDTO> values) {
        this.values = values;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SensorStatsDTO)) return false;
        SensorStatsDTO that = (SensorStatsDTO) o;
        return Objects.equals(getName(), that.getName());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getName());
    }

    @Override
    public String toString() {
        return (
            "SensorIdDTO{" +
            "sid='" +
            sid +
            '\'' +
            ", measureUnit='" +
            measureUnit +
            '\'' +
            ", name='" +
            name +
            '\'' +
            ", type='" +
            type +
            '\'' +
            ", source='" +
            source +
            '\'' +
            '}'
        );
    }
}
