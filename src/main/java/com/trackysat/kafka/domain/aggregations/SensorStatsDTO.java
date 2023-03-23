package com.trackysat.kafka.domain.aggregations;

import java.util.List;
import java.util.Map;
import java.util.Objects;

public class SensorStatsDTO {

    String sid;
    String measureUnit; //mis
    String name; // iid
    String type; // typ
    String source; //src
    List<SensorValDTO> values;
    Map<String, Long> count;
    SensorValDTO firstValue;
    SensorValDTO lastValue;
    Double max;
    Double min;
    Double avg;
    Double sum;

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

    public Map<String, Long> getCount() {
        return count;
    }

    public void setCount(Map<String, Long> count) {
        this.count = count;
    }

    public SensorValDTO getFirstValue() {
        return firstValue;
    }

    public void setFirstValue(SensorValDTO firstValue) {
        this.firstValue = firstValue;
    }

    public SensorValDTO getLastValue() {
        return lastValue;
    }

    public void setLastValue(SensorValDTO lastValue) {
        this.lastValue = lastValue;
    }

    public Double getMax() {
        return max;
    }

    public void setMax(Double max) {
        this.max = max;
    }

    public Double getMin() {
        return min;
    }

    public void setMin(Double min) {
        this.min = min;
    }

    public Double getAvg() {
        return avg;
    }

    public void setAvg(Double avg) {
        this.avg = avg;
    }

    public Double getSum() {
        return sum;
    }

    public void setSum(Double sum) {
        this.sum = sum;
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
