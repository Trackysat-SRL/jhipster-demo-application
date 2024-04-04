package com.trackysat.kafka.domain.aggregations;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class SensorStatsDTO {

    private final Logger log = LoggerFactory.getLogger(SensorStatsDTO.class);

    String sid;
    String measureUnit; //mis
    String name; // iid
    String type; // typ
    String source; //src
    List<SensorValDTO> values;

    List<SensorValDTO> lastDailyValues;
    Map<String, Long> count;
    SensorValDTO firstValue;
    SensorValDTO lastValue;
    Double max;
    Double min;
    Double avg;
    Double sum;
    Double diff;

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

    public Double getDiff() {
        return diff;
    }

    public void setDiff(Double diff) {
        this.diff = diff;
    }

    public List<SensorValDTO> getLastDailyValues() {
        return lastDailyValues;
    }

    public void setLastDailyValues(List<SensorValDTO> lastDailyValues) {
        this.lastDailyValues = lastDailyValues;
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

    public void recalculate(int days) {
        this.setMax(null);
        this.setMin(null);
        this.setDiff(null);
        this.setCount(null);
        this.setAvg(null);
        this.setSum(null);
        this.setFirstValue(null);
        this.setLastValue(null);
        values.stream().min(Comparator.comparing(c -> c.getCreationDate().getEpochSecond())).ifPresent(this::setFirstValue);
        values.stream().max(Comparator.comparing(c -> c.getCreationDate().getEpochSecond())).ifPresent(this::setLastValue);

        if (Objects.equals(this.getType(), "TELLTALE") || Objects.equals(this.getType(), "BOOLEAN")) {
            this.setCount(
                    values.stream().map(SensorValDTO::getValue).collect(Collectors.groupingBy(Function.identity(), Collectors.counting()))
                );
        } else {
            try {
                List<Double> doubleList = values.stream().map(SensorValDTO::getValue).map(Double::parseDouble).collect(Collectors.toList());
                doubleList.stream().max(Comparator.naturalOrder()).ifPresent(this::setMax);
                doubleList.stream().min(Comparator.naturalOrder()).ifPresent(this::setMin);
                this.setDiff(Optional.ofNullable(this.getMax()).orElse(0.0) - Optional.ofNullable(this.getMin()).orElse(0.0));

                this.setSum(doubleList.stream().reduce(0.0, Double::sum));
                if (days >= 0) {
                    this.setCount(Collections.singletonMap("total", (long) days));
                    this.setAvg(doubleList.stream().reduce(0.0, Double::sum) / days);
                } else {
                    this.setCount(Collections.singletonMap("total", (long) doubleList.size()));
                    this.setAvg(doubleList.stream().reduce(0.0, Double::sum) / doubleList.size());
                }
            } catch (Exception e) {
                log.error("Cannot parse double form values. {}", this);
            }
        }
    }
}
