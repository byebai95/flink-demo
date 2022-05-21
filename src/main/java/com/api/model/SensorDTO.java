package com.api.model;

public class SensorDTO {

    private String id;
    private String timestamp;
    private Double temperature;

    public SensorDTO() {
    }

    public SensorDTO(String id, String timestamp, Double temperature) {
        this.id = id;
        this.timestamp = timestamp;
        this.temperature = temperature;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public Double getTemperature() {
        return temperature;
    }

    public void setTemperature(Double temperature) {
        this.temperature = temperature;
    }

    @Override
    public String toString() {
        return "Sensor{" +
                "id=" + id +
                ", timestamp='" + timestamp + '\'' +
                ", temperature=" + temperature +
                '}';
    }
}
