package com.ran.streamapi;

/**
 * ClassName: SensorReading
 * Description:
 * date: 2022/2/26 11:29
 *
 * @author ran
 */
public class SensorReading {
    private String name;
    private int value;
    private long timeStamp;

    public SensorReading(String name, int value) {
        this.name = name;
        this.value = value;
    }

    public SensorReading() {
    }

    public long getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(long timeStamp) {
        this.timeStamp = timeStamp;
    }

    public SensorReading(String name, int value, long timeStamp) {
        this.name = name;
        this.value = value;
        this.timeStamp = timeStamp;
    }

    @Override
    public String toString() {
        return "SensorReading{" +
                "name='" + name + '\'' +
                ", value=" + value +
                ", timeStamp=" + timeStamp +
                '}';
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getValue() {
        return value;
    }

    public void setValue(int value) {
        this.value = value;
    }
}
