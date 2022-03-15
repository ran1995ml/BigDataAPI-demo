package com.ran.tableapi;

/**
 * ClassName: Word
 * Description:
 * date: 2022/3/12 16:00
 *
 * @author ran
 */
public class Word {
    public String name;
    public int value;
    public long timestamp;

    public Word(String name, int value, long timestamp) {
        this.name = name;
        this.value = value;
        this.timestamp = timestamp;
    }
    public Word(String name, int value) {
        this.name = name;
        this.value = value;
    }


    public Word() {
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public int getValue() {
        return value;
    }

    public void setValue(int value) {
        this.value = value;
    }
}
