package com.bin.delay.file.common;

import java.util.Objects;

public class Position {
    private Integer position;

    private Long delayTime;

    public Position(Integer position, Long delayTime) {
        this.position = position;
        this.delayTime = delayTime;
    }

    public Long getDelayTime() {
        return delayTime;
    }

    public void setDelayTime(Long delayTime) {
        this.delayTime = delayTime;
    }

    public Integer getPosition() {
        return position;
    }

    public void setPosition(Integer position) {
        this.position = position;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Position position1 = (Position) o;
        return Objects.equals(position, position1.position) && Objects.equals(delayTime, position1.delayTime);
    }

    @Override
    public int hashCode() {
        return Objects.hash(position, delayTime);
    }
}
