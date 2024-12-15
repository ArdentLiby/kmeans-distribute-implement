package org.example;

import java.io.Serializable;
import java.util.Arrays;

public class Point implements Serializable {
    private double x;
    private double y;

    public Point() {}

    public Point(double x, double y) {
        this.x = x;
        this.y = y;
    }

    /** 从字符串解析Point，如 "3.0 4.5" */
    public static Point fromString(String str) {
        String[] tokens = str.split("\\s+");
        return new Point(Double.parseDouble(tokens[0]), Double.parseDouble(tokens[1]));
    }

    public double distanceTo(Point other) {
        double dx = this.x - other.x;
        double dy = this.y - other.y;
        return Math.sqrt(dx * dx + dy * dy);
    }

    public Point add(Point other) {
        return new Point(this.x + other.x, this.y + other.y);
    }

    public Point div(long val) {
        return new Point(this.x / val, this.y / val);
    }

    @Override
    public String toString() {
        return String.format("(%f, %f)", x, y);
    }

    public double getX() {
        return x;
    }

    public double getY() {
        return y;
    }
}
