package org.example;

import java.io.Serializable;

public class Centroid implements Serializable {
    private int id;
    private Point center;

    public Centroid() {}

    public Centroid(int id, Point center) {
        this.id = id;
        this.center = center;
    }

    /** 比如文件或字符串中，centroid 的格式是 "id x y" */
    public static Centroid fromString(String str) {
        String[] tokens = str.split("\\s+");
        int id = Integer.parseInt(tokens[0]);
        double x = Double.parseDouble(tokens[1]);
        double y = Double.parseDouble(tokens[2]);
        return new Centroid(id, new Point(x, y));
    }

    public int getId() {
        return id;
    }

    public Point getCenter() {
        return center;
    }

    public void setCenter(Point center) {
        this.center = center;
    }

    @Override
    public String toString() {
        return String.format("Centroid %d: %s", id, center.toString());
    }
}
