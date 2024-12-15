package org.example;

/**
 * Hello world!
 *
 */
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import scala.Serializable;

public class KMeans {

    public static void main(String[] args) throws Exception {
        /*
         * 参数示例：
         * --points path/to/points.txt
         * --centroids path/to/centroids.txt
         * --output path/to/output.txt
         * --iterations 10
         */

        // 解析命令行参数
        String pointsPath = null;
        String centroidsPath = null;
        String outputPath = null;
        int iterations = 10;

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--points":
                    pointsPath = args[++i];
                    break;
                case "--centroids":
                    centroidsPath = args[++i];
                    break;
                case "--output":
                    outputPath = args[++i];
                    break;
                case "--iterations":
                    iterations = Integer.parseInt(args[++i]);
                    break;
                default:
                    // 未识别的参数
                    break;
            }
        }

        if (pointsPath == null || centroidsPath == null || outputPath == null) {
            System.err.println("Usage: KMeans --points <file> --centroids <file> --output <file> --iterations <n>");
            System.exit(1);
        }

        // 创建执行环境
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 读取数据集
        DataSet<Point> points = env.readTextFile(pointsPath)
                .map(Point::fromString);
        DataSet<Centroid> centroids = env.readTextFile(centroidsPath)
                .map(Centroid::fromString);

        // 创建可迭代的 DataSet
        IterativeDataSet<Centroid> iterativeCentroids = centroids.iterate(iterations);

        // 把每个点映射到最近质心的 (centroidId, point)
        DataSet<Centroid> newCentroids = points
                .map(new SelectNearestCenter()).withBroadcastSet(iterativeCentroids, "centroids")
                .groupBy("f0") // f0即centroidId
                .reduceGroup(new GroupReduceFunction<Tuple2<Integer, Point>, Centroid>() {
                    @Override
                    public void reduce(Iterable<Tuple2<Integer, Point>> values, Collector<Centroid> out) {
                        int centroidId = -1;
                        long count = 0;
                        Point sum = new Point(0, 0);
                        for (Tuple2<Integer, Point> val : values) {
                            centroidId = val.f0;
                            sum = sum.add(val.f1);
                            count++;
                        }
                        out.collect(new Centroid(centroidId, sum.div(count)));
                    }
                });

        // 将新的质心返回到下一次迭代
        DataSet<Centroid> finalCentroids = iterativeCentroids.closeWith(newCentroids);

        // 把最终聚类的结果写到文件：先把每个点映射到最终质心并输出 (centroidId, point)
        DataSet<Tuple2<Integer, Point>> clusteredPoints = points
                .map(new SelectNearestCenter()).withBroadcastSet(finalCentroids, "centroids");

        // 输出结果
        finalCentroids.writeAsText(outputPath + "/final_centroids.txt").setParallelism(1);
        clusteredPoints.writeAsText(outputPath + "/clustered_points.txt").setParallelism(1);

        // 执行 Flink 作业
        env.execute("Flink KMeans Example");
    }

    /**
     * SelectNearestCenter MapFunction
     *  - Broadcast 变量: Iterable<Centroid>
     *  - 输入: Point
     *  - 输出: Tuple2<centroidId, Point>
     */
    public static class SelectNearestCenter extends RichMapFunction<Point, Tuple2<Integer, Point>> {

        private Iterable<Centroid> centroids;

        @Override
        public void open(Configuration parameters) throws Exception {
            this.centroids = getRuntimeContext().getBroadcastVariable("centroids");
        }

        @Override
        public Tuple2<Integer, Point> map(Point p) throws Exception {
            double minDistance = Double.MAX_VALUE;
            int closestCentroidId = -1;

            for (Centroid centroid : centroids) {
                double distance = p.distanceTo(centroid.getCenter());
                if (distance < minDistance) {
                    minDistance = distance;
                    closestCentroidId = centroid.getId();
                }
            }
            return new Tuple2<>(closestCentroidId, p);
        }
    }

    /**
     * 一个简单的 Tuple2 的模拟类，如果不想引入 Flink 的 Tuple2 可以自己定义。
     * 这里为演示方便，直接使用 org.apache.flink.api.java.tuple.Tuple2。
     */
    public static class Tuple2<T0, T1> implements Serializable {
        public T0 f0;
        public T1 f1;

        public Tuple2() {}

        public Tuple2(T0 f0, T1 f1) {
            this.f0 = f0;
            this.f1 = f1;
        }

        @Override
        public String toString() {
            return "(" + f0 + "," + f1 + ")";
        }
    }
}
