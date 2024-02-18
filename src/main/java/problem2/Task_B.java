package problem2;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Task_B {

  //helper class to store the Points

  private static class Point{
    private final double x;
    private final double y;

    public Point(double x, double y) {
      this.x = x;
      this.y = y;
    }

    public double getX() {
      return x;
    }

    public double getY() {
      return y;
    }

    @Override
    public String toString() {
      return this.x + "," + this.y;
    }

    public double calculateEuclideanDistance(Point otherPoint) {
      return Math.sqrt(Math.pow((this.x - otherPoint.getX()), 2) + Math.pow((this.y - otherPoint.getY()), 2));
    }
  }

  public static class ClosestCentroidMapper
          extends Mapper<Object, Text, Text, Text>{

    ArrayList<Point> centroids = new ArrayList<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      URI[] cacheFiles = context.getCacheFiles();
      Path path = new Path(cacheFiles[0]);

      FileSystem fs = FileSystem.get(context.getConfiguration());
      FSDataInputStream fis = fs.open(path);

      BufferedReader reader = new BufferedReader(new InputStreamReader(fis,"UTF-8"));

      String line;
      while (StringUtils.isNotEmpty(line = reader.readLine())) {
        String newCentroid = line.split("\t")[0];
        String[] pointComponents = newCentroid.split(",");
        double x;
        double y;
        try {
          x = Double.parseDouble(pointComponents[0]);
          y = Double.parseDouble(pointComponents[1]);
          centroids.add(new Point(x, y));
        }
        catch (NumberFormatException e){
//          System.out.println("not parsable ints, skipped this line");
        }
      }
//      System.out.println(centroids);
      IOUtils.closeStream(reader);
    }

    public void map(Object key, Text value, Context context
    ) throws IOException, InterruptedException {
      double lowestDistance = Double.MAX_VALUE;
      Point nearestCentroid = null;
      String[] components = value.toString().split(",");
      try {
        Point currentPoint = new Point(Double.parseDouble(components[0]), Double.parseDouble(components[1]));
        for (Point centroid: centroids) {
          double dist = currentPoint.calculateEuclideanDistance(centroid);
          if(dist < lowestDistance) {
            lowestDistance = dist;
            nearestCentroid = centroid;
          }
        }
        assert nearestCentroid != null;
        context.write(new Text(nearestCentroid.toString()), new Text(currentPoint.toString()));
      } catch (NumberFormatException e) {
//        System.out.println("not parsable ints, skipped this line");
      } catch (NullPointerException e) {
        System.out.println("Point nearestCentroid is null");
      }
    }
  }

  public static class NewCentroidCalculatorReducer
          extends Reducer<Text,Text,Text,Text> {

    public void reduce(Text key, Iterable<Text> values,
                       Context context
    ) throws IOException, InterruptedException {
      int numDataPoints = 0;
      double xSum = 0;
      double ySum = 0;
      for(Text value: values) {
        numDataPoints++;
        String[] dataPointComponents = value.toString().split(",");
        double x = Double.parseDouble(dataPointComponents[0]);
        double y = Double.parseDouble(dataPointComponents[1]);
        xSum += x;
        ySum += y;
      }
      Point newCentroid = new Point(xSum/numDataPoints,ySum/numDataPoints);
      context.write(new Text(newCentroid.toString()), key);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    int iterations = 5;

    for (int i = 0; i < iterations; i++) {
      Job job = Job.getInstance(conf, "Find centroid " + i);
      job.setJarByClass(Task_B.class);
      job.setMapperClass(Task_B.ClosestCentroidMapper.class);
      job.setReducerClass(Task_B.NewCentroidCalculatorReducer.class);
      job.setMapOutputKeyClass(Text.class);
      job.setMapOutputValueClass(Text.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);

      // adding cache file
      if(i == 0) {
        job.addCacheFile(new URI("file:///C:/schoolMahir/CS4433-BigData/Project2/CS-4433-Project-2/data_set/k_centroids.csv"));
      } else {
        job.addCacheFile(new URI("file:///C:/schoolMahir/CS4433-BigData/Project2/CS-4433-Project-2/problem2_output/centroids" + (i-1) + "/part-r-00000"));
      }
      FileInputFormat.addInputPath(job, new Path("C:\\schoolMahir\\CS4433-BigData\\Project2\\CS-4433-Project-2\\data_set\\data_points.csv"));
      FileOutputFormat.setOutputPath(job, new Path("problem2_output/centroids" + i));
      job.waitForCompletion(true);
    }
//    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}