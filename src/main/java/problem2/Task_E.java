package problem2;
import java.io.*;
import java.net.URI;
import java.util.ArrayList;
import java.util.Scanner;

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

public class Task_E {

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
        context.write(new Text(nearestCentroid.toString()), new Text(1 + "-" + currentPoint.toString()));
//        System.out.println(1 + "-" + currentPoint.toString());
      } catch (NumberFormatException e) {
//        System.out.println("not parsable ints, skipped this line");
      } catch (NullPointerException e) {
        System.out.println("Point nearestCentroid is null");
      }
    }
  }

  public static class FinalClusterDataPointsMapper
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

  public static class AggregatorOptimizationCombiner
          extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterable<Text> values,
                       Context context
    ) throws IOException, InterruptedException {
//      System.out.println("Using the combiner!!!");
      int numDataPoints = 0;
      double xSum = 0;
      double ySum = 0;
      for(Text value: values) {
        String[] tokens = value.toString().split("-");
        int count = Integer.parseInt(tokens[0]);
        numDataPoints += count;
        String[] dataPointComponents = tokens[1].split(",");
        double x = Double.parseDouble(dataPointComponents[0]);
        double y = Double.parseDouble(dataPointComponents[1]);
        xSum += x;
        ySum += y;
      }
      Point aggregatePoint = new Point(xSum, ySum); //not a real point, just to make String formatting easier for output
      context.write(key, new Text(numDataPoints + "-" + aggregatePoint.toString()));
    }
  }

  public static class NewCentroidCalculatorReducer
          extends Reducer<Text,Text,Text,Text> {
    private double convergenceThreshold;
    private boolean converged = true;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      convergenceThreshold = context.getConfiguration().getDouble("convergenceThreshold", 1.0);
//      System.out.println("ConvergenceThreshold: " + convergenceThreshold);
    }

    public void reduce(Text key, Iterable<Text> values,
                       Context context
    ) throws IOException, InterruptedException {
//      System.out.println("Using the reducer!!!");
      int numDataPoints = 0;
      double xSum = 0;
      double ySum = 0;
      for(Text value: values) {
        String[] tokens = value.toString().split("-");
        int count = Integer.parseInt(tokens[0]);
        numDataPoints += count;
        String[] dataPointComponents = tokens[1].split(",");
        double x = Double.parseDouble(dataPointComponents[0]);
        double y = Double.parseDouble(dataPointComponents[1]);
        xSum += x;
        ySum += y;
      }
      Point newCentroid = new Point(xSum/numDataPoints,ySum/numDataPoints);
      String[] oldCentroidTokens = key.toString().split(",");
      Point oldCentroid = new Point(Double.parseDouble(oldCentroidTokens[0]), Double.parseDouble(oldCentroidTokens[1]));
      if (oldCentroid.calculateEuclideanDistance(newCentroid) > convergenceThreshold) {
        converged = false;
      }
      context.write(new Text(newCentroid.toString()), key);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
      if (converged) {
        context.write(new Text("Yes"), new Text(""));
      } else {
        context.write(new Text("No"), new Text(""));
      }
    }
  }

  public static void main(String[] args) throws Exception {
    long startTime = System.currentTimeMillis();
    Configuration conf = new Configuration();
    int iterations = 20; // max number of iterations parameter here
    double convergenceThreshold = 1.0; // convergence threshold here
    /*
    The returnFinalClusterDataPoints variable below is used to switch between the two different output options
    designated in subparts a) and b) of Task E. When this variable is set to false, it will only return
    the most updated centroids along with a flag at the end of either "Yes" or "No" which indicates
    whether the centroids have converged or not (Task E, part a). When it is set to true, the
    output will return the centroids along with their associated data points in key-value pairs
    (Task E, part b).
     */
    boolean returnFinalClusterDataPoints = true;

    boolean converged = false;
    String lastIterationCentroidFilePath = null;
    conf.set("convergenceThreshold", Double.toString(convergenceThreshold));

    for (int i = 0; i < iterations; i++) {
      Job job = Job.getInstance(conf, "Find centroid " + i);
      job.setJarByClass(Task_E.class);
      job.setMapperClass(Task_E.ClosestCentroidMapper.class);
      job.setCombinerClass(Task_E.AggregatorOptimizationCombiner.class);
      job.setReducerClass(Task_E.NewCentroidCalculatorReducer.class);
      job.setMapOutputKeyClass(Text.class);
      job.setMapOutputValueClass(Text.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);

      // adding cache file
      if(i == 0) {
        job.addCacheFile(new URI("file:///C:/schoolMahir/CS4433-BigData/Project2/CS-4433-Project-2/data_set/k_centroids.csv"));
        lastIterationCentroidFilePath = "file:///C:/schoolMahir/CS4433-BigData/Project2/CS-4433-Project-2/data_set/k_centroids.csv";
      } else {
        job.addCacheFile(new URI("file:///C:/schoolMahir/CS4433-BigData/Project2/CS-4433-Project-2/problem2_output/centroids" + (i-1) + "/part-r-00000"));
        lastIterationCentroidFilePath = "file:///C:/schoolMahir/CS4433-BigData/Project2/CS-4433-Project-2/problem2_output/centroids" + (i-1) + "/part-r-00000";
      }
      FileInputFormat.addInputPath(job, new Path("C:\\schoolMahir\\CS4433-BigData\\Project2\\CS-4433-Project-2\\data_set\\data_points.csv"));
      FileOutputFormat.setOutputPath(job, new Path("problem2_output/centroids" + i));
      job.waitForCompletion(true);

      //checking the convergence after the job completes
      if (hasConverged("C:\\schoolMahir\\CS4433-BigData\\Project2\\CS-4433-Project-2\\problem2_output\\centroids" + (i) + "\\part-r-00000", convergenceThreshold)) {
        System.out.println("Convergence threshold has been reached at iteration " + (i));
        converged = true;
        break;
      }
    }

    // final map-only job to output the key-value pairs of
    // (new centroid computed in the very last iteration, point coord in the same cluster)
    // when returnFinalClusterDataPoints is true
    if (converged && returnFinalClusterDataPoints) {
      Job job = Job.getInstance(conf, "Output final cluster data points");
      job.setJarByClass(Task_E.class);
      job.setMapperClass(Task_E.FinalClusterDataPointsMapper.class);
      job.setMapOutputKeyClass(Text.class);
      job.setMapOutputValueClass(Text.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);
      job.setNumReduceTasks(0);

      // adding cache file
      assert lastIterationCentroidFilePath != null;
      job.addCacheFile(new URI(lastIterationCentroidFilePath));
      FileInputFormat.addInputPath(job, new Path("C:\\schoolMahir\\CS4433-BigData\\Project2\\CS-4433-Project-2\\data_set\\data_points.csv"));
      FileOutputFormat.setOutputPath(job, new Path("problem2_output/final"));
      job.waitForCompletion(true);
    }
    long taskFEndTime = System.currentTimeMillis();
    System.out.println("Task E Execution Time: " + (taskFEndTime - startTime)/1000.0 + " seconds.");
  }

  private static boolean hasConverged(String pathToOutputFile, Double convergenceThreshold) {
    try {
      File outputFile = new File(pathToOutputFile);
      Scanner scanner = new Scanner(outputFile);
      while (scanner.hasNextLine()) {
        String line = scanner.nextLine();
        // to handle the "yes" or "no" at the end of file
        if(line.split("\t")[0].equalsIgnoreCase("yes") || line.split("\t")[0].equalsIgnoreCase("no")) {
          break;
        }
        String[] centroids = line.split("\t");
        String newCentroid = centroids[0];
        String oldCentroid = centroids[1];
        double newCentroidX = Double.parseDouble(newCentroid.split(",")[0]);
        double newCentroidY = Double.parseDouble(newCentroid.split(",")[1]);
        double oldCentroidX = Double.parseDouble(oldCentroid.split(",")[0]);
        double oldCentroidY = Double.parseDouble(oldCentroid.split(",")[1]);
        Point newPoint = new Point(newCentroidX, newCentroidY);
        Point oldPoint = new Point(oldCentroidX, oldCentroidY);
        double distance = newPoint.calculateEuclideanDistance(oldPoint);
        if (distance > convergenceThreshold) {
          return false;
        }
      }
      scanner.close();
    } catch (FileNotFoundException e) {
      System.out.println("Output file not found error.");
      e.printStackTrace();
    }
    return true;
  }
}