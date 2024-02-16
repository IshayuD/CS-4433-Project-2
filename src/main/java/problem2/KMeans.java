package problem2;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class KMeans {

  //helper class to store the Points

  private static class Point{
    private int x;
    private int y;

    public Point(int x, int y) {
      this.x = x;
      this.y = y;
    }

    public int getX() {
      return x;
    }

    public int getY() {
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

      BufferedReader reader = new BufferedReader(new InputStreamReader(fis,
              "UTF-8"));

      String line;
      while (StringUtils.isNotEmpty(line = reader.readLine())) {
        String[] pointComponents = line.split(",");
        int x;
        int y;
        try {
          x = Integer.parseInt(pointComponents[0]);
          y = Integer.parseInt(pointComponents[1]);
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
        Point currentPoint = new Point(Integer.parseInt(components[0]), Integer.parseInt(components[1]));
        for (Point p: centroids) {
          double dist = currentPoint.calculateEuclideanDistance(p);
          if(dist < lowestDistance) {
            lowestDistance = dist;
            nearestCentroid = p;
          }
        }
        assert nearestCentroid != null;
        context.write(new Text(nearestCentroid.toString()), new Text(currentPoint.toString()));
      } catch (NumberFormatException e) {
        System.out.println("not parsable ints, skipped this line");
      } catch (NullPointerException e) {
        System.out.println("Point nearestCentroid is null");
      }
    }
  }

//  public static class IntSumReducer
//          extends Reducer<Text,IntWritable,Text,IntWritable> {
//    private IntWritable result = new IntWritable();
//
//    public void reduce(Text key, Iterable<IntWritable> values,
//                       Context context
//    ) throws IOException, InterruptedException {
//      int sum = 0;
//      for (IntWritable val : values) {
//        sum += val.get();
//      }
//      result.set(sum);
//      context.write(key, result);
//    }
//  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Find centroid");
    job.setJarByClass(KMeans.class);
    job.setMapperClass(ClosestCentroidMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.addCacheFile(new URI("file:///C:/schoolMahir/CS4433-BigData/Project2/CS-4433-Project-2/data_set/k_centroids.csv"));
    job.setNumReduceTasks(0);
    FileInputFormat.addInputPath(job, new Path("C:\\schoolMahir\\CS4433-BigData\\Project2\\CS-4433-Project-2\\data_set\\data_points.csv"));
    FileOutputFormat.setOutputPath(job, new Path("problem2_output"));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}