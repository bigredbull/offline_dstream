package pl.edu.agh.student.offlinedstream;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.commons.collections.map.HashedMap;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//public class FilterMapper extends Mapper<Coordinates, Grid, Text, GridInfo> {
//
//    public void map(Coordinates coords, Grid grid, Context context) throws IOException, InterruptedException {
//        context.write(new Text("ONE"), new GridInfo(coords, grid));
//    }
//}

public class FilterMapper extends Mapper<Object, Text, Text, Text> {

    public void map(Object k, Text t, Context context) throws IOException, InterruptedException {
        context.write(new Text("ONE"), t);
    }
}