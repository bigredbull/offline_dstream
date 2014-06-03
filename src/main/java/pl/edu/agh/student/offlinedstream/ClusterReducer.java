package pl.edu.agh.student.offlinedstream;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import sun.tools.tree.NewInstanceExpression;

public class ClusterReducer extends Reducer<Text, Text, IntWritable, Text> {

    public void reduce(Text coords, Iterable<Text> grids, Context context) throws IOException, InterruptedException {
        Iterator<Text> it = grids.iterator();
        context.write(new IntWritable(1), it.next());
    }
}

//public class ClusterReducer extends Reducer<Text, GridInfo, IntWritable, LinkedList> {
//
//    public static final double LAMBDA = 0.8;
//
//    public double currentDensity(double oldDensity, int lastTime, int currentTime) {
//        int deltaTime = currentTime - lastTime;
//        return Math.pow(LAMBDA, deltaTime) * oldDensity + 1;
//    }
//
//    public void reduce(Coordinates coords, Iterable<Grid> grids, Context context) throws IOException, InterruptedException {
//        int last_rt = -1;
//        for(Grid g : grids) {
//            if(g.refreshmentTime > last_rt) {
//                last_rt = g.refreshmentTime;
//            }
//        }
//
//        Grid mergedGrid = new Grid();
//        mergedGrid.refreshmentTime = last_rt;
//
//        for(Grid g : grids) {
//            if(g.refreshmentTime != last_rt) {
//                g.density = currentDensity(g.density, g.refreshmentTime, last_rt);
//            }
//            mergedGrid.density += g.density;
//        }
////        context.write(coords, mergedGrid);
//        LinkedList<Coordinates> cluster = new LinkedList<>();
//        cluster.add(new Coordinates());
//        context.write(new IntWritable(1), cluster);
//    }
//}