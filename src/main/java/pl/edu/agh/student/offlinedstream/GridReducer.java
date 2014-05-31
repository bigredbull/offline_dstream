package pl.edu.agh.student.offlinedstream;

import java.io.IOException;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import sun.tools.tree.NewInstanceExpression;

public class GridReducer extends Reducer<Coordinates, Grid, IntWritable, Text> {

    public static final double LAMBDA = 0.8;

    public double currentDensity(double oldDensity, int lastTime, int currentTime) {
        int deltaTime = currentTime - lastTime;
        return Math.pow(LAMBDA, deltaTime) * oldDensity + 1;
    }

    public void reduce(Coordinates coords, Iterable<Grid> grids, Context context) throws IOException, InterruptedException {
        int last_rt = -1;
        for(Grid g : grids) {
            if(g.refreshmentTime > last_rt) {
                last_rt = g.refreshmentTime;
            }
        }

        Grid mergedGrid = new Grid();
        mergedGrid.refreshmentTime = last_rt;

        for(Grid g : grids) {
            if(g.refreshmentTime != last_rt) {
                g.density = currentDensity(g.density, g.refreshmentTime, last_rt);
            }
            mergedGrid.density += g.density;
        }
//        context.write(coords, mergedGrid);
        context.write(new IntWritable(1), new Text("hello!"));
    }
}