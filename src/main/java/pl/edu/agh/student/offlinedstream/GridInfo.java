package pl.edu.agh.student.offlinedstream;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

public class GridInfo implements WritableComparable<GridInfo>, Serializable {

    public Coordinates coords;
    public Grid grid;

    public GridInfo(Coordinates c, Grid g) {
        coords = c;
        grid = g;
    }

    @Override
    public int compareTo(GridInfo gridInfo) {
        if(coords.compareTo(gridInfo.coords) == 0) {
            return grid.compareTo(gridInfo.grid);
        } else {
            return coords.compareTo(gridInfo.coords);
        }
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        coords.write(dataOutput);
        grid.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        coords.readFields(dataInput);
        grid.readFields(dataInput);
    }
}
