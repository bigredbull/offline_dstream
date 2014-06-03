package pl.edu.agh.student.offlinedstream;

import org.apache.hadoop.io.WritableComparable;

import java.io.*;

public class Grid implements WritableComparable<Grid>, Serializable {
    public double density;
    public int refreshmentTime;

    public Grid(){
        density = 0.0;
        refreshmentTime = -1;
    }

    public Grid(double d, int r) {
        density = d;
        refreshmentTime = r;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeDouble(density);
        dataOutput.writeInt(refreshmentTime);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        density = dataInput.readDouble();
        refreshmentTime = dataInput.readInt();
    }

    @Override
    public int compareTo(Grid grid) {
        int cmp = new Double(density).compareTo(grid.density);
        if(cmp == 0) {
            return new Integer(refreshmentTime).compareTo(grid.refreshmentTime);
        } else {
            return cmp;
        }
    }

    public static String toString(Grid g) {
        return Double.toString(g.density) + " " + Integer.toString(g.refreshmentTime);
    }

    public static Grid fromString(String s) {
        double d = Double.parseDouble(s.split(" ")[0]);
        int r = Integer.parseInt(s.split(" ")[1]);
        return new Grid(d, r);
    }
}
