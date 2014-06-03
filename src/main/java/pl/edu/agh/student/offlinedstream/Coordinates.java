package pl.edu.agh.student.offlinedstream;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class Coordinates implements WritableComparable<Coordinates>, Serializable {
    public List<Integer> coords = new ArrayList<>();

    public Coordinates() {}

    public Coordinates(List<Integer> coords) {
        this.coords = coords;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(coords.size());
        for(Integer i: coords) {
            dataOutput.writeInt(i);
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        int length = dataInput.readInt();
        for(int i = 0; i < length; ++i) {
            coords.add(dataInput.readInt());
        }
    }

    @Override
    public int compareTo(Coordinates otherCoords) {
        for(int i = 0; i < coords.size(); ++i) {
            int cmp = coords.get(i).compareTo(otherCoords.coords.get(i));
            if (cmp != 0) {
                return cmp;
            }
        }
        return 0;
    }

    public static String toString(Coordinates g) {
        StringBuilder sb = new StringBuilder(Integer.toString(g.coords.size()));
        for(Integer i : g.coords) {
            sb.append(" ").append(i.intValue());
        }
        return sb.toString();
    }

    public static Coordinates fromString(String s) {
        String[] splitted = s.split(" ");
        ArrayList<Integer> list = new ArrayList<>();
        for(String fragment : splitted) {
            list.add(Integer.parseInt(fragment));
        }
        return new Coordinates(list);
    }

}
