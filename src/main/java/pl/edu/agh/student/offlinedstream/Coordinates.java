package pl.edu.agh.student.offlinedstream;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class Coordinates implements WritableComparable<Coordinates>, Iterable<Integer>, Serializable {

    private List<Integer> coords = new ArrayList<>();

    public Coordinates() {
    }

    public Coordinates(List<Integer> coords) {
        this.coords = coords;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(coords.size());
        for (Integer coord : coords) {
            dataOutput.writeInt(coord);
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        coords = new ArrayList<>();

        int size = dataInput.readInt();
        for (int i = 0; i < size; i++) {
            coords.add(dataInput.readInt());
        }
    }

    @Override
    public Iterator<Integer> iterator() {
        return coords.iterator();
    }

    @Override
    public int compareTo(Coordinates otherCoords) {
        Iterator<Integer> it = iterator();
        Iterator<Integer> otherIt = otherCoords.iterator();

        while (it.hasNext() && otherIt.hasNext()) {
            Integer coord = it.next();
            Integer otherCoord = otherIt.next();

            int comp = coord.compareTo(otherCoord);
            if (comp != 0) {
                return comp;
            }
        }

        return 0;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }

        if (!(obj instanceof Coordinates)) {
            return false;
        }

        Coordinates other = (Coordinates) obj;
        return other.coords.equals(coords);
    }

    @Override
    public int hashCode() {
        return coords.hashCode();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        Iterator<Integer> it = coords.iterator();

        sb.append('(');

        if (it.hasNext()) {
            sb.append(it.next());
        }

        while (it.hasNext()) {
            sb.append(',');
            sb.append(it.next());
        }

        sb.append(')');

        return sb.toString();
    }

}
