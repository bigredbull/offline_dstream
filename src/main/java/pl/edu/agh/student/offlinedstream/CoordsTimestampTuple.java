package pl.edu.agh.student.offlinedstream;

import com.google.common.base.Objects;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CoordsTimestampTuple implements WritableComparable<CoordsTimestampTuple> {

    private Coordinates coords;
    private IntWritable timestamp;

    public CoordsTimestampTuple() {
        this.coords = new Coordinates();
        this.timestamp = new IntWritable();
    }

    public CoordsTimestampTuple(Coordinates coords, IntWritable timestamp) {
        this.coords = coords;
        this.timestamp = timestamp;
    }

    public Coordinates getCoords() {
        return coords;
    }

    public IntWritable getTimestamp() {
        return timestamp;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        coords.write(dataOutput);
        timestamp.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        coords.readFields(dataInput);
        timestamp.readFields(dataInput);
    }

    @Override
    public int compareTo(CoordsTimestampTuple otherTuple) {
        int comp = coords.compareTo(otherTuple.coords);

        if (comp != 0) {
            return comp;
        }

        return timestamp.compareTo(otherTuple.timestamp);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }

        if (!(obj instanceof CoordsTimestampTuple)) {
            return false;
        }

        CoordsTimestampTuple other = (CoordsTimestampTuple) obj;
        return other.coords.equals(coords) && other.timestamp.equals(timestamp);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(coords, timestamp);
    }

    @Override
    public String toString() {
        return "[" + coords.toString() + ", " + timestamp.toString() + "]";
    }

}
