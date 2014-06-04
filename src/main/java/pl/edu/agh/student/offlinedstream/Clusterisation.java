package pl.edu.agh.student.offlinedstream;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

public class Clusterisation {

    public Map<List<Integer>, Grid> hashmap;

    public static final String filename = "output/part-r-00000";
    public static final double TRANSITIONAL_THRESHOLD = 0.0;
    public static final double DENSE_THRESHOLD = 0.0;

    public Clusterisation() {
        hashmap = new HashMap<>();
    }

    public void readFromFile() {
        BufferedReader br;
        try {
            br = new BufferedReader(new FileReader(filename));
            String line;
            int cluster = 1;
            while ((line = br.readLine()) != null) {
                String[] coords = line.split("\\t")[0].split(" ");
                double density = Double.parseDouble(line.split("\\t")[1]);

                if(density > TRANSITIONAL_THRESHOLD) { // te filtracje mozna przeprowadzic juz wczesniej
                    ArrayList<Integer> key = new ArrayList<>();

                    for(String coord : coords) {
                        key.add(Integer.parseInt(coord));
                    }

                    if(density > DENSE_THRESHOLD) { // dzielimy na dense grid oraz transitional grids
                        hashmap.put(key, new Grid(false, cluster++, density));
                    } else {
                        hashmap.put(key, new Grid(true, -1, density));
                    }
                }
            }
            br.close();
        } catch (FileNotFoundException e) {
            System.out.println("File not found!");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void initialClustering() {

    }

    public static void main(String[] args) {
        Clusterisation c = new Clusterisation();

        c.readFromFile();

        System.out.println(c.hashmap);
    }
}

class Grid {
    public Boolean visited;
    public int cluster;
    public double density;

    public Grid(Boolean v, int c, double d) {
        visited = v;
        cluster = c;
        density = d;
    }
}
