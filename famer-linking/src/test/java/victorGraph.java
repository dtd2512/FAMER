






import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.*;

/**
 */
public class victorGraph {
    public static void main (String[] args) throws ParseException, IOException {
        String path = "/home/alieh/git44Prject/sources/MusicDataSet-Hamburg/GraphByVictor/soft_tfidf_matcher_t=0.35maxDelta0.05.json";
        String newPath = "/home/alieh/git44Prject/sources/MusicDataSet-Hamburg/GraphByVictor/edges.csv";
        BufferedReader br = new BufferedReader(new FileReader(path));
        BufferedWriter bw = new BufferedWriter(new FileWriter(newPath));

        JSONParser parser = new JSONParser();
        String line = br.readLine();
        do {
            JSONObject json = (JSONObject) parser.parse(line);
            String newLine = json.get("source")+","+json.get("target")+","+((JSONObject) json.get("data")).get("similarity")+"\n";
            bw.append(newLine);
            line = br.readLine();
        }while (line != null);
        bw.flush();
        bw.close();
    }
}
