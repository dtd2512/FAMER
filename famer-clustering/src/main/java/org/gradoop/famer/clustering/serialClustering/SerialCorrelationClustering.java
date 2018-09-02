package org.gradoop.famer.clustering.serialClustering;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.gradoop.famer.clustering.serialClustering.util.SerialVertexComponents;

/**
 * The serial implementation of Correlation Clustering algorithm
 */
public class SerialCorrelationClustering {


    public boolean doSerialClustering(int priority, int type, String verticesInputPath, String edgesInputPath, String outputDir, boolean isBidirection) {
        HashMap<Long, SerialVertexComponents> VPVertices = new HashMap<Long, SerialVertexComponents>();
        HashMap<String, SerialVertexComponents> GIdVertices = new HashMap<String, SerialVertexComponents>();
        Long maxVP = Long.MIN_VALUE;
        Long minVP = Long.MAX_VALUE;

        BufferedReader br = null;

        try {
            String line;
            br = new BufferedReader(new FileReader(verticesInputPath));
            while ((line = br.readLine()) != null) {
                String id = line.split(",")[0];
                Long vp = Long.parseLong(line.split(",")[1]);
                SerialVertexComponents svc = new SerialVertexComponents(vp,0L, false);
                svc.setId(id);
                GIdVertices.put(id, svc);
                VPVertices.put(vp, svc);
                if (vp < minVP)
                    minVP = vp;
                if (vp > maxVP)
                    maxVP = vp;
            }

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (br != null) br.close();
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
        try {
            String line;
            br = new BufferedReader(new FileReader(edgesInputPath));
            while ((line = br.readLine()) != null) {
                String srcId = line.split(",")[0];
                String trgtId = line.split(",")[1];
                GIdVertices.get(srcId).addToNeighborList(trgtId);
                if (!isBidirection)
                    GIdVertices.get(trgtId).addToNeighborList(srcId);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (br != null) br.close();
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
        Long interval = maxVP-minVP;
        Long remaining = maxVP-minVP;
        while (remaining > 0){
            boolean randgenerator = true;
            Long randomNum = 0L;
            while (randgenerator) {
                randomNum = (long) Math.floor(Math.random() * interval + minVP);
                randgenerator = GIdVertices.get(VPVertices.get(randomNum).getId()).getIsAssigned();
            }
            String centerId = VPVertices.get(randomNum).getId();
            Long centerClusterId = randomNum;
            GIdVertices.get(centerId).setIsAssigned(true);
            GIdVertices.get(centerId).setClusterId(centerClusterId);
            remaining --;
            Collection<String> neighborList = GIdVertices.get(centerId).getNeighborList();
            for(String neighbor:neighborList){
                if (!GIdVertices.get(neighbor).getIsAssigned()){
                    GIdVertices.get(neighbor).setIsAssigned(true);
                    GIdVertices.get(neighbor).setClusterId(centerClusterId);
                    remaining --;
                }
            }
        }

         try {
            File file = new File(outputDir);
            if (!file.exists()) {
                file.createNewFile();
            }
            FileWriter fw = new FileWriter(file.getAbsoluteFile());
            BufferedWriter bw = new BufferedWriter(fw);

            Iterator it = VPVertices.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry pair = (Map.Entry) it.next();
                String id = pair.getKey().toString();
                String clusterIds = ((SerialVertexComponents) pair.getValue()).getClusterIds();
//                boolean isCenter = ((SerialVertexComponents) pair.getValue()).getIsCenter();
                it.remove(); // avoids a ConcurrentModificationException
//                String line= id+","+clusterId.toString()+","+isCenter+"\n";

                String line= id+","+clusterIds+"\n";
//                if (clusterIds.contains(","))
//                    System.out.print(line);
                bw.write(line);
            }
            bw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return true;
    }
}


