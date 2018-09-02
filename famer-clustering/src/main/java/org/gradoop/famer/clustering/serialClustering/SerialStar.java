package org.gradoop.famer.clustering.serialClustering;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.gradoop.famer.clustering.serialClustering.util.SerialVertexComponents;

/**
 * The serial implementation of Star algorithm
 */
public class SerialStar {


    public boolean doSerialClustering(int priority, int type, String verticesInputPath, String edgesInputPath, String outputDir) {
        HashMap<String, SerialVertexComponents> vertices = new HashMap<String, SerialVertexComponents>();

        BufferedReader br = null;

        try {
            String line;
            br = new BufferedReader(new FileReader(verticesInputPath));
            while ((line = br.readLine()) != null) {
                SerialVertexComponents svc = new SerialVertexComponents(Long.parseLong(line.split(",")[1]), Long.parseLong(line.split(",")[1]), false);
                svc.setId(line.split(",")[0]);
                vertices.put(line.split(",")[0], svc);
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
                Double degree = Double.parseDouble(line.split(",")[2]);
                SerialVertexComponents svc = vertices.get(srcId);
                svc.addToNeighborList(trgtId);
                svc.AddDegree(1);
                svc.AddSimDegree(degree);
                svc = vertices.get(trgtId);
                svc.addToNeighborList(srcId);
                svc.AddDegree(1);
                svc.AddSimDegree(degree);
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
        SerialVertexComponents[] verticesArray = new SerialVertexComponents[vertices.size()];
        Set entries = vertices.entrySet();
        Iterator entriesIterator = entries.iterator();
        int i = 0;
        while(entriesIterator.hasNext()){

            Map.Entry mapping = (Map.Entry) entriesIterator.next();
            verticesArray[i] = (SerialVertexComponents) mapping.getValue();
            double sumSimDegree = verticesArray [i].getSimDegree();
            Integer sumDegree = verticesArray [i].getDegree();
            if (type == 1)
                verticesArray[i].SetSimDegree(sumDegree.doubleValue());
            else if (type == 2) {
                if (sumDegree != 0)
                    verticesArray[i].SetSimDegree(sumSimDegree / sumDegree);
                else
                    verticesArray[i].SetSimDegree(sumSimDegree);
            }
//            System.out.println(verticesArray[i].getVertexPrio()+" "+verticesArray[i].getSimDegree());

            i++;
        }
        final int prio = priority;
        java.util.Arrays.sort(verticesArray, new Comparator<SerialVertexComponents>() {
            public int compare(SerialVertexComponents in1, SerialVertexComponents in2) {
                if (in1.getSimDegree() > in2.getSimDegree())
                    return -1;
                if (in1.getSimDegree() < in2.getSimDegree())
                    return 1;
                if ((in1.getClusterId() > in2.getClusterId() && prio == 1) || (in1.getClusterId() < in2.getClusterId() && prio == 0))
                    return -1;
                if ((in1.getClusterId() > in2.getClusterId() && prio == 0) || (in1.getClusterId() < in2.getClusterId() && prio == 1))
                    return 1;
                return 0;
            }
        });
        for (i = 0; i < verticesArray.length; i++) {
            SerialVertexComponents center = verticesArray[i];
            if (vertices.get(verticesArray[i].getId()).getClusterIds().equals("")) {
                String clusterId = verticesArray[i].getVertexPrio().toString();
                center.setClusterIds(clusterId);
                vertices.remove(verticesArray[i].getId());
                vertices.put(verticesArray[i].getId(), center);
                Collection<String> neighborList = verticesArray[i].getNeighborList();
                for (String neighborId : neighborList) {
                    SerialVertexComponents svc = vertices.get(neighborId);
//                    if(svc.getId().equals("06872a9c-a166-4e12-8ae9-e23a63184e5d")) {
//                        System.out.println(i + " " + clusterId);
//                        svc.setClusterIds(clusterId);
//                        System.out.println(svc.getClusterIds()+" svc.getClusterIds()");
//                    }
//                    else
                    svc.setClusterIds(clusterId);
                    vertices.remove(neighborId);
                    vertices.put(neighborId, svc);
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

            Iterator it = vertices.entrySet().iterator();
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


