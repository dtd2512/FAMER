package org.gradoop.famer.clustering.serialClustering;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.gradoop.famer.clustering.serialClustering.util.SerialEdgeComponents;
import org.gradoop.famer.clustering.serialClustering.util.SerialVertexComponents;

/**
 * The serial implementation of new Merge Center algorithm
 */
public class SerialNewMergeCenter {

    public boolean doSerialClustering(int priority, String verticesInputPath, String edgesInputPath, String outputDir) {
        HashMap<String, SerialVertexComponents> vertices = new HashMap<String, SerialVertexComponents>();
        List<SerialEdgeComponents> edgesList = new ArrayList<SerialEdgeComponents>();

        BufferedReader br = null;

        try {
            String line;
            br = new BufferedReader(new FileReader(verticesInputPath));
            while ((line = br.readLine()) != null) {
                SerialVertexComponents svc = new SerialVertexComponents(Long.parseLong(line.split(",")[1]), Long.parseLong(line.split(",")[1]), false);
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
                SerialEdgeComponents sec = new SerialEdgeComponents(line.split(",")[0], line.split(",")[1], Double.parseDouble(line.split(",")[2]));
                edgesList.add(sec);
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
        SerialEdgeComponents[] edges = edgesList.toArray(new SerialEdgeComponents[edgesList.size()]);

        java.util.Arrays.sort(edges, new Comparator<SerialEdgeComponents>() {
            public int compare(SerialEdgeComponents in1, SerialEdgeComponents in2) {
                if (in1.getDegree() > in2.getDegree())
                    return -1;
                if (in1.getDegree() < in2.getDegree())
                    return 1;
                return 0;
            }
        });
        System.out.println("sdsdsd");
        for (int i = 0; i < edges.length; i++) {

            SerialVertexComponents src = vertices.get(edges[i].getSrcId());
            SerialVertexComponents trgt = vertices.get(edges[i].getTargtId());
            Long srcVertexPrio = src.getVertexPrio();
            Long trgtVertexPrio = trgt.getVertexPrio();
            boolean isChanged = false;
            if (!src.getIsAssigned() && !trgt.getIsAssigned()) {
                if (priority == 0) {
                    if(srcVertexPrio < trgtVertexPrio)
                        src.setIsCenter(true);
                    else
                        trgt.setIsCenter(true);
                }
                else if (priority == 1) {
                    if(srcVertexPrio > trgtVertexPrio)
                        src.setIsCenter(true);
                    else
                        trgt.setIsCenter(true);
                }
                if(src.getIsCenter()) {
//                    System.out.println(srcVertexPrio);
                    src.setClusterId(srcVertexPrio);
                    trgt.setClusterId(srcVertexPrio);
                }
                else if(trgt.getIsCenter()){
//                    System.out.println(trgtVertexPrio);
                    src.setClusterId(trgtVertexPrio);
                    trgt.setClusterId(trgtVertexPrio);
                }
                src.setIsAssigned(true);
                trgt.setIsAssigned(true);
                isChanged = true;
            }
            else if (!src.getIsAssigned() && trgt.getIsCenter()) {
                src.setClusterId(trgt.getClusterId());
                src.setIsAssigned(true);
                isChanged = true;
            }
            else if (!trgt.getIsAssigned() && src.getIsCenter()) {
                trgt.setClusterId(src.getClusterId());
                trgt.setIsAssigned(true);
                isChanged = true;
            }
            if (isChanged) {
                vertices.remove(edges[i].getSrcId());
                vertices.remove(edges[i].getTargtId());
                vertices.put(edges[i].getSrcId(), src);
                vertices.put(edges[i].getTargtId(), trgt);
            }
        }
        for (int i = 0; i < edges.length; i++) {

            SerialVertexComponents src = vertices.get(edges[i].getSrcId());
            SerialVertexComponents trgt = vertices.get(edges[i].getTargtId());
            Long srcVertexPrio = src.getVertexPrio();
            Long trgtVertexPrio = trgt.getVertexPrio();
            boolean isChanged = false;
            if (!src.getIsAssigned() && !trgt.getIsAssigned()) {
                System.out.println("1");
            }
            else if (!src.getIsAssigned() && trgt.getIsCenter()) {
                System.out.println("2");

            }
            else if (!trgt.getIsAssigned() && src.getIsCenter()) {
                System.out.println("3");
            }
            else if (src.getIsCenter() && trgt.getIsCenter()) {
                Long clusterId, modifyingClusterId;
                clusterId = modifyingClusterId = 0L;
                if (priority == 0) {
                    if (src.getClusterId() < trgt.getClusterId()){
                        clusterId = src.getClusterId();
                        modifyingClusterId = trgt.getClusterId();
                        trgt.setClusterId(clusterId);
                    }
                    else if (src.getClusterId() > trgt.getClusterId()){
                        clusterId = trgt.getClusterId();
                        modifyingClusterId = src.getClusterId();
                        src.setClusterId(clusterId);
                    }
                }
                else if (priority == 1) {
                    if (src.getClusterId() > trgt.getClusterId()){
                        clusterId = src.getClusterId();
                        modifyingClusterId = trgt.getClusterId();
                        trgt.setClusterId(clusterId);
                    }
                    else if (src.getClusterId() < trgt.getClusterId()){
                        clusterId = trgt.getClusterId();
                        modifyingClusterId = src.getClusterId();
                        src.setClusterId(clusterId);
                    }
                }
                if(modifyingClusterId != clusterId) {
                    Iterator it = vertices.entrySet().iterator();
                    while (it.hasNext()) {
                        Map.Entry pair = (Map.Entry) it.next();
                        Long cid = ((SerialVertexComponents) pair.getValue()).getClusterId();
                        //                    if (cid.equals(modifyingClusterId)){
                        if (cid == modifyingClusterId) {
                            SerialVertexComponents svc = (SerialVertexComponents) pair.getValue();
                            svc.setClusterId(clusterId);
                            pair.setValue(svc);
                        }
                    }
                }
                isChanged = true;
            }
            else if (src.getIsCenter() || trgt.getIsCenter()) {
                Long clusterId, modifyingClusterId;
                if (trgt.getIsCenter()){
                    clusterId = trgt.getClusterId();
                    modifyingClusterId = src.getClusterId();
                    src.setClusterId(clusterId);
//                    vertices.remove(edges[i].getSrcId());
//                    vertices.put(edges[i].getSrcId(), src);
                }
                else {
                    clusterId = src.getClusterId();
                    modifyingClusterId = trgt.getClusterId();
                    trgt.setClusterId(clusterId);
//                    vertices.remove(edges[i].getTargtId());
//                    vertices.put(edges[i].getTargtId(), trgt);

                }
                if(modifyingClusterId != clusterId) {
                    Iterator it = vertices.entrySet().iterator();
                    while (it.hasNext()) {
                        Map.Entry pair = (Map.Entry) it.next();
                        Long cid = ((SerialVertexComponents) pair.getValue()).getClusterId();
                        if (cid == modifyingClusterId) {
                            SerialVertexComponents svc = (SerialVertexComponents) pair.getValue();
                            svc.setClusterId(clusterId);
                            pair.setValue(svc);
                        }
                    }
                }
                isChanged = true;
            }

            if (isChanged) {
                vertices.remove(edges[i].getSrcId());
                vertices.remove(edges[i].getTargtId());
                vertices.put(edges[i].getSrcId(), src);
                vertices.put(edges[i].getTargtId(), trgt);
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
                Long clusterId = ((SerialVertexComponents) pair.getValue()).getClusterId();
                it.remove(); // avoids a ConcurrentModificationException
                boolean isCenter = ((SerialVertexComponents) pair.getValue()).getIsCenter();
//                String line= id+","+clusterId.toString()+","+isCenter+"\n";
                String line= id+","+clusterId.toString()+"\n";

                bw.write(line);
            }
            bw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return true;
    }
}


