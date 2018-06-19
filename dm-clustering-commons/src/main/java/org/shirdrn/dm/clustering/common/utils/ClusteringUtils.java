package org.shirdrn.dm.clustering.common.utils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.util.*;
import java.util.Map.Entry;

import com.google.common.collect.Lists;
import org.shirdrn.dm.clustering.common.ClusterPoint;
import org.shirdrn.dm.clustering.common.Point2D;

public class ClusteringUtils {
    //private  int cluster;


    public static output print2DClusterPoints(Map<Integer, Set<ClusterPoint<Point2D>>> clusterPoints) throws IOException {
        List<output> outPut=new ArrayList();
        output out=new output(0,0,0);
        String path = "E:\\JAVA Work\\src\\main\\data\\outlier\\cluster.txt";
        File file =new File(path);
        InputStream is =new FileInputStream(file);
        if(!file.exists()){
            file.createNewFile();
        }
        FileOutputStream fos =new FileOutputStream(file);
        //获得输出流
        OutputStreamWriter bos =new OutputStreamWriter(fos);
        BufferedWriter bw = new BufferedWriter(bos);
        //遍历输出
        int cluster=0;
        String a = "";
        Iterator<Entry<Integer, Set<ClusterPoint<Point2D>>>> iter = clusterPoints.entrySet().iterator();
        while(iter.hasNext()) {
            Entry<Integer, Set<ClusterPoint<Point2D>>> entry = iter.next();
            int clusterId = entry.getKey();
            cluster=clusterId;
            for(ClusterPoint<Point2D> cp : entry.getValue()) {
                System.out.println(cp.getPoint().getX() + "," + cp.getPoint().getY() + "," + clusterId);
                a+=cp.getPoint().getX()+","+cp.getPoint().getY() + "," + clusterId;
                a+="\r\n";
            }
        }
        out.setCluster(cluster);
        bw.write(a);
        bw.flush();
        bw.close();
        return out;
    }


}
