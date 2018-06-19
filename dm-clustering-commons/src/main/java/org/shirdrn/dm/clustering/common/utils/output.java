package org.shirdrn.dm.clustering.common.utils;

/**
 * Created by HMH on 2018/4/9.
 */
public class output {
    private int cluster;
    private double Kdistance;
    private int outlier;

    public output(int cluster, double kdistance,int outlier) {

        this.cluster = cluster;
        this.Kdistance = kdistance;
        this.outlier=outlier;
    }

    public int getCluster() {
        return cluster;
    }

    public void setCluster(int cluster) {
        this.cluster = cluster;
    }

    public void setKdistance(double kdistance) {
        Kdistance = kdistance;
    }

    public double getKdistance() {
        return Kdistance;
    }

    public int getOutlier() {
        return outlier;
    }

    public void setOutlier(int outlier) {
        this.outlier = outlier;
    }
}

