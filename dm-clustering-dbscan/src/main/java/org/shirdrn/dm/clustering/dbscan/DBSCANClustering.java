

/**
 * DBSCAN clustering algorithm implementation.
 *
 * @author yanjun
 */
package org.shirdrn.dm.clustering.dbscan;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import com.google.common.collect.Lists;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.shirdrn.dm.clustering.common.ClusterPoint;
import org.shirdrn.dm.clustering.common.ClusterPoint2D;
import org.shirdrn.dm.clustering.common.Clustering2D;
import org.shirdrn.dm.clustering.common.ClusteringResult;
import org.shirdrn.dm.clustering.common.NamedThreadFactory;
import org.shirdrn.dm.clustering.common.Point2D;
import org.shirdrn.dm.clustering.common.utils.ClusteringUtils;
import org.shirdrn.dm.clustering.common.utils.FileUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.shirdrn.dm.clustering.common.utils.KPoint;
import org.shirdrn.dm.clustering.common.utils.output;

/**
 * DBSCAN clustering algorithm implementation.
 *
 * @author yanjun
 */
public class DBSCANClustering extends Clustering2D {
	private final static List<KPoint> allDatas = Lists.newArrayList();
	private static final Log LOG = LogFactory.getLog(DBSCANClustering.class);
	private double eps;
	private int minPts;
	private final EpsEstimator epsEstimator;
	private final Map<Point2D, Set<Point2D>> corePointWithNeighbours = Maps.newHashMap();
	private final Set<Point2D> outliers = Sets.newHashSet();
	private final CountDownLatch latch;
	private final ExecutorService executorService;
	private final BlockingQueue<Point2D> taskQueue;
	private volatile boolean completed = false;
	private int clusterCount;

	public DBSCANClustering(int minPts, int parallism) {
		super(parallism);
		Preconditions.checkArgument(minPts > 0, "Required: minPts > 0!");
		this.minPts = minPts;
		epsEstimator = new EpsEstimator(minPts, parallism);
		latch = new CountDownLatch(parallism);
		executorService = Executors.newCachedThreadPool(new NamedThreadFactory("CORE"));
		taskQueue = new LinkedBlockingQueue<Point2D>();
		LOG.info("Config: minPts=" + minPts + ", parallism=" + parallism);
	}

	public void generateSortedKDistances() throws IOException {
		Preconditions.checkArgument(inputFiles != null, "inputFiles == null");
		epsEstimator.computeKDistance(inputFiles).estimateEps();
	}

	@Override
	public void clustering() {
		// recognize core points
		try {
			for (int i = 0; i < parallism; i++) {
				CorePointCalculator calculator = new CorePointCalculator();
				executorService.execute(calculator);
				LOG.info("Core point calculator started: " + calculator);
			}

			Iterator<Point2D> iter = epsEstimator.allPointIterator();
			while(iter.hasNext()) {
				Point2D p = iter.next();
				while(!taskQueue.offer(p)) {
					Thread.sleep(10);
				}
				LOG.debug("Added to taskQueue: " + p);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				completed = true;
				latch.await();
			} catch (InterruptedException e) { }
			LOG.info("Shutdown executor service: " + executorService);
			executorService.shutdown();
		}
		LOG.info("Point statistics: corePointSize=" + corePointWithNeighbours.keySet().size());
		// join connected core points
		LOG.info("Joining connected core points ...");
		final Map<Point2D, Set<Point2D>> clusteringPoints = Maps.newHashMap();
		Set<Point2D> corePoints = Sets.newHashSet(corePointWithNeighbours.keySet());
		while(true) {
			Set<Point2D> set = Sets.newHashSet();
			Iterator<Point2D> iter = corePoints.iterator();
			if(iter.hasNext()) {
				Point2D p = iter.next();
				iter.remove();
				Set<Point2D> connectedPoints = joinConnectedCorePoints(p, corePoints);
				set.addAll(connectedPoints);
				while(!connectedPoints.isEmpty()) {
					connectedPoints = joinConnectedCorePoints(connectedPoints, corePoints);
					set.addAll(connectedPoints);
				}
				clusteringPoints.put(p, set);
			} else {
				break;
			}
		}
		LOG.info("Connected core points computed.");

		// process outliers
		Iterator<Point2D> iter = outliers.iterator();
		while(iter.hasNext()) {
			Point2D np = iter.next();
			if(corePointWithNeighbours.containsKey(np)) {
				iter.remove();
			} else {
				for(Set<Point2D> set : corePointWithNeighbours.values()) {
					if(set.contains(np)) {
						iter.remove();
						break;
					}
				}
			}
		}

		// generate clustering result
		Iterator<Entry<Point2D, Set<Point2D>>> coreIter = clusteringPoints.entrySet().iterator();
		int id = 0;
		while(coreIter.hasNext()) {
			Entry<Point2D, Set<Point2D>> core = coreIter.next();
			Set<Point2D> set = Sets.newHashSet();
			set.add(core.getKey());
			set.addAll(corePointWithNeighbours.get(core.getKey()));
			for(Point2D p : core.getValue()) {
				set.addAll(core.getValue());
				set.addAll(corePointWithNeighbours.get(p));
			}

			Set<ClusterPoint<Point2D>> clusterSet = Sets.newHashSet();
			for(Point2D p : set) {
				clusterSet.add(new ClusterPoint2D(p, id));
			}
			clusteredPoints.put(id, clusterSet);
			++id;
		}

		LOG.info("Finished clustering: clusterCount=" + clusterCount + ", outliersCount=" + outliers.size());
	}

	private Set<Point2D> joinConnectedCorePoints(Set<Point2D> connectedPoints, Set<Point2D> leftCorePoints) {
		Set<Point2D> set = Sets.newHashSet();
		for(Point2D p1 : connectedPoints) {
			set.addAll(joinConnectedCorePoints(p1, leftCorePoints));
		}
		return set;
	}

	private Set<Point2D> joinConnectedCorePoints(Point2D p1, Set<Point2D> leftCorePoints) {
		Set<Point2D> set = Sets.newHashSet();
		for(Point2D p2 : leftCorePoints) {
			double distance = epsEstimator.getDistanceCache().computeDistance(p1, p2);
			if(distance <= eps) {
				// join 2 core points to the same cluster
				set.add(p2);
			}
		}
		// remove connected points
		leftCorePoints.removeAll(set);
		return set;
	}

	public void setMinPts(int minPts) {
		this.minPts = minPts;
	}

	public void setEps(double eps) {
		this.eps = eps;
	}

	/**
	 * Compute core point and neighbourhood points.
	 *
	 * @author yanjun
	 */
	private final class CorePointCalculator extends Thread {

		private final Log LOG = LogFactory.getLog(CorePointCalculator.class);
		private int processedPoints;

		@Override
		public void run() {
			try {
				Thread.sleep(1000);
				while(true) {
					while(!taskQueue.isEmpty()) {
						Point2D p1 = taskQueue.poll();
						++processedPoints;
						Set<Point2D> set = Sets.newHashSet();
						Iterator<Point2D> iter = epsEstimator.allPointIterator();
						while(iter.hasNext()) {
							Point2D p2 = iter.next();
							if(!p2.equals(p1)) {
								double distance = epsEstimator.getDistanceCache().computeDistance(p1, p2);
								// collect a point belonging to the point p1
								if(distance <= eps) {
									set.add(p2);
								}
							}
						}
						// decide whether p1 is core point
						if(set.size() >= minPts) {
							corePointWithNeighbours.put(p1, set);
							LOG.debug("Decide core point: point" + p1 + ", set=" + set);
						} else {
							// here, perhaps a point was wrongly put into outliers set
							// afterwards we should remedy outliers set
							if(!outliers.contains(p1)) {
								outliers.add(p1);
							}
						}

					}

					Thread.sleep(100);

					if(taskQueue.isEmpty() && completed) {
						break;
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				latch.countDown();
				LOG.info("Calculator exit, STAT: [id=" + this + ", processedPoints=" + processedPoints + "]");
			}
		}
	}

	public EpsEstimator getEpsEstimator() {
		return epsEstimator;
	}

	public Set<Point2D> getOutliers() {
		return outliers;
	}

	public static void main(String[] args) throws IOException {
		// generate sorted k-distances sequences
		/*List<output> outputs=new ArrayList<output>();
		File file = new File(FileUtils.getDataRootDir(), "in.txt");
		if (file != null) {
			FileUtils.readKPointsFromFiles(allDatas, "[\t,;\\s]", file);
			System.out.println("数据读取成功");
		}
		for(int i=0;i<allDatas.size();i+=10){
			int minPts=allDatas.get(i).getMinPts();
			double eps=allDatas.get(i).getkDistance();
			output out=new output(0,0,0);*/

		int minPts=8;
		double eps=0.000999276086690;
		DBSCANClustering c = new DBSCANClustering(minPts, 8);
		c.setInputFiles(new File(FileUtils.getDataRootDir(), "台江-30-2282.txt"));
		c.getEpsEstimator().setOutputKDsitance(true);
		c.generateSortedKDistances();

		// execute clustering procedure
		c.setEps(eps);
		c.setMinPts(minPts);
		c.clustering();

		System.out.println("== Clustered points ==");
		ClusteringResult<Point2D> result = c.getClusteringResult();
		ClusteringUtils.print2DClusterPoints(result.getClusteredPoints());
			/*out=ClusteringUtils.print2DClusterPoints(result.getClusteredPoints());
			out.setKdistance(eps);
			out.setOutlier(c.getOutliers().size());
			outputs.add(out);*/


		// print outliers
		int outliersClusterId = -1;
		System.out.println("== Outliers =="+c.getOutliers().size());
		String path = "E:\\JAVA Work\\src\\main\\data\\outlier\\outlier.txt";
		File file1 =new File(path);
		InputStream is =new FileInputStream(file1);
		if(!file1.exists()){
			file1.createNewFile();
		}
		FileOutputStream fos =new FileOutputStream(file1);
		//获得输出流
		OutputStreamWriter bos =new OutputStreamWriter(fos);
		BufferedWriter bw = new BufferedWriter(bos);
		//遍历输出
		String a = "";
		for(Point2D p : c.getOutliers()) {
			//System.out.println(p.getX() + "," + p.getY() + "," + outliersClusterId);
			a+=p.getX() + "," + p.getY() + "," + outliersClusterId;
			a+="\r\n";
		}

		bw.write(a);
		bw.flush();
		bw.close();
	}
		/*String path = "E:\\JAVA Work\\src\\main\\data\\out.txt";
		File file2 =new File(path);
		InputStream is =new FileInputStream(file2);
		if(!file2.exists()){
			file2.createNewFile();
		}
		FileOutputStream fos1 =new FileOutputStream(file2);
		//获得输出流
		OutputStreamWriter bos1 =new OutputStreamWriter(fos1);
		BufferedWriter bw1 = new BufferedWriter(bos1);
		//遍历输出
		String a = "";
		for(output p : outputs) {
			//System.out.println(p.getX() + "," + p.getY() + "," + outliersClusterId);
			//a+=p.getX() + "," + p.getY() + "," + outliersClusterId;
			a+=p.getKdistance()+","+p.getCluster()+","+p.getOutlier();
			a+="\r\n";
		}

		bw1.write(a);
		bw1.flush();
		bw1.close();*/

}


