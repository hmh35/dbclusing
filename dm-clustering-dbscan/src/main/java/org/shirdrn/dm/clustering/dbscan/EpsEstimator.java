package org.shirdrn.dm.clustering.dbscan;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.shirdrn.dm.clustering.common.ClusteringResult;
import org.shirdrn.dm.clustering.common.DistanceCache;
import org.shirdrn.dm.clustering.common.NamedThreadFactory;
import org.shirdrn.dm.clustering.common.Point2D;
import org.shirdrn.dm.clustering.common.utils.ClusteringUtils;
import org.shirdrn.dm.clustering.common.utils.FileUtils;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * Estimate value of radius <code>Eps</code> for DBSCAN clustering algorithm.
 *  
 * @author yanjun
 */
public class EpsEstimator {

	private static final Log LOG = LogFactory.getLog(EpsEstimator.class);
	private final List<Point2D> allPoints = Lists.newArrayList();//所有的点
	private final DistanceCache distanceCache;//距离
	private int k = 4;
	private int parallism = 5;//并发器
	private final ExecutorService executorService;//线程池
	private CountDownLatch latch;
	private final List<KDistanceCalculator> calculators = Lists.newArrayList();//计算器
	private int taskIndex = 0;
	private int calculatorQueueSize = 200;//计算器队列大小
	private volatile boolean completeToAssignTask = false;//完成分配任务
	private boolean isOutputKDsitance = true;//是否输出K-distance
	
	public EpsEstimator() {
		this(4, 5);
	}
	
	public EpsEstimator(int k, int parallism) {
		super();
		this.k = k;
		this.parallism = parallism;
		distanceCache = new DistanceCache(Integer.MAX_VALUE);
		latch = new CountDownLatch(parallism);
		executorService = Executors.newCachedThreadPool(new NamedThreadFactory("KDCALC"));
		LOG.info("Config: k=" + k + ", parallism=" + parallism);
	}
	
	public Iterator<Point2D> allPointIterator() {
		return allPoints.iterator();
	}
	
	public void setOutputKDsitance(boolean isOutputKDsitance) {
		this.isOutputKDsitance = isOutputKDsitance;
	}
	//计算K-距离
	public EpsEstimator computeKDistance(File... files) {
			// 解析实例文字
			FileUtils.read2DPointsFromFiles(allPoints, "[\t,;\\s]+", files);
			// 计算K-距离
			try {
				for (int i = 0; i < parallism; i++) {
					KDistanceCalculator calculator = new KDistanceCalculator(calculatorQueueSize);
					calculators.add(calculator);
					executorService.execute(calculator);
					LOG.info("k-distance calculator started: " + calculator);
				}
				
				// convert Point2D to KPoint2D
				for(int i=0; i<allPoints.size(); i++) {
					Point2D p = allPoints.get(i);
					KPoint2D kp = new KPoint2D(p);
					Collections.replaceAll(allPoints, p, kp);
				}
				// 分配点任务
				for(int i=0; i<allPoints.size(); i++) {
					while(true) {
						KDistanceCalculator calculator = getCalculator();
						Task task = new Task((KPoint2D) allPoints.get(i), i);
						if(!calculator.q.offer(task)) {
							continue;
						}
						LOG.debug("Assign Point[" + task.kp + "] to " + calculator);
						break;
					}
				}
				LOG.info("Input: totalPoints=" + allPoints.size());
				
				completeToAssignTask = true;
			} catch(Exception e) {
				throw Throwables.propagate(e);
			} finally {
				try {
					latch.await();
				} catch (InterruptedException e) { }
				LOG.info("Shutdown executor service: " + executorService);
				executorService.shutdown();
			}
		return this;
	}
	
	public void estimateEps() throws IOException {
		// sort k-distance s
		Collections.sort(allPoints, new Comparator<Point2D>() {

			@Override
			public int compare(Point2D o1, Point2D o2) {
				KPoint2D kp1 = (KPoint2D) o1;
				KPoint2D kp2 = (KPoint2D) o2;
				double diff = kp1.kDistance.doubleValue() - kp2.kDistance.doubleValue();
				if(diff == 0.0) {
					return 0;
				}
				return diff < 0 ? -1 : 1;
			}
			
		});
		
		if(isOutputKDsitance) {
			String path = "E:\\JAVA Work\\Step\\src\\main\\data\\core.txt";
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
	        String a = "";
			for(int i=0; i<allPoints.size(); i++) {
				KPoint2D kp = (KPoint2D) allPoints.get(i);
				System.out.println(i + "\t" + kp.kDistance);
		    		a+=kp.kDistance;
		    		a+="\r\n";
              }
		        bw.write(a);
		        bw.flush();
		        bw.close();}
			}


	private KDistanceCalculator getCalculator() {
		int index = taskIndex++ % parallism;
		return calculators.get(index);
	}

	private class KDistanceCalculator extends Thread {
		
		private final Log LOG = LogFactory.getLog(KDistanceCalculator.class);
		private final BlockingQueue<Task> q;
		
		public KDistanceCalculator(int qsize) {
			q = new LinkedBlockingQueue<Task>(qsize);
		}
		
		@Override
		public void run() {
			try {
				while(!completeToAssignTask) {
					try {
						while(!q.isEmpty()) {
							Task task = q.poll();
							final KPoint2D p1 = (KPoint2D) task.kp;
							final TreeSet<Double> sortedDistances = Sets.newTreeSet(new Comparator<Double>() {

								@Override
								public int compare(Double o1, Double o2) {
									double diff = o1 - o2;
									if(diff > 0) {
										return -1;
									}
									if(diff < 0) {
										return 1;
									}
									return 0;
								}
								
							});
							for (int i = 0; i < allPoints.size(); i++) {
								if(task.pos != i) {
									final Point2D p2 = allPoints.get(i);
									Double distance = distanceCache.computeDistance((Point2D) p1, (Point2D) p2);
									
									if(!sortedDistances.contains(distance)) {
										sortedDistances.add(distance);
									}
									if(sortedDistances.size() > k) {
										Iterator<Double> iter = sortedDistances.iterator();
										iter.next();
										// remove (k+1)th minimum distance
										iter.remove();
									}
								}
							}						
							// collect k-distance
							p1.kDistance = sortedDistances.iterator().next();
							LOG.debug("Processed, point=(" + p1 + "), k-distance=" + p1.kDistance);
						}
						Thread.sleep(100);
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			} finally {
				latch.countDown();
				LOG.info("k-distance calculator exited: " + this);
			}
		}
		
	}
	
	private class Task {
		
		private final KPoint2D kp;
		private final int pos;
		
		public Task(KPoint2D kp, int pos) {
			super();
			this.kp = kp;
			this.pos = pos;
		}
	}
	
	/**
	 * k-distance point
	 * 
	 * @author yanjun
	 */
	private class KPoint2D extends Point2D {

		private Double kDistance = 0.0;
		
		public KPoint2D(Point2D point) {
			super(point.getX(), point.getY());
		}
		
		public KPoint2D(Double x, Double y) {
			super(x, y);
		}
		
		@Override
		public int hashCode() {
			return super.hashCode();
		}
		
		@Override
		public boolean equals(Object obj) {
			return super.equals(obj);
		}
		
	}
	
	public DistanceCache getDistanceCache() {
		return distanceCache;
	}
	/*public static void main(String[] args) {
		EpsEstimator epsEstimator=new EpsEstimator(4, 5);
		File inputFile=new File(FileUtils.getDataRootDir(), "hmh.txt");
		epsEstimator.computeKDistance(inputFile).estimateEps();
		Iterator<Point2D> iter = epsEstimator.allPointIterator();
		while(iter.hasNext()) {
			Point2D p = iter.next();
			while(!taskQueue.offer(p)) {
				Thread.sleep(10);
			}
			LOG.debug("Added to taskQueue: " + p);
		}
		
		// generate sorted k-distances sequences
//		int minPts = 4;
//		double eps = 0.0025094814205335555;
//		double eps = 0.004417483559674606;
//		double eps = 0.006147849217403014;
		
		int minPts = 8;
//		double eps = 0.004900098978598581;
		double eps = 0.009566439044911;
//		double eps = 0.013621050253196359;
		
		DBSCANClustering c = new DBSCANClustering(minPts, 8);
		c.setInputFiles(new File(FileUtils.getDataRootDir(), "xy_zfmx.txt"));
		c.getEpsEstimator().setOutputKDsitance(false);
		c.generateSortedKDistances();
		
		// execute clustering procedure
		c.setEps(eps);
		c.setMinPts(4);
		c.clustering();
		
		System.out.println("== Clustered points ==");
		ClusteringResult<Point2D> result = c.getClusteringResult();
		ClusteringUtils.print2DClusterPoints(result.getClusteredPoints());
		
		// print outliers
		int outliersClusterId = -1;
		System.out.println("== Outliers ==");
		for(Point2D p : c.getOutliers()) {
			System.out.println(p.getX() + "," + p.getY() + "," + outliersClusterId);
		}
	}
	*/
}
