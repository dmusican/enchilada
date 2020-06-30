package edu.carleton.enchilada.analysis.clustering;

/* Cluster validation (Dunn index, silhouettes)
 * Michael Murphy 2014, University of Toronto
 */

import edu.carleton.enchilada.database.Database;
import gnu.trove.iterator.TIntShortIterator;
import gnu.trove.map.hash.TIntFloatHashMap;
import gnu.trove.map.hash.TIntShortHashMap;
import edu.carleton.enchilada.gui.FileDialogPicker;

import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.TreeSet;

import javax.swing.JDialog;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.SwingWorker;

import edu.carleton.enchilada.collection.Collection;
import edu.carleton.enchilada.ATOFMS.ParticleInfo;
import edu.carleton.enchilada.analysis.BinnedPeakList;
import edu.carleton.enchilada.analysis.DistanceMetric;
import edu.carleton.enchilada.database.CollectionCursor;
import edu.carleton.enchilada.database.Database.ArrayCursor;
import edu.carleton.enchilada.errorframework.ErrorLogger;

public class ClusterValidation {
	private static JDialog updateBox;
	private static SwingWorker<Void,Void> worker;
	private static boolean runningFlag = false; // having both of these is probably redundant
	private static boolean interruptFlag = false;
	
	private static final float ZERO_THRESH = 0.0000001f; // for eliminating duplicates
	
	private static void showUpdateBox() {
		updateBox = new JDialog(new JFrame(), "Calculating...",true);
		JLabel updateLabel = new JLabel("Calculating validation indices...");
		updateBox.setBounds(0,0,100,250);
		updateBox.add(updateLabel);
		updateBox.setDefaultCloseOperation(JDialog.DISPOSE_ON_CLOSE);
		updateBox.addWindowListener(new WindowAdapter() {
			@Override
			public void windowClosed(WindowEvent e) {
				interruptFlag = true;
				worker.cancel(true);
			}
		});
		updateBox.pack();
		updateBox.validate();
		updateBox.setVisible(true);
	}
	
	public static BinnedPeakList getClusterCentroid(Database db, Collection collection) {
		CollectionCursor curs = null;
		
		try {
			curs = db.getCentroidCursor(collection);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		curs.next();
		ParticleInfo centroid = curs.getCurrent(); // this cursor should only return one result anyway
		curs.close();
		
		return centroid.getBinnedList();
	}
	
	private static void closeUpdateBox() {
		updateBox.dispose();
	}
	
	public static void run(
		final Database db,
		final Collection parent, 
		final DistanceMetric dMetric,
		final boolean posNegNorm, 
		final ArrayList<Integer> excludeClusters, 
		final ValidityTest vTest,
		final SampleType sampleType,
		final float frac,
		final int iter)
	{
		// doing this as a singleton would have been a good idea. but this works too
		if (runningFlag) {
			System.out.println("Cluster validation task already running.");
			return;
		}
		runningFlag = true;
		interruptFlag = false;
		
		worker = new SwingWorker<Void,Void>(){
			private String output;
			
			@Override
			protected Void doInBackground() {
				try {
					System.out.println();
					System.out.println("-- Cluster Validation --");
					System.out.println();
					
					long startTime = System.nanoTime();
					
					// retrieve cluster collection IDs
					ArrayList<Integer> childrenIds = parent.getSubCollectionIDs();
					
					// remove excluded clusters
					for (int i = 0, cid; i < childrenIds.size(); i++) {
						cid = Integer.parseInt(db.getCollectionName(childrenIds.get(i)));
						if (excludeClusters.contains(cid)) {
							childrenIds.remove(i);
							i--;
						}
					}
					int numClusters = childrenIds.size();
					//System.out.println("Number of clusters: "+numClusters);

					if (interruptFlag) { // not wholly pleased with the interrupt handling
						return null;
					}
					System.out.println("Loading cluster membership...\n");
					
					// these take in the data from each iteration
//					ArrayList<Float> iterMinInter = new ArrayList<Float>();
//					ArrayList<Float> iterMaxIntra = new ArrayList<Float>();
//					ArrayList<float[]> iterSils = new ArrayList<float[]>();
//					
					for (int currentIter = 0; currentIter < iter; currentIter++) {
						
						CollectionCursor preCurs;
						ParticleInfo p, q;
						BinnedPeakList pBinnedList, qBinnedList;
						int outerId, innerId, outerCluster, innerCluster;
						//int innerPos, outerPos;
						short idx;
						float magnitude, dist, pointSil, silInter, silIntra;
						float minInterDist = -1;
						float maxIntraDist = -1;
						float[] silIndices = new float[numClusters]; // only tracking per-cluster averages
						int[] clusterSizes = new int[numClusters];
						TIntShortHashMap clusterMapping = new TIntShortHashMap(100, 0.75f, -1, (short)-1); // map AtomIDs to indices of clusters in childrenIds

						if (interruptFlag) {
							return null;
						}
						System.out.println("Iteration: "+(currentIter+1)+" / "+iter);
						// subsampling, cluster mapping, atom-counting goes on here
						int numParticles = 0;
						if (sampleType == SampleType.RAND || sampleType == SampleType.FULL) {
							for (int cid : childrenIds) {
								idx = (short) childrenIds.indexOf(cid);
								if (sampleType == SampleType.RAND && frac < 1) {
									int sampleSize = (int) Math.ceil(db.getCollectionSize(cid)*frac); // problem *might* be here
									preCurs = db.getRandomizedCursor(db.getCollection(cid));
									int i = 0;
									while (preCurs.next() && i < sampleSize) {
										if (interruptFlag) {
											return null;
										}
										p = preCurs.getCurrent();
										clusterMapping.put(p.getID(), idx);
										clusterSizes[idx]++;
										i++;
										numParticles++;
									}
									preCurs.close();
								} else if (sampleType == SampleType.FULL){
									preCurs = db.getBPLOnlyCursor(db.getCollection(cid));
									while (preCurs.next()) {
										if (interruptFlag) {
											return null;
										}
										p = preCurs.getCurrent();
										clusterMapping.put(p.getID(), idx);
										clusterSizes[idx]++;
										numParticles++;
									}
									preCurs.close();
								}
								preCurs = null;
							}
						// halo sampling; any points not in the boundary set of the cluster get omitted from search
						// boundary set generated based on distance from own centroid and closeness to others
						// this is sort-of small angle approximation
						} else if (sampleType == SampleType.HALO || sampleType == SampleType.MEAN) {

							ArrayList<BinnedPeakList> centroids = new ArrayList<BinnedPeakList>();
							ArrayList<Float> centMags = new ArrayList<Float>();
							ArrayList<Integer> collectionSizes = new ArrayList<Integer>();
							int collectionId;
							for (int c = 0; c < numClusters; c++) {
								collectionId = childrenIds.get(c);
								BinnedPeakList centroid = getClusterCentroid(db,db.getCollection(collectionId));
								centroid.normalize(dMetric);
								centroids.add(centroid);
								centMags.add(centroid.getMagnitude(dMetric));
								collectionSizes.add(db.getCollectionSize(collectionId));
							}

							BinnedPeakList bpl;
							if (sampleType == SampleType.HALO) {
								System.out.print("Generating cluster halo sets... ");
								ArrayList<TreeSet<IntFloatPair>> subsets;
								// generate the halo set for each cluster, one at a time
								for (int c = 0; c < numClusters; c++) {
									System.out.print((c+1)+" ");
									subsets = new ArrayList<TreeSet<IntFloatPair>>();
									collectionId = childrenIds.get(c);
									preCurs = db.getBPLOnlyCursor(db.getCollection(collectionId));
									
									for (int d = 0; d < numClusters; d++) {
										subsets.add(new TreeSet<IntFloatPair>(new IntFloatComparator()));
									}
									// go through all points in cluster, compare to centroids
									while (preCurs.next()) {
										if (interruptFlag) {
											return null;
										}
										p = preCurs.getCurrent();
										bpl = p.getBinnedList();
										bpl.normalize(dMetric, posNegNorm);
										for (int d = 0; d < numClusters; d++) {
											dist = bpl.getDistance(centroids.get(d), centMags.get(d), dMetric);
											subsets.get(d).add(new IntFloatPair(p.getID(), dist));
											// trim the sets so only top (or bottom) distances remain
											if (subsets.get(d).size() > (int)Math.ceil(collectionSizes.get(c)*frac)) {
												if (c == d)
													subsets.get(d).pollFirst();
												else
													subsets.get(d).pollLast();
											}
										}
									}
									// mark each point in this cluster's halo set for inclusion; don't want duplicates
									for (int d = 0; d < numClusters; d++) {
										for (IntFloatPair v : subsets.get(d)) {
											if (!clusterMapping.containsKey(v.getInt())) {
												clusterMapping.put(v.getInt(), (short) c);
												numParticles++;
												clusterSizes[c]++;
											}
											v = null;
										}
										subsets.set(d, null);
									}
									preCurs.close();
									subsets = null;
								}
								System.out.println();
							} else if (sampleType == SampleType.MEAN) {
								System.out.println("Computing mean distances...");
								float[] avgDists = new float[numClusters];
								minInterDist = Float.MAX_VALUE;
								for (int c = 0; c < numClusters; c++) {
									collectionId = childrenIds.get(c);
									preCurs = db.getBPLOnlyCursor(db.getCollection(collectionId));
									
									// go through all points in cluster, compare to centroid
									while (preCurs.next()) {
										if (interruptFlag) {
											return null;
										}
										p = preCurs.getCurrent();
										bpl = p.getBinnedList();
										bpl.normalize(dMetric, posNegNorm);
										dist = bpl.getDistance(centroids.get(c), centMags.get(c), dMetric);
										avgDists[c] += dist;
										clusterSizes[c]++;
									}
									preCurs.close();
									preCurs = null;
									
									// inter-centroid spacing
									for (int d = c+1; d < numClusters; d++) {
										dist = centroids.get(c).getDistance(centroids.get(d), dMetric);
										if (dist < minInterDist) {
											minInterDist = dist;
										}
									}
								}
								
								maxIntraDist = -1;
								for (int c = 0; c < numClusters; c++) {
									avgDists[c] /= clusterSizes[c];
									if (avgDists[c] > maxIntraDist)
										maxIntraDist = avgDists[c];
								}

								//iterMinInter.add(minInterDist);
								//iterMaxIntra.add(maxIntraDist);
							}
						}
						
						if (sampleType != SampleType.MEAN) {
							//float[][] distData = new float[numParticles][numClusters];
							ArrayList<TIntFloatHashMap> distData = new ArrayList<TIntFloatHashMap>();
							for (int i = 0; i < numClusters; i++)
								distData.add(new TIntFloatHashMap());
							//int[] atomMapping = new int[numParticles]; // maps array indices to AtomIDs, because the latter are discontinuous

							if (interruptFlag) {
								return null;
							}
							System.out.println("Loading points into memory...");
							ArrayCursor outerCurs = db.getArrayCursor(parent);
							ArrayCursor innerCurs = db.getArrayCursor(outerCurs);

							if (interruptFlag) {
								return null;
							}
							if (vTest == ValidityTest.BOTH || vTest == ValidityTest.SIL || vTest == ValidityTest.DUNN) {
								
								minInterDist = Float.MAX_VALUE;
								maxIntraDist = -1;
								
								//outerPos = innerPos = -1;
								
								System.out.println("Computing clustering indices...");
								
								while (outerCurs.next()) {
									if (interruptFlag) {
										return null;
									}
									outerId = outerCurs.getCurrentAtomID();
									outerCluster = clusterMapping.get(outerId);
									if (outerCluster == -1) {
										continue;
									}
									System.out.println(outerId);
									//outerPos++;
									//atomMapping[outerPos] = outerId;
									pBinnedList = outerCurs.getCurrentPeakList(); // since i'm not transforming, copies aren't necessary
									pBinnedList.normalize(dMetric,posNegNorm);
									magnitude = pBinnedList.getMagnitude(dMetric);
									
									// line up the inner and outer cursors, then bump the inner ahead by one
									innerCurs.align(outerCurs);
									//innerPos = outerPos;
			
									while (innerCurs.next()) {
										if (interruptFlag) {
											return null;
										}
										innerId = innerCurs.getCurrentAtomID();
										if (innerId == outerId) // hack from before ArrayCursor was sorted
											continue;
										innerCluster = clusterMapping.get(innerId);
										if (innerCluster == -1)
											continue;
										//innerPos++;
										qBinnedList = innerCurs.getCurrentPeakList();
										qBinnedList.normalize(dMetric,posNegNorm);
										dist = qBinnedList.getDistance(pBinnedList, magnitude, dMetric);
										if (dist < ZERO_THRESH) // hack
											continue;
										// Dunn index stats, saves a bit of time by computing these on the fly
										if (outerCluster == innerCluster && dist > maxIntraDist)
											maxIntraDist = dist;
										else if (outerCluster != innerCluster && dist < minInterDist)
											minInterDist = dist;
										// Silhouette index stats
										distData.get(outerCluster).adjustOrPutValue(innerId, dist, dist);
										distData.get(innerCluster).adjustOrPutValue(outerId, dist, dist);
									}
									innerCurs.reset();
								}
			
								// compute silhouette indices from aggregated data
		//						for (int i = 0; i < distData.length; i++) {
		//							clusterId = clusterMapping.get(atomMapping[i]);
		//							if (clusterSizes[clusterId] == 1)
		//								silIntra = 0;
		//							else
		//								silIntra = distData[i][clusterId] / (clusterSizes[clusterId]-1);
		//							silInter = Float.MAX_VALUE;
		//							for (int j = 0; j < numClusters; j++)
		//								if (j != clusterId && distData[i][j] < silInter)
		//									silInter = distData[i][j] / clusterSizes[j];
		//							pointSil = (silInter - silIntra) / Math.max(silInter, silIntra);
		//							silIndices[clusterId] += pointSil;
		//						}
								
								int atomId, clusterId;
								for (TIntShortIterator it = clusterMapping.iterator(); it.hasNext();) {
									it.advance();
									clusterId = it.value();
									if (clusterId == -1)
										continue;
									atomId = it.key();
									silInter = Float.MAX_VALUE;
									silIntra = 0;
									for (int i = 0; i < numClusters; i++) {
										if (i == clusterId && clusterSizes[i] > 1) {
											silIntra = distData.get(i).get(atomId) / (clusterSizes[i]-1);
										} else if (i != clusterId && distData.get(i).get(atomId) < silInter) {
											if (clusterSizes[i] == 0) {
												throw new ArithmeticException("Zero division error: cluster "+i+" empty");
											}
											silInter = distData.get(i).get(atomId) / clusterSizes[i];
										}
									}
									pointSil = (silInter - silIntra) / Math.max(silInter, silIntra);
									silIndices[clusterId] += pointSil;
								}
								for (int i = 0; i < numClusters; i++) {
									if (clusterSizes[i] == 0) {
										throw new ArithmeticException("Zero division error: cluster "+i+" empty");
									}
									silIndices[i] /= clusterSizes[i];
									distData.set(i, null);
								}
								distData = null;
								
								//iterMinInter.add(minInterDist);
								//iterMaxIntra.add(maxIntraDist);
								//iterSils.add(silIndices);
							}
							innerCurs.close();
							outerCurs.close();
						}
	
						// reduce the iteration results; trivial if only one
						// Dunn index can only increase with subsampling - so we take the minimum
						//float minInterDist = Float.MAX_VALUE;
						//float maxIntraDist = -1;
						float averageSilhouette = 0;
						//float[] silIndices = new float[numClusters];
						float dunnIndex = 0;
						
//						for (int i = 0; i < iter; i++) {
//							if (vTest == ValidityTest.BOTH || vTest == ValidityTest.DUNN) {
//								if (iterMinInter.get(i) < minInterDist)
//									minInterDist = iterMinInter.get(i);
//								if (iterMaxIntra.get(i) > maxIntraDist)
//									maxIntraDist = iterMaxIntra.get(i);
//							}
//							if (vTest == ValidityTest.BOTH || vTest == ValidityTest.SIL) {
//								for (int j = 0; j < numClusters; j++) {
//									silIndices[j] += iterSils.get(i)[j];
//								}
//							}
//						}
						if (vTest == ValidityTest.BOTH || vTest == ValidityTest.SIL) {
							for (int i = 0; i < numClusters; i++) {
								silIndices[i] /= iter;
								averageSilhouette += silIndices[i];
							}
							averageSilhouette /= numClusters;
						}
						if (vTest == ValidityTest.BOTH || vTest == ValidityTest.DUNN) {
							dunnIndex = minInterDist / maxIntraDist;
						}
							
						// print results
						output = "-- Results --\r\n";
						if (vTest == ValidityTest.BOTH || vTest == ValidityTest.DUNN) {
							output += "Minimum inter-cluster distance: "+minInterDist+"\r\n";
							output += "Maximum intra-cluster distance: "+maxIntraDist+"\r\n";
							output += "Dunn index: "+dunnIndex+"\r\n";
						}
						if (vTest == ValidityTest.BOTH || vTest == ValidityTest.SIL) {
							for (int i = 0; i < silIndices.length; i++)
								output += "Cluster "+db.getCollectionName(childrenIds.get(i))+" silhouette: "+silIndices[i]+"\r\n";
							output += "Average silhouette: "+averageSilhouette+"\r\n";
						}
						
						System.out.println();
						System.out.println(output);
					}
					System.out.println("Time elapsed: "+(System.nanoTime()-startTime)/1000000+"ms");

					runningFlag = false;
					
				} catch (Exception e) {
					interruptFlag = true;
					runningFlag = false;
					e.printStackTrace();
				}
				
				return null;
			}
			
			@Override
			public void done() {
				closeUpdateBox();
				if (!interruptFlag && !runningFlag) {
					// have a save-to-file prompt here
					int save = JOptionPane.showConfirmDialog(null, "Export results to text?", "Export Results", JOptionPane.YES_NO_OPTION);
					if (save == JOptionPane.YES_OPTION) {
						// show save file dialog
						String filename = (new FileDialogPicker("Choose output file destination", "txt", new JFrame(), false)).getFileName();
						if(filename != null && !filename.equals("") && !filename.equals("*.txt")) {
							filename = filename.replaceAll("'", "");
							try {
								PrintWriter out = null;
								DecimalFormat formatter = new DecimalFormat("0.00");
								File file = new File(filename);
								out = new PrintWriter(new FileOutputStream(file, false));
								StringBuffer sb = new StringBuffer();
								sb.append(output);
								out.println(sb.toString());
								sb.setLength(0);
								out.close();
							} catch (IOException e) {
								ErrorLogger.writeExceptionToLogAndPrompt("File Exporter","Error writing file. Please ensure the application can write to the specified file.");
								System.err.println("Problem writing file: ");
								e.printStackTrace();
							}
						}
					}
				} else {
					runningFlag = false;
					interruptFlag = false;
					System.out.println("An error occurred.");
					JOptionPane.showMessageDialog(null, "An error occurred.");
				}
			}
		};
		worker.execute();
		showUpdateBox();
	}
	
	// for the halo subsampling, saves some RAM
	private static class IntFloatPair {
		private int i;
		private float f;
		
		IntFloatPair(int i, float f) {
			this.i = i;
			this.f = f;
		}
		
		public int getInt() {
			return this.i;
		}
		
		public float getFloat() {
			return this.f;
		}
	}
	
	private static class IntFloatComparator implements Comparator<IntFloatPair> {
		public int compare(IntFloatPair o1, IntFloatPair o2) {
			if (o1.f > o2.f) return 1;
			else if (o1.f < o2.f) return -1;
			return 0;
		}
	}
	
	public static enum ValidityTest {
		DUNN, SIL, BOTH, NONE;
	}
	
	public static enum SampleType {
		RAND, HALO, FULL, MEAN;
	}
}
