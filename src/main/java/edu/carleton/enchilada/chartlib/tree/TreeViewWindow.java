package edu.carleton.enchilada.chartlib.tree;

import java.awt.*;

import javax.swing.*;

import edu.carleton.enchilada.database.Database;
import org.apache.commons.collections15.Transformer;

import edu.carleton.enchilada.collection.Collection;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.MouseEvent;
import java.awt.geom.Ellipse2D;
import java.awt.geom.Point2D;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import edu.carleton.enchilada.chartlib.*;
import edu.carleton.enchilada.chartlib.hist.HistogramMouseDisplay;
import edu.carleton.enchilada.chartlib.hist.HistogramsPlot;
import edu.uci.ics.jung.algorithms.layout.PolarPoint;
import edu.uci.ics.jung.algorithms.layout.RadialTreeLayout;
import edu.uci.ics.jung.graph.Graph;
import edu.uci.ics.jung.graph.Tree;
import edu.uci.ics.jung.visualization.Layer;
import edu.uci.ics.jung.visualization.VisualizationServer;
import edu.uci.ics.jung.visualization.VisualizationViewer;
import edu.uci.ics.jung.visualization.control.CrossoverScalingControl;
import edu.uci.ics.jung.visualization.control.DefaultModalGraphMouse;
import edu.uci.ics.jung.visualization.control.GraphMouseListener;
import edu.uci.ics.jung.visualization.control.ModalGraphMouse;
import edu.uci.ics.jung.visualization.control.ScalingControl;
import edu.uci.ics.jung.visualization.decorators.EdgeShape;
import edu.uci.ics.jung.visualization.decorators.EllipseVertexShapeTransformer;

/**
 * A window which contains a tree view of a hierarchy.
 * 
 * 
 * @author jtbigwoo
 *
 */

public class TreeViewWindow extends JFrame {

	private Database db;
	
	private JPanel buttonPanel;

	private Collection rootCollection;
	
	private GraphBuilder builder;
	
	private Tree<Collection, String> tree;
	
	/**
     * the visual component and renderer for the graph
     */
    VisualizationViewer<Collection, String> vv;

    VisualizationServer.Paintable rings;
    
    RadialTreeLayout<Collection, String> layout;

	private HistogramsPlot plot;
	ZoomableChart zPlot;
	
	// default minimum and maximum of the zoom window
	private int defMin = 0, defMax = 300;
	
	private JPanel plotPanel;

	private JPanel plotRightPanel;
	
	private HistogramMouseDisplay histMouseDisplay;

	private JTextField collectionNameField;
	
	public TreeViewWindow(final Database db, int collID) {
		super("Hierarchy Tree View");
		this.db = db;

		setLayout(new BorderLayout());
		JPanel leftPanel = new JPanel();
		leftPanel.setLayout(new BorderLayout());
		this.add(leftPanel, BorderLayout.WEST);
		buttonPanel = new JPanel();
		buttonPanel.setLayout(new BoxLayout(buttonPanel, BoxLayout.Y_AXIS));
		leftPanel.add(buttonPanel, BorderLayout.SOUTH);
		
		rootCollection = db.getCollection(collID);
		builder = new GraphBuilder(db, rootCollection);
//		Tree<Collection, String> tree = builder.getFullGraph();
		tree = builder.getSubGraph(8);
		
//		Layout<Collection, String> layout = new TreeLayout<Collection, String>(tree, 25, 25);
//		Layout<Collection, String> layout = new CircleLayout<Collection, String>(tree);
//		layout.setSize(new Dimension(500, 500));
//		VisualizationViewer<Collection, String> vv = new VisualizationViewer<Collection, String>(layout);
//		vv.setPreferredSize(new Dimension(700, 700));
//		DefaultModalGraphMouse mouse = new DefaultModalGraphMouse();
//		mouse.setMode(ModalGraphMouse.Mode.TRANSFORMING);
//		vv.setGraphMouse(mouse);

//		layout = new BalloonLayout<Collection, String>(tree);
//		layout.setSize(new Dimension(900,900));
		
		layout = new RadialTreeLayout<Collection, String>(tree);
		layout.setSize(new Dimension(600,600));

//      layout = new TreeLayout<Collection, String>(tree, 15, 15);

        vv =  new VisualizationViewer<Collection, String>(layout, new Dimension(600,600));
        vv.setBackground(Color.white);
        vv.addGraphMouseListener(new OurGraphMouseListener<Collection>());
        vv.getRenderContext().setEdgeShapeTransformer(new EdgeShape.Line());
//        vv.getRenderContext().setVertexLabelTransformer(new ToStringLabeller());
        vv.getRenderContext().setVertexShapeTransformer(new ClusterVertexShapeFunction());
        // add a listener for ToolTips
        vv.setVertexToolTipTransformer(new CollectionLabeller());
        vv.getRenderContext().setVertexLabelTransformer(new ClusterVertexLabelFunction());
        rings = new Rings();
		vv.addPreRenderPaintable(rings);

        leftPanel.add(vv, BorderLayout.NORTH);

        final DefaultModalGraphMouse<Collection, String> graphMouse = new DefaultModalGraphMouse<Collection, String>();

        vv.setGraphMouse(graphMouse);

        graphMouse.setMode(ModalGraphMouse.Mode.PICKING);

        final ScalingControl scaler = new CrossoverScalingControl();

        JButton plus = new JButton("+");
        plus.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent e) {
                scaler.scale(vv, 1.1f, vv.getCenter());
            }
        });
        JButton minus = new JButton("-");
        minus.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent e) {
                scaler.scale(vv, 1/1.1f, vv.getCenter());
            }
        });

        JButton moveCenter = new JButton("Recenter Graph on Particle");
        moveCenter.addActionListener(new ActionListener() {

            public void actionPerformed(ActionEvent e) {
                Set picked =new HashSet(vv.getPickedVertexState().getPicked());
                Collection newRoot;
                if(picked.size() == 1) {
                	Object newRootObj = picked.iterator().next();
                	if (newRootObj instanceof Collection) {
                    	newRoot = (Collection) newRootObj;
                		tree = builder.getSubGraph(newRoot, 8);
                		RadialTreeLayout<Collection, String> newLayout = new RadialTreeLayout<Collection, String>(tree);
                		newLayout.setSize(new Dimension(600,600));

                		vv.setGraphLayout(newLayout);
                		layout = newLayout;
//                        vv.getPickedVertexState().clear();
                        vv.repaint();
                	}
                	else if (newRootObj instanceof Tree)  {
                		Tree littleTree = (Tree) newRootObj;
                		newRoot = (Collection) littleTree.getRoot();
                		tree = builder.getSubGraph(newRoot, 8);
                		RadialTreeLayout<Collection, String> newLayout = new RadialTreeLayout<Collection, String>(tree);
                		newLayout.setSize(new Dimension(600,600));

                		vv.setGraphLayout(newLayout);
                		layout = newLayout;
                        vv.getPickedVertexState().clear();
                        vv.repaint();
                	}
                }
            }});

        JButton reCenter = new JButton("Recenter Graph on Home");
        reCenter.addActionListener(new ActionListener() {

            public void actionPerformed(ActionEvent e) {
            	tree = builder.getSubGraph(rootCollection, 8);
        		RadialTreeLayout<Collection, String> newLayout = new RadialTreeLayout<Collection, String>(tree);
        		newLayout.setSize(new Dimension(600,600));

        		vv.setGraphLayout(newLayout);
        		layout = newLayout;
                vv.getPickedVertexState().clear();
                vv.repaint();
            }});

        JPanel controls = new JPanel();
        controls.add(moveCenter);
        controls.add(reCenter);
        buttonPanel.add(controls);

		JLabel collectionNameLabel = new JLabel("Collection Name: ");
        collectionNameField = new JTextField(20);
        collectionNameField.setText(rootCollection.getName());
		
		JButton saveNameButton = new JButton("Save Collection Name");
		saveNameButton.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
                Set picked =new HashSet(vv.getPickedVertexState().getPicked());
                if(picked.size() == 1) {
                	Collection coll = (Collection) picked.iterator().next();
                	coll.setName(collectionNameField.getText());
                	db.renameCollection(coll, collectionNameField.getText());
                }
			}
		});
		
		JPanel collectionNamePanel = new JPanel();
		collectionNamePanel.add(collectionNameLabel);
		collectionNamePanel.add(collectionNameField);
		collectionNamePanel.add(saveNameButton);
		buttonPanel.add(collectionNamePanel);

        plotPanel = new JPanel();
		plotPanel.setLayout(new BorderLayout());
		plotPanel.setSize(600, 600);
		this.add(plotPanel, BorderLayout.EAST);
		
		try {
			plot = new HistogramsPlot(collID);
			plot.setTitle("Spectrum histogram for collection #" + collID);
			zPlot = new ZoomableChart(plot);

			zPlot.setCScrollMin(defMin);
			zPlot.setCScrollMax(defMax);
			
			plotPanel.add(zPlot, BorderLayout.CENTER);
		}
		catch (SQLException sqe) {
			plotPanel.add(new JTextArea(sqe.toString()));
		}

		plotRightPanel = new JPanel();
		plotRightPanel.setLayout(new BoxLayout(plotRightPanel, BoxLayout.Y_AXIS));
		plotPanel.add(plotRightPanel, BorderLayout.SOUTH);
		JPanel plotButtonPanel = new JPanel();
		plotRightPanel.add(plotButtonPanel);
		
		JButton zdef, zout;
		zdef = new JButton("Reset Spectrum Zoom");
		zdef.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				zPlot.zoomOut();
			}
		});
		plotButtonPanel.add(zdef);
		
		zout = new JButton("Zoom Spectrum Out");
		zout.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				zPlot.zoomOutHalf();
			}
		});
		plotButtonPanel.add(zout);

		histMouseDisplay = new HistogramMouseDisplay(plot, false);
		plotRightPanel.add(histMouseDisplay);

		validate();
		pack();
	}

    class ClusterVertexShapeFunction<V> extends EllipseVertexShapeTransformer<V> {
        ClusterVertexShapeFunction() {
            setSizeTransformer(new ClusterVertexSizeFunction<V>());
        }
        
        public Shape transform(V v) {
            if(v instanceof Graph<?,?>) {
            	return factory.getRegularPolygon(v, 6);
            }
            return super.transform(v);
        }
    }

    class ClusterVertexSizeFunction<V> implements Transformer<V,Integer> {
        public ClusterVertexSizeFunction() {
        }

        public Integer transform(V v) {
            return 6;
        }
    }

    class ClusterVertexLabelFunction<V> implements Transformer<V, String> {
    	ClusterVertexLabelFunction() {

    	}
        
        public String transform(V v) {
        	if (v instanceof Collection) {
        		Collection coll = (Collection) v;
        		try {
        			Integer.parseInt(coll.getName());
        			return "";
        		}
        		catch (NumberFormatException nfe) {
        			return coll.getName();
        		}
        	}
        	else if (v instanceof Tree)  {
        		Tree littleTree = (Tree) v;
        		Collection coll = (Collection) littleTree.getRoot();
        		try {
        			Integer.parseInt(coll.getName());
        			return "";
        		}
        		catch (NumberFormatException nfe) {
        			return coll.getName();
        		}
        	}
        	
        	return v.toString();
        }
    }

    class CollectionLabeller<V> implements Transformer<V,String> {
        public CollectionLabeller() {

        }

        public String transform(V v) {
        	if (v instanceof Collection) {
        		Collection coll = (Collection) v;
        		return coll.getName() + " : " + coll.getCollectionSize() + " particles";
        	}
        	else if (v instanceof Tree)  {
        		Tree littleTree = (Tree) v;
        		Collection coll = (Collection) littleTree.getRoot();
        		return coll.getName() + " : " + coll.getCollectionSize() + " particles";
        	}

            return v.toString();
        }
    }
    
    class Rings implements VisualizationServer.Paintable {
    	
    	Set<Double> depths;
    	
    	public Rings() {
    		depths = getDepths();
    	}
    	
    	private Set<Double> getDepths() {
    		Set<Double> depths;
			depths = new HashSet<Double>();
    		Map<Collection,PolarPoint> polarLocations = layout.getPolarLocations();
    		for(Object o : tree.getVertices()) {
    			PolarPoint pp = polarLocations.get(o);
        			depths.add(pp.getRadius());
    		}
    		return depths;
    	}

		public void paint(Graphics g) {
			depths = getDepths();
			g.setColor(Color.lightGray);
		
			Graphics2D g2d = (Graphics2D)g;
			Point2D center = layout.getCenter();

			Ellipse2D ellipse = new Ellipse2D.Double();
			for(double d : depths) {
				ellipse.setFrameFromDiagonal(center.getX()-d, center.getY()-d, 
						center.getX()+d, center.getY()+d);
				Shape shape = vv.getRenderContext().getMultiLayerTransformer().getTransformer(Layer.LAYOUT).transform(ellipse);
				g2d.draw(shape);
			}
		}

		public boolean useTransform() {
			return true;
		}
    }

    /**
     * A nested class to demo the GraphMouseListener finding the
     * right vertices after zoom/pan
     */
    class OurGraphMouseListener<V> implements GraphMouseListener<V> {
        
    		public void graphClicked(V v, MouseEvent me) {
    		    System.err.println("Vertex "+v+" was clicked at ("+me.getX()+","+me.getY()+")");
    		}
    		public void graphPressed(V v, MouseEvent me) {
                Collection selectedColl;
    			if (v instanceof Collection) {
    				selectedColl = (Collection) v;
    				collectionNameField.setText(selectedColl.getName());
            		plotPanel.remove(zPlot);
            		plotRightPanel.remove(histMouseDisplay);
            		try {
	        			plot = new HistogramsPlot(selectedColl.getCollectionID());
	        			plot.setTitle("Spectrum histogram for collection #" + selectedColl.getCollectionID());
	        			zPlot = new ZoomableChart(plot);

	        			zPlot.setCScrollMin(defMin);
	        			zPlot.setCScrollMax(defMax);
	        			
	        			plotPanel.add(zPlot, BorderLayout.CENTER);
	        			histMouseDisplay = new HistogramMouseDisplay(plot, false);
	        			plotRightPanel.add(histMouseDisplay);
	        			plotPanel.validate();
	        			plotPanel.repaint();
            		}
                	catch (SQLException sqe) {
            			plotPanel.add(new JTextArea(sqe.toString()));
                	}
    			}
    			else if (v instanceof Tree) {
            		Tree littleTree = (Tree) v;
					selectedColl = (Collection) littleTree.getRoot();
    				collectionNameField.setText(selectedColl.getName());
	        		plotPanel.remove(zPlot);
            		plotRightPanel.remove(histMouseDisplay);
	        		try {
	        			
	        			plot = new HistogramsPlot(selectedColl.getCollectionID());
	        			plot.setTitle("Spectrum histogram for collection #" + selectedColl.getCollectionID());
	        			zPlot = new ZoomableChart(plot);
	
	        			zPlot.setCScrollMin(defMin);
	        			zPlot.setCScrollMax(defMax);
	        			
	        			plotPanel.add(zPlot);
	        			histMouseDisplay = new HistogramMouseDisplay(plot, false);
	        			plotRightPanel.add(histMouseDisplay);
	        			plotPanel.validate();
	        			plotPanel.repaint();
	        		}
	            	catch (SQLException sqe) {
	        			plotPanel.add(new JTextArea(sqe.toString()));
	            	}
    			}
    		    System.err.println("Vertex "+v+" was pressed at ("+me.getX()+","+me.getY()+")");
    		}

    		public void graphReleased(V v, MouseEvent me) {
    		    System.err.println("Vertex "+v+" was released at ("+me.getX()+","+me.getY()+")");
    		}
    }

}