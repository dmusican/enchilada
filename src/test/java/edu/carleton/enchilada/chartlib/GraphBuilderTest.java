package edu.carleton.enchilada.chartlib;

import edu.carleton.enchilada.collection.Collection;
import edu.carleton.enchilada.database.CreateTestDatabase;
import edu.carleton.enchilada.database.Database;
import edu.uci.ics.jung.graph.Graph;

import junit.framework.TestCase;

/**
 * It's Pancho Villa's birthday!
 * Created June 5, 2010
 * @author jtbigwoo
 *
 */
public class GraphBuilderTest extends TestCase {
	
	GraphBuilder classToTest;
	Database db;
	
	protected void setUp() throws Exception {
        super.setUp();
        
		new CreateTestDatabase();
		db = Database.getDatabase("TestDB");
		db.openConnection();
	}

	public void testBuildSimpleGraph() {
		Collection coll = db.getCollection(2);
		Collection coll3 = null;
		db.moveCollection(db.getCollection(3), coll);
		db.moveCollection(db.getCollection(4), coll);
		db.moveCollection(db.getCollection(5), db.getCollection(3));
		
		classToTest = new GraphBuilder(db, coll);
		Graph<Collection, String> graph = classToTest.getFullGraph();
		System.out.println(graph);
		assertEquals(4, graph.getEdgeCount());
		assertEquals(5, graph.getVertexCount());
		java.util.Collection<Collection> neighbors = graph.getNeighbors(coll);
		assertEquals(2, neighbors.size());
		for (Collection child : neighbors) {
			if (child.equals(db.getCollection(3))) {
				// hang on to this one for the next test
				coll3 = child;
			}
			else if (child.equals(db.getCollection(4))) {
				//we're ok here
			}
			else {
				fail();
			}
		}
		java.util.Collection<Collection> successors = graph.getSuccessors(coll3);
		assertEquals(1, successors.size());
	}
	
	public void tearDown()
	{
		db.closeConnection();
	}

}
