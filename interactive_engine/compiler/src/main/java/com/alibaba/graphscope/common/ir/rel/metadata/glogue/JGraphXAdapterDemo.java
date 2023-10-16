package com.alibaba.graphscope.common.ir.rel.metadata.glogue;

import com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern.Pattern;
import com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern.PatternEdge;
import com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern.PatternVertex;
import com.alibaba.graphscope.common.ir.rel.metadata.schema.EdgeTypeId;
import com.alibaba.graphscope.common.ir.rel.metadata.schema.GlogueSchema;
import com.mxgraph.layout.*;
import com.mxgraph.swing.*;
import org.jgrapht.*;
import org.jgrapht.ext.*;
import org.jgrapht.graph.*;

import javax.swing.*;
import java.awt.*;

/**
 * A demo applet that shows how to use JGraphX to visualize JGraphT graphs.
 * Applet based on
 * JGraphAdapterDemo.
 *
 */
public class JGraphXAdapterDemo
        extends JApplet {
    private static final long serialVersionUID = 2202072534703043194L;

    private static final Dimension DEFAULT_SIZE = new Dimension(530, 320);

    private JGraphXAdapter<PatternVertex, PatternEdge> jgxAdapter;

    /**
     * An alternative starting point for this demo, to also allow running this
     * applet as an
     * application.
     *
     * @param args command line arguments
     */
    public static void main(String[] args) {
        JGraphXAdapterDemo applet = new JGraphXAdapterDemo();
        applet.init();

        JFrame frame = new JFrame();
        frame.getContentPane().add(applet);
        frame.setTitle("JGraphT Adapter to JGraphX Demo");
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        frame.pack();
        frame.setVisible(true);
    }

    @Override
    public void init() {

        // GlogueSchema schema = new GlogueSchema().DefaultGraphSchema();
        // Glogue gl = new Glogue(); // .create(g, 3);

        // jgxAdapter = new JGraphXAdapter<>(gl.glogueGraph);

        Pattern p = new Pattern();
        jgxAdapter = new JGraphXAdapter<>(p.patternGraph);

        // // create a visualization using JGraph, via an adapter
        // jgxAdapter = new JGraphXAdapter<>(g);

        setPreferredSize(DEFAULT_SIZE);
        mxGraphComponent component = new mxGraphComponent(jgxAdapter);
        component.setConnectable(false);
        component.getGraph().setAllowDanglingEdges(false);
        getContentPane().add(component);
        resize(DEFAULT_SIZE);

        // gl.create(schema, 2);

        PatternVertex v0 = new PatternVertex(11, 0);
        PatternVertex v1 = new PatternVertex(22, 1);
        PatternVertex v2 = new PatternVertex(11, 0);
        EdgeTypeId e = new EdgeTypeId(11, 22, 1122);
        p.addVertex(v0);
        p.addVertex(v1);
        p.addVertex(v2);
        p.addEdge(v0, v1, e);
        p.addEdge(v2, v1, e);

        // String v1 = "v1";
        // String v2 = "v2";
        // String v3 = "v3";
        // String v4 = "v4";

        // // add some sample data (graph manipulated via JGraphX)
        // g.addVertex(v1);
        // g.addVertex(v2);
        // g.addVertex(v3);
        // g.addVertex(v4);

        // g.addEdge(v1, v2);
        // g.addEdge(v2, v3);
        // g.addEdge(v3, v1);
        // g.addEdge(v4, v3);

        // positioning via jgraphx layouts
        mxCircleLayout layout = new mxCircleLayout(jgxAdapter);

        // center the circle
        int radius = 100;
        layout.setX0((DEFAULT_SIZE.width / 2.0) - radius);
        layout.setY0((DEFAULT_SIZE.height / 2.0) - radius);
        layout.setRadius(radius);
        layout.setMoveCircle(true);

        layout.execute(jgxAdapter.getDefaultParent());
        // that's all there is to it!...
    }
}