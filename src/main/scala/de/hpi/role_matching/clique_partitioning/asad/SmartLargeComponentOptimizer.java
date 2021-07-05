package de.hpi.role_matching.clique_partitioning.asad;

import de.hpi.role_matching.compatibility.graph.representation.SubGraph;

import java.util.HashSet;

public class SmartLargeComponentOptimizer {

    private final SubGraph graph;
    private int MAX_VERTEX_COUNT_RELATED_WORK = 500;

    public SmartLargeComponentOptimizer(SubGraph component){
        this.graph = component;
    }


    public void dummyOptimization() {
        //measure time for the entire execution:

        //selects a clique by greedy method:
        //RoleMerge clique = new GreedyComponentOptimizer(component,true).singleIteration() //for now just use a dummy procedure that selects two vertices

        //remove the vertices and all incident edges:

        //componentList = rediscover all connected components

        //for all connected components c in componentList where c.size < MAX_VERTEX_COUNT_RELATED_WORK
            //remove connected component (just ignore it)
        //for all connected components > MAX_VERTEX_COUNT_RELATED_WORK
            //call yourself recursively
    }

}
