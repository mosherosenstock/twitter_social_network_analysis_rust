// Moshe Rosenstock
// Twitter Social Network Analysis
// 

// import crates
use rand::Rng;
use std::fs::File;
use std::io::prelude::*;
use std::io::BufRead;
use std::collections::VecDeque;
use std::sync::Arc;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::cmp::Ordering;
use std::collections::{ HashMap, HashSet};
use std::iter::successors;


fn main() {
    // Read our text file containing the Data
    // -------------------------------------------------------------------------
    let mut n=1000;
    let mut list_edges = read_file("edges_huawei.txt");
    list_edges.sort();

    // Calculate the Page Rank of our list_edges
    let mut page_rank = page_rank(&mut list_edges, n);

    // With this code we are filtering our list_edges to only contain the connections between the top 50 edges
    list_edges.retain(|&(a, _)| page_rank.iter().any(|&(b, _)| a == b));
    list_edges.retain(|&(_, a)| page_rank.iter().any(|&(b, _)| a == b));

    // Sort the list_edges in ascending order
    list_edges.sort_by(|a, b| a.0.cmp(&b.0));

    // Sort page_rank in ascending order
    page_rank.sort_by(|a, b| a.0.cmp(&b.0));
    let top50: Vec<usize> = page_rank.iter().map(|(num, _)| *num).collect();

    // Print Some Info about our List of Edges
    println!("\nLength of our entire list of edges (conections between nodes):{:?}",list_edges.len());
    
    //  HERE WE CREATE OUR GRAPH FOR THE CONNECTIONS BETWEEN THE TOP 50 NODES
    let mut adj_list = grouped_vertex_tuples(&list_edges);
    println!("Length of our Graph: {:?}\n",adj_list.len());

    //  Make "n" be the largest Vertex inside list_edges + 1
    let n = list_edges.iter()
                  .flat_map(|&(u, v)| vec![u, v])
                  .max()
                  .map(|x| x + 1)
                  .unwrap_or(0);
    println!("Biggest Vertex plus 1 (our N value): {}\n", n);

    // Create our graph that ontains only the edges between the top50 nodes (calculated by Page Rank)
    let mut graph = Graph::create_directed(n,&list_edges);
    graph.outedges.retain(|edges| !edges.is_empty());

    // Sort our graph in ascending order
    graph = graph.sort_ascending_order();

    // Create a HashMap that contains the top 50 nodes and their connections
    let mut graph_hashmap = HashMap::new();
    for (i, num) in top50.iter().enumerate() {
        graph_hashmap.insert(*num, &adj_list[i]);
    }

    // PREVIEW OUR GRAPH
    // -------------------------------------------------------------------------
    println!("These are the top 50 nodes and their connections between them: ");
    for (i, edges) in graph.outedges.iter().enumerate() {
        // Skip vertices that don't have any outgoing edges
        if edges.is_empty() {
            continue;
        }
        // Print the vertex number and its outgoing edges
        println!("({}) Node: {} - Edges: {:?}",i, page_rank[i].0, edges); //list_edges[i].0,edges);//
    }

    // IMPLEMENTING BFS ALGORITHM 
    // -------------------------------------------------------------------------
    let source_node = page_rank[0].0;
    let visited = bfs(&graph,page_rank[0].0);
    println!("Visited vertices BFS:\n {:?}", visited);  
    println!("");

    // CALCULATE THE DISTANCE BETWEEN EACH NODE INSIDE THE TOP 50 NODES
    // -------------------------------------------------------------------------
    println!(" - I dont want to print the distance between each of the top 50 nodes between them because it will print a huge string of numbers. So I will just print the distance between the first node and the rest of the nodes inside the top 50 nodes.");
    println!("\nThe distance between the first node = ({}) and the rest of nodes inside the top 50 nodes is: ", top50[0]);
    // let mut distance_nodes= 0; //bfs_distance_nodes(&graph_hashmap, top50[0], top50[0]);
    for i in 0..top50.len() {
        let mut distance_nodes= bfs_distance_nodes(&graph_hashmap, top50[0], top50[i]);
        println!("->The distance between node {} and node {} is {}.   ", top50[0], top50[i],distance_nodes);
    }
}


// -------------------------------------------------------------------------
// CALCULATE THE DISTANCES BETWEEN VECTORS
// The distance between two nodes can be obtained in terms of lowest common ancestor 
// -------------------------------------------------------------------------
fn bfs_distance_nodes(graph: &HashMap<usize, &Vec<usize>>, source: usize, target: usize) -> usize {
    let mut queue = VecDeque::new();
    let mut distances = HashMap::new();
    let mut visited = HashMap::new();

    queue.push_back(source);
    distances.insert(source, 0);
    visited.insert(source, true);

    while !queue.is_empty() {
        let current = queue.pop_front().unwrap();
        if current == target {
            return distances[&target];
        }

        for &neighbor in graph[&current] {
            if !visited.contains_key(&neighbor) {
                queue.push_back(neighbor);
                visited.insert(neighbor, true);

                let distance = distances[&current] + 1;
                distances.insert(neighbor, distance);
            }
        }
    }
    std::usize::MAX
}



fn minDistance(dist: Vec<usize>, sptSet: Vec<bool>) -> usize {
    let mut min = std::usize::MAX;
    let mut minIndex = 0;

    for v in 0..dist.len() {
        if !sptSet[v] && dist[v] <= min {
            min = dist[v];
            minIndex = v;
        }
    }

    minIndex
}


// // IMPLEMENTING BFS ALGORITHM 
// // -------------------------------------------------------------------------
fn bfs(g: &Graph,source_node:usize) -> Vec<Vertex> {
    // Create a queue to hold the vertices that are waiting to be processed
    let mut queue = VecDeque::new();

    // Create a vector to hold the vertices in the order they were visited
    let mut visited = Vec::new();

    // Start the BFS algorithm at the specified source node
    queue.push_back(source_node);

    // Continue processing vertices until the queue is empty
    while let Some(v) = queue.pop_front() {
        // Skip this vertex if it has already been visited
        if visited.contains(&v) {
            continue;
        }
        // Add this vertex to the visited list
        visited.push(v);

        // Add all the unvisited neighbors of this vertex to the queue
        if v < g.outedges.len() {
            for &w in &g.outedges[v] {
                if !visited.contains(&w) {
                    queue.push_back(w);
                }
            }
        }
    }
    println!("Number of Edges Visited in BFS: {:?}",visited.len());
    // Return the list of vertices in the order they were visited
    visited
}


// MARK COMPONENT BFS
fn mark_component_bfs(vertex:Vertex, graph:&Graph, component:&mut Vec<Option<Component>>, component_no:Component) {
    // Check that the index of the vertex is within the bounds of the component vector
    if vertex < component.len() {
        component[vertex] = Some(component_no);
    }
    let mut queue = std::collections::VecDeque::new();
    queue.push_back(vertex);

    while let Some(v) = queue.pop_front() {
        for w in graph.outedges[v].iter() {
            if let None = component[*w] {
                // Check that the index of the vertex is within the bounds of the component vector
                if *w < component.len() {
                    component[*w] = Some(component_no);
                }
                queue.push_back(*w);
            }
        }
    }
}


// FUNCTION TO CREATE AN ADJACENCY LIST GIVEN A LIST OF EDGES
fn grouped_vertex_tuples(vertex_tuples: &[(Vertex, Vertex)]) -> AdjacencyLists {
    let mut adjacency_lists: AdjacencyLists = vec![Vec::new(); vertex_tuples.len()];

    for &(u, v) in vertex_tuples {
        adjacency_lists[u].push(v);
    }
    // Remove empty adjacency lists
    adjacency_lists.retain(|l| !l.is_empty());
    adjacency_lists
}

// FUNCTION TO READ TEXT FILE AND RETURN A LIST OF EDGES
fn read_file(_path: &str) -> Vec<(Vertex, Vertex)>{
    let mut list_edges: Vec<(Vertex, Vertex)> = Vec::new();
    let file = File::open("edges_huawei.txt").expect("Could not open file");

    let mut buf_reader = std::io::BufReader::new(file).lines();
    buf_reader.next();
    for line in buf_reader {
        let line_str = line.expect("Error reading");
        let line_str = line_str.replace("[","");
        let line_str = line_str.replace("]","");
        let line_str = line_str.replace(" ","");
        let v: Vec<&str> = line_str.trim().split(',').collect();
        let x = v[0].parse::<i128>().unwrap();
        let y = v[1].parse::<i128>().unwrap();
        list_edges.push((x as Vertex, y as Vertex));
    }
    return list_edges;
}


// CREATE A GRAPH TYPE
type Vertex = usize;
type ListOfEdges = Vec<(Vertex,Vertex)>;
type AdjacencyLists = Vec<Vec<Vertex>>;
type Component = usize;

#[derive(Debug)]
struct Graph {
    n: usize, // vertex labels in {0,...,n-1}
    outedges: AdjacencyLists,
}

// reverse direction of edges on a list
fn reverse_edges(list:&ListOfEdges)
        -> ListOfEdges {
    let mut new_list = vec![];
    for (u,v) in list {
        new_list.push((*v,*u));
    }
    new_list
}

// IMPLEMENTATION OF GRAPH
impl Graph {
    fn add_directed_edges(&mut self,
                          edges:&ListOfEdges) {
        for (u,v) in edges {
            self.outedges[*u].push(*v);
        }
    }
    fn sort_graph_lists(&mut self) {
        for l in self.outedges.iter_mut() {
            l.sort();
        }
    }
    fn create_directed(n:usize,edges:&ListOfEdges)
                                            -> Graph {
        let mut g = Graph{n,outedges:vec![vec![];n]};
        g.add_directed_edges(edges);
        g.sort_graph_lists();
        g                                        
    }
    
    fn create_undirected(n:usize,edges:&ListOfEdges)
                                            -> Graph {
        let mut g = Self::create_directed(n,edges);
        g.add_directed_edges(&reverse_edges(edges));
        g.sort_graph_lists();
        g                                        
    }

    fn sort_ascending_order(&self) -> Graph {
        // Create a vector of tuples representing the edges in the graph
        let mut edges: ListOfEdges = Vec::new();

        // Iterate over the vertices in the graph
        for (u, neighbors) in self.outedges.iter().enumerate() {
            // Iterate over the neighbors of the current vertex
            for &v in neighbors {
                // Add the edge (u, v) to the list of edges
                edges.push((u, v));
            }
        }
        // Sort the edges in ascending order
        edges.sort();

        // Create a vector of vectors of vertices to hold the sorted graph
        let mut sorted_outedges = Vec::new();

        // Iterate over the sorted edges
        for (u, v) in edges {
            // If the current vertex is not already in the sorted graph, add it
            if sorted_outedges.len() <= u {
                sorted_outedges.push(Vec::new());
            }

            // Add the neighbor of the current vertex to the sorted graph
            sorted_outedges[u].push(v);
        }

        // Create and return a new Graph struct using the sorted graph
        Graph {
            n: self.n,
            outedges: sorted_outedges,
        }
    }

    fn sort_descending_order(&self) -> Graph {
        // Create a vector of tuples representing the edges in the graph
        let mut edges: ListOfEdges = Vec::new();
    
        // Iterate over the vertices in the graph
        for (u, neighbors) in self.outedges.iter().enumerate() {
            // Iterate over the neighbors of the current vertex
            for &v in neighbors {
                // Add the edge (u, v) to the list of edges
                edges.push((u, v));
            }
        }
    
        // Sort the edges in descending order
        edges.sort_by(|a, b| b.cmp(a));
    
        // Create a new Graph struct with the sorted edges
        Graph::create_directed(self.n, &edges)
    }
}


// FUNCTION THAT COMPUTES THE PAGE RANK
// COMPUTE THE PAGE RANK FOR TOP 50 VERTEXES
fn page_rank(data: &mut Vec<(Vertex, Vertex)>, n_vertices:usize) -> Vec<(Vertex, f64)> { 
    // This function creates a page rank calculator for each node
    // select a random node 
    let mut rng = rand::thread_rng();
    let r_node = rng.gen_range(0..=(data.len()));
    // convert r_node to usize
    let r_node = r_node as usize;
    // make a variable node that contains the initial node that is being calculated
    let mut node = data[r_node].0;
    // create a variable page_rank to store the PageRank value for each node
    // the first value should be a random iteration from 0 to 1000, 
    // the second value should be the PageRank value (starting at 0.0)
    let mut page_rank: Vec<(Vertex, Vertex)> = Vec::new();

    for i in 0..n_vertices {
        page_rank.push((i, 0));
    }
    // create a empty vector "out" to store the outgoing edges; and variable "out_edges" to count how many of them
    let mut out = Vec::new();
    let mut out_edges = 0;
    // starting from that random node make 100 random steps
    for _i in 0..100 {
        // check if the node has any outgoing edges that are not himself
        for j in 0..data.len() {
            if data[j].0 == node {
                out_edges += 1;
                out.push(data[j].1);
            }
        }
        // if the node has no outgoing edges, select a new randon node
        while out_edges == 0 {
            let mut rng = rand::thread_rng();
            node = rng.gen_range(0..=(data.len()));
            node = data[node as usize].0;
        }
        // check if the node has any outgoing edges
        for j in 0..data.len() {
            if data[j].0 == node {
                out_edges += 1;
                // add the outgoing edges to the vector "out"
                out.push(data[j].1);
            }
        }
        // if the node has outgoing edges, select a random outgoing edge with 9/10 probability and a random node with 1/10 probability
        let random = rng.gen_range(0..10);
        // probability of 1/10 to select a random node (from entire graph)
        if random == 0 {
            node = rng.gen_range(0..=(data.len()));
            node = data[node as usize].0;
        }
        // probability of 9/10 to select a random node (from conecting edges)
        else {
            // select a random value from vector out
            // select a random node from the conecting edges
            let mut rng = rand::thread_rng();
            node = rng.gen_range(0..out_edges);
            node = out[node as usize];
        }
        // now we should add one point to the PageRank value of the node that we are on
        for i in 0..page_rank.len() {
            if page_rank[i].0 == node {
                page_rank[i].1 += 1;
            }
        }
        // reset the vector "out" and the variable "out_edges"
        out = Vec::new();
        out_edges = 0;
    }
    // END OF THE 100 ITERATIONS
    // sort the page_rank vector by the second element of the tuple
    page_rank.sort_by(|a, b| b.1.cmp(&a.1));
    // create a new vector that contains the page rank of the top 50 nodes
    let mut top_50: Vec<(Vertex, f64)> = Vec::new();
    for i in 0..50 {
        // divide the second value of each tuple by 100 to get the page rank
        top_50.push((page_rank[i].0 as Vertex, page_rank[i].1 as f64 / 100.0));
    }
    return top_50;
}