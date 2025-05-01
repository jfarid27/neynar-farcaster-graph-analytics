import pl from "npm:nodejs-polars";
import Graph from "npm:graphology";
import pagerank from "npm:graphology-pagerank";

type PROptions = {
  damping?: number;
  maxIterations?: number;
  tolerance?: number;
};

const defaultPRParams = {
  damping: 0.85,
  maxIterations: 100,
  tolerance: 1e-6,
};


export async function computePageRank(dfClean, opts: PROptions = {}) {

  const { damping, maxIterations, tolerance } = {
    ...defaultPRParams,
    ...opts,
  };

  // 3. Extract JS arrays of edges
  const fids    = dfClean.getColumn("fid").toArray() as number[];
  const targets = dfClean.getColumn("target_fid").toArray() as number[];

  // 4. Build a directed graph
  const graph = new Graph({ type: "directed" });
  for (let i = 0; i < fids.length; i++) {
    const u = fids[i].toString();
    const v = targets[i].toString();
    if (!graph.hasNode(u)) graph.addNode(u);
    if (!graph.hasNode(v)) graph.addNode(v);
    graph.addEdge(u, v);
  }

  // 5. Run PageRank
  const scores = pagerank(graph, {
    damping,      // teleport probability
    maxIterations, // stop after 100 iterations (tune as needed)
    tolerance     // convergence threshold
  });

  // 6. Turn scores back into a Polars DataFrame
  const nodes = Object.keys(scores);
  const ranks = Object.values(scores);
  const prDf = pl.DataFrame({ node: nodes, pageRank: ranks });

  return prDf;
}