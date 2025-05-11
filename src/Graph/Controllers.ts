/**
 * Run the following command to compute the page rank of the graph:
 * deno run --allow-read src/Graph/Controllers.ts compute
 */

import * as pl from "npm:nodejs-polars@0.18.0";

import { computePageRank } from "./Model.ts"

const USER_FOLLOW_LIMIT = 100;

async function generatePageRankFile(lazyData: pl.LazyFrame): Promise<pl.DataFrame> {
  const degrees = lazyData.groupBy("target_fid")
    .agg(pl.count("target_fid").alias("in_deg"))
    .filter(pl.col("in_deg").gtEq(USER_FOLLOW_LIMIT));

  // Adding filtering to remove users without at least 100 follows
  const filtered = lazyData.join(degrees, { leftOn: "fid", rightOn: "target_fid", how: "inner" });

  const current_network = filtered
    .groupBy(["fid", "target_fid"])
    .agg(
      pl.col("type").sum().alias("type_sum"),
    )

  // Added just in case users follow then unfollow in the data.
  const cleaned_network = current_network
    .withColumns([
      pl
        .when(pl.col("type_sum").gtEq(1))
        .then(pl.lit(1))
        .otherwise(pl.lit(0))
        .alias("type_sum"),
    ])
    .filter(pl.col("type_sum").eq(1));

  const live = await cleaned_network.collect();

  return await computePageRank(live);
}

if (import.meta.main) {
  const src_data = "data/follows/farcasterdata-*.csv";
  const lazyData = pl.scanCSV(src_data, { hasHeader: true });
  const pr_data = await generatePageRankFile(lazyData);

  // Write the DataFrame to CSV using a writable file handle
  const file = await Deno.open("data/pr_data.csv", { write: true, create: true, truncate: true });
  await pr_data.writeCSV(file);
  file.close();
}
