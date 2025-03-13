library(data.table)
library(igraph)
require(bit64)	

suppressMessages(require(bit64,quietly=TRUE,warn.conflicts=FALSE))
 
message("Loading files")
dflist <- lapply(commandArgs(trailingOnly = TRUE), fread, sep="|", header=T, select=1:2, colClasses="integer64")
df <- rbindlist(dflist)
setnames(df, 1:2, c("src", "dst"))

message("Creating graph object")
# this is a hack to create the graph faster than graph.data.frame
# see http://smallstats.blogspot.nl/2012/12/loading-huge-graphs-with-igraph-and-r.html
vertex.attrs <- list(name = unique(c(df$src, df$dst)))
edges <- rbind(match(df$src, vertex.attrs$name), match(df$dst,vertex.attrs$name))
G <- graph.empty(n = 0, directed = T)
G <- add.vertices(G, length(vertex.attrs$name), attr = vertex.attrs)
G <- add.edges(G, edges)

message("Calculating transitivity")
#message("STATISTICS: Graph transitivity (clustering coefficient): ", round(transitivity(G), 4))

message("\\hline   Clustering Coef. &   \\multicolumn{4}{|c|}{",round(transitivity(G), 4),"} \\\\")
