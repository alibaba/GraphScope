library(data.table)
library(igraph)
require(bit64)	

suppressMessages(require(bit64,quietly=TRUE,warn.conflicts=FALSE))
 
message("Loading files")
dflist <- lapply(commandArgs(trailingOnly = TRUE), fread, sep="|", header=T, select=1:2, colClasses="integer64")
df <- rbindlist(dflist)

	
message("Set column names")
#names(df)=c("Post", "Person")
setNames(df, c("Person", "PostOrCommont"))

#library(plyr)
#d2 <- ddply(df, "Person", summarize, PCount = length(PostOrCommont))
#d3 <- as.numeric(d2$PCount)
#message("\\hline  \\#likes/user  &", min(d3) ,  " &  ", max(d3),  " & ", round(mean(d3)) , " & ", round(median(d3)), " \\\\")

d2 <- df[,length(PostOrCommont),by=Person]
message("\\hline  \\#likes/user  &", min(d2$V1),  " &  ", max(d2$V1),  " & ", round(mean(d2$V1)) , " & ", round(median(d2$V1)), " \\\\")
