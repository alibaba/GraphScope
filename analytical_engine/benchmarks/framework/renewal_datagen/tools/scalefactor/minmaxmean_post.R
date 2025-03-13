library(data.table)
library(igraph)
require(bit64)	

suppressMessages(require(bit64,quietly=TRUE,warn.conflicts=FALSE))
 
message("Loading files")
dflist <- lapply(commandArgs(trailingOnly = TRUE), fread, sep="|", header=T, select=1:2, colClasses="integer64")
df <- rbindlist(dflist)

#message("Set column names")
#names(df)=c("Post", "Person")
#setNames(df, c("Post", "Person"))

#library(plyr)
#d2 <- ddply(df, "Person.id", summarize, PCount = length(Post.id))
#d3 <- as.numeric(d2$PCount)
#message("\\hline  \\#posts/user  &", min(d3) ,  " &  ", max(d3),  " & ", round(mean(d3)) , " & ", round(median(d3)), " \\\\")

d2 <- df[,length(Post.id),by=Person.id]
message("\\hline  \\#posts/user  &", min(d2$V1),  " &  ", max(d2$V1),  " & ", round(mean(d2$V1)) , " & ", round(median(d2$V1)), " \\\\")

message("Plot histogram #post/users")
pdf("numPostsUserHistSCALEFACTOR.pdf")
hist(d2$V1,main="Histogram #posts per user", xlab="Number of posts", ylab="Number of users")	
dev.off()
