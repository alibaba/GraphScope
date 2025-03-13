library(data.table)
library(igraph)
suppressMessages(require(bit64,quietly=TRUE,warn.conflicts=FALSE))
 
#library(RSvgDevice)
library(plotrix)
require(bit64)
#require(int64)	

message("Loading files")
dflist <- lapply(commandArgs(trailingOnly = TRUE), fread, sep="|", header=T, select=1:2, colClasses="integer64")
df <- rbindlist(dflist)

message("Set column names")
setNames(df, c("Person1", "Person2"))


#doby
#library(doBy)	
#d2<-summaryBy(Person2~Person1, data=df, FUN=c(length))
#message("Mean: ",d3)	

#library(plyr)
#d2 <- ddply(df, "Person1", summarize, P2Count = length(Person2))
#d3 <- as.numeric(d2$Person2.length)
#message("\\hline  \\#friends/user  &", min(d3) ,  " &  ", max(d3),  " & ", round(mean(d3)) , " & ", round(median(d3)), " \\\\")

d2 <- df[,length(Person2),by=Person1]
#print(d2)
message("\\hline  \\#friends/user  &", min(d2$V1) ,  " &  ", max(d2$V1),  " & ", round(mean(d2$V1)) , " & ", round(median(d2$V1)), " \\\\")


#devSVG(file = "numFriendsDensity.svg")
#pdf("numFriendsDensity.pdf")
#plot(density(d2$V1), main="Density #friends per user") 
#dev.off()
#devSVG(file = "numFriendsCumm.svg")	

message("Plot cummulative distribution of #friends/users")
pdf("numFriendsCummSCALEFACTOR.pdf")
plot(ecdf(d2$V1),main="Cummulative distribution #friends per user", xlab="Number of friends", ylab="Percentage number of users", log="x", xlim=c(0.8, max(d2$V1) + 20))	
dev.off()
