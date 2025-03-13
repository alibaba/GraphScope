
args <- commandArgs(trailingOnly=TRUE);
print(args[1]);
data <- as.matrix(read.table(paste(args[1],sep=''),header=T))
n = sum(!is.na(data))
data.ecdf = ecdf(data)
data.mean = mean(data)
data.median = median(data)
data.max = max(data)
data.min = min(data)

postscript(args[2])
plot(data.ecdf, xlab = args[3], ylab = "cumulative probability", main = '', log="x", xlim=c(1,data.max))
text((data.max - data.min)/10,0.9,paste("Mean: ",data.mean,sep=" "))
text((data.max - data.min)/10,0.85,paste("Median: ",data.median,sep=" "))
text((data.max - data.min)/10,0.8,paste("Min: ",data.min,sep=" "))
text((data.max - data.min)/10,0.75,paste("Max: ",data.max,sep=" "))
dev.off()
