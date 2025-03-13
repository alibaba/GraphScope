

library("VGAM")
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
#plot(data.ecdf, xlab = args[3], ylab = "cumulative probability", main = '', log="x", xlim=c(1,data.max))
print(max(data))
h <- hist(data, plot=F, breaks=c(seq(0,max(data)+1, 1)))
par(mar=c(6.0,5.1,1.1,1.1))
plot(h$counts, log="xy", pch=20, col="black",
      main="", 
        xlab="Degree", ylab="Frequency", cex.lab=3, cex.axis=2.5 )
degValues <- seq(1:10000)
#geosequence <- 10000*dgeom(degValues, 0.12, log=FALSE)
#lines(geosequence, col="limegreen", lwd=3)
#legend(1, 50, c("Datagen", "Geometric"), 
#       cex=3, col=c("black","limegreen"), 
#       lty=c(0,1), pch=c(20,NA), lwd=3)
geosequence <- 1000000*dzeta(degValues, 0.7, log=FALSE)
lines(geosequence, col="brown4", lwd=3)
#legend(1, 15, c("Datagen", "Zeta"), 
#       cex=3, col=c("black","brown4"), 
#       lty=c(0,1), pch=c(20,NA), lwd=3)
#
dev.off()
