
args <- commandArgs(trailingOnly=TRUE);
print(args[1]);
table = read.table(paste(args[1],sep=''),header=TRUE)
names(table)
m <- lm(formula=log(Persons) ~ log(Bytes),data=table)
#here is the list of scale factors to retrieve
scaleFactors <- data.frame(Bytes=c(1073741824, 3221225472, 10737418240, 32212254720, 107374182400, 322122547200, 1099511628000, 3298534883328, 10995116277760, 32985348833280, 329853488332800, 1125899906842624))
predicted <- exp(predict(m,scaleFactors))
for( i in 1:length(predicted)) {
    print(paste(round(predicted[[i]],digits=-3),scaleFactors$Bytes[[i]],sep=' '))
}
