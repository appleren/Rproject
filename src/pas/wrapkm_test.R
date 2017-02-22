setwd("/disk2/home/k2data/Rworkspace/Rprojects/src/pas")

x <- rbind(matrix(rnorm(100, sd = 0.3), ncol = 2),
           matrix(rnorm(100, mean = 1, sd = 0.3), ncol = 2))
colnames(x) <- c("x", "y")
param.x <- x
param.centers

mainFunction<-function(inpath, outpath) {
  kmeans(x=getParam())
}