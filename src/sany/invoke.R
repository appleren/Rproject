standardizationmm<-function(x) {
  min<-min(x)
  max<-max(x)
  r<-(x-min)/(max-min)
  r
}