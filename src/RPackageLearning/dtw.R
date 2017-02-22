library(dtw)
data("aami3a")
ref<-window(aami3a, start=0, end=2)
test<-window(aami3a, start=2.7, end=5)
al<-dtw(test, ref, keep=TRUE)
al$distance
al2<-dtw(test, ref, step.pattern=asymmetric, keep=TRUE)
al2$distance

query<-cbind(1:10, 1)
ref<-cbind(11:15, 2)
haha<-dtw(query, ref, dist.method="Manhattan")
haha<-dtw(query, ref)
