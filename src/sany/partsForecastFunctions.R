gdp<-read.csv("../../data/sany/gdp1.csv")
colnames(gdp)<-c("year","month","fdckf", "gdzctz", "dycy")

y<-merge(oy, gdp, all=TRUE, by=c("year", "month"))
y[is.na(y)]<-0
y$fdckf<-c(0, diff(y$fdckf))
y$gdzctz<-c(0, diff(y$gdzctz))
y$dycy<-c(0, diff(y$dycy))
n<-0:42
y$order_qty[which(n%%12==0)+1]<-y$order_qty[which(n%%12==0)+2]+y$order_qty[which(n%%12==0)]
y<-subset(y, y$fdckf>0)

stl1<-stlm(ts1, s.window=7)
stl2<-stlm(ts2, s.window=7)
stl3<-stlm(ts3, s.window=7)
stl4<-stlm(ts4, s.window=7)

s1<-stl1$stl$time.series[,"trend"]
s2<-stl2$stl$time.series[,"trend"]
s3<-stl3$stl$time.series[,"trend"]
s4<-stl4$stl$time.series[,"trend"]