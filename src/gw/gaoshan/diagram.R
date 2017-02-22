library(ggplot2)
library(plyr)

#two dims: 1, schemaIndex, tidCount
a<-data.frame(c1=c(1,2,3,4), c2=c(1,2,1,2))
ggplot(a, aes(x=c2))+geom_histogram(binwidth = 1, fill="lightblue", colour="black")
ggplot(a, aes(x=c1, y=c2))+stat_density2d(aes(fill=..density..), geom="raster", contour = FALSE)

#old data
datapath<-"../../data/gw/gw-rt-data-schm/data"
resultpath<-"../../data/gw/gw-rt-data-schm/result"
tmp1<-read.csv(paste0(resultpath, "/sortedResult.csv"))
a<-data.frame(tid=as.integer(as.factor(as.character(tmp1$tid))), id=as.integer(tmp1$id))
# a$id<-as.factor(a$id)
# a<-data.frame(tid=as.integer(as.factor(as.character(tmp$tid))), id=as.factor(as.integer(tmp$id)))
# b<-aggregate(tid~id, data=a, length)
# b<-b[order(b$tid),]
# write.csv(b, "b.csv")

b<-read.csv("b.csv")
b$index<-seq(from=1, to=length(b$id))
d<-subset(b, schema_id==2)
c<-ddply(b, .(schema_id), function(d){
  r<-subset(a, id==d$schema_id[1])
  index<-d$index[1]
  r$id=d$index
  r
})
a<-c[,c("tid", "id")]

a1<-a

ac<-tmp1[, c("id", "colcount", "tid")]
ac$id<-as.integer(ac$id)
ac$tid<-as.integer(as.factor(ac$tid))
ggplot(ac, aes(x=id, y=colcount))+geom_point()
p1<-ggplot(ac, aes(x=id, y=tid))+geom_point(color="blue",size=1.5)+xlab("schema")+ylab("turbine")
p1+coord_polar(theta="x")


#new data
datapath<-"../../data/gw/gaoshan"
resultpath<-"../../data/gw/gaoshan/result"

# tmp<-read.csv(paste0(resultpath, "/sortedResult.csv"))
# write.csv(assets, paste0(resultpath, "/assets.csv"))
tmp<-read.csv(paste0(resultpath, "/assets.csv"))

a<-data.frame(tid=as.integer(as.factor(as.character(tmp$tid))), id=as.integer(tmp$fieldGroupId))

# a$id<-as.factor(a$id)
# a<-data.frame(tid=as.integer(as.factor(as.character(tmp$tid))), id=as.factor(as.integer(tmp$id)))
# b<-aggregate(tid~id, data=a, length)
# b<-b[order(b$tid),]
# write.csv(b, "bb.csv")
b<-read.csv("bb.csv")
b$index<-seq(from=1, to=length(b$id))
d<-subset(b, schema_id==2)
c<-ddply(b, .(schema_id), function(d){
  r<-subset(a, id==d$schema_id[1])
  index<-d$index[1]
  r$id=d$index
  r
})
a<-c[,c("tid", "id")]
a2<-a

ac<-a2
ac$id<-as.integer(ac$id)
ac$tid<-as.integer(as.factor(ac$tid))
p<-ggplot(ac, aes(x=id, y=tid))+geom_point(color="blue",size=1.5)+xlab("schema")+ylab("turbine")
p+coord_polar(theta="x")

a1$type<-"blue"
a2$type<-"yellow"
aa<-rbind(a1, a2)
aa<-rbind(a2, a1)

aa$type<-as.factor(aa$type)

ggplot(a1, aes(x=id, fill=type))+geom_density(alpha=0.8,fill="yellow")+xlim(1,150)+ylim(0,0.19)+xlab("schema")+ylab("turbine density")
ggplot(a2, aes(x=id, fill=type))+geom_density(alpha=0.5,fill="blue")+xlim(1,20)+ylim(0,1.8)+xlab("schema")+ylab("turbine density")
ggplot(aa, aes(x=id, fill=type))+geom_density(alpha=1, aes(fill=type))+xlab("schema")+ylab("turbine density")

# a3<-a1[round(runif(100,1,length(a1$id))),]
# a4<-a3
# a3$id<-a3$id/4
# ggplot(a3, aes(x=id, fill=type))+geom_density(alpha=0.5,fill="blue")+xlim(1,150)+xlab("schema")+ylab("turbine density")

  scale_fill_brewer(palette="YlOrRd")

ggplot(aa, aes(x=id, fill=type))+geom_histogram(position="identity", alpha=0.4)

