library(plyr)
library(reshape2)
library(ggplot2)
library(rpart)
source("functions.R")

#=========3 gdp==============
# xiangmushu<-read.csv("../../data/sanyIndex/xiangmushu.csv", header=TRUE)
# zhufangmianji<-read.csv("../../data/sanyIndex/zhufangmianji.csv", header=TRUE)
# zhufangtouzi<-read.csv("../../data/sanyIndex/zhufangtuozi.csv", header=TRUE)
# 
# xiangmushu<-subset(xiangmushu, select=c("Province", "Level", "year", "month", "Month"))
# colnames(xiangmushu)<-c("province", "xiangmushu", "year", "month", "Month")
# zhufangmianji<-subset(zhufangmianji, select=c("Province", "Level", "year", "month", "Month"))
# colnames(zhufangmianji)<-c("province", "zhufangmianji", "year", "month", "Month")
# zhufangtouzi<-subset(zhufangtouzi, select=c("Province", "Level", "year", "month", "Month"))
# colnames(zhufangtouzi)<-c("province", "zhufangtouzi", "year", "month", "Month")
# 
# econ<-merge(xiangmushu, zhufangmianji, by=c("province", "Month", "year", "month"), all.x=TRUE)
# econ<-merge(econ, zhufangtouzi, by=c("province", "Month", "year", "month"), all.x=TRUE)
# econ[is.na(econ)]<-0
# econ<-subset(econ, !(year==2015&month==7))
# write.csv(econ, "../../data/sanyIndex/mid/econ.csv", row.names=FALSE)

newStartWork<-read.csv("../../data/sanyIndex/newStartWork.csv", header=TRUE)
colnames(newStartWork)[3:34]<-substr(colnames(newStartWork)[3:34], 17, 20)
newStartWorkM<-melt(newStartWork, id.vars=c("date", "Month"))
newStartWorkM<-subset(newStartWorkM, select=-1)
colnames(newStartWorkM)<-c("Month", "province", "newStartWork")

econ<-read.csv("../../data/sanyIndex/econ.csv", header=TRUE)

y<-econ
mapping<-read.csv("../../data/sanyIndex/provc.csv")
y$province<-as.character(y$province)
for(i in 1:length(mapping[,1])){
  m<-mapping[i,]
  y$province[which(y$province==m$c1)]<-as.character(m$c2)
}

y<-merge(y, newStartWorkM, by=c("province", "Month"))

y<-ddply(y, .(province), function(x){
  for (i in c(5:8)){
    x[,i]<-c(0,diff(x[,i],1))
  }
  x
})
# y$diffzfmj<-c(0, diff(y$zhufangmianji))
# y$diffzftz<-c(0, diff(y$zhufangtouzi))
# y$diffxms<-c(0, diff(y$xiangmushu))
y<-subset(y,month>2)
y<-subset(y, !(year==2009&month==10))
# y1<-subset(y, province=="Beijing")


#===========1 operate rate===================
# bc_or<-read.csv("../../data/sanyIndex/bc_operator_rate.csv", header=TRUE)
# colnames(bc_or)[1]<-"rate"
# d<-colnames(bc_or)[-1]
# d<-substr(d, 2, 5)
# t1<-t(data.frame(bc_or, row.names=1))
# t2<-as.data.frame(t1, row.names=F)
# #rete data and provicne
# t3<-cbind(d, t2)

t3<-read.csv("../../data/sanyIndex/t3.csv", header=TRUE)

#=====================
bc<-melt(t3, id.vars=c("d"))
bc$year<-as.numeric(paste("20",substr(bc$d, 1,2), sep=""))
bc$month<-as.numeric(substr(bc$d, 3, 4))
colnames(bc)<-c("date", "province", "rate", "year", "month")
write.csv(bc, "../../data/sanyIndex/mid/bcrate.csv", row.names=FALSE)
# write.csv(t3, "../../data/sanyIndex/mid/t3.csv", row.names=FALSE)

#================kmeans===================
t3[is.na(t3)]<-0
cn<-t3[,1]
t4<-as.data.frame(t(t3))
colnames(t4)<-cn
t4<-t4[-1,]
t4<-cbind(province=rownames(t4), t4)
t5<-subset(t4, select=c(-1))
set.seed(252964)
(kmeans <- kmeans(na.omit(t5), 5))
a<-as.vector(kmeans$cluster)
t4<-cbind(a, t4)
t4<-t4[order(t4$a),]

# bc1<-merge(bc, subset(t4, province!="全国", select=c("a", "province")), by=c("province"))
# bc1$date<-as.Date(paste(bc1$year, "/", bc1$month, "/1", sep=""))
# qplot( x=date, y=rate, data=subset(bc1, a==1), colour=province, geom="line", main=paste("????=",1))
# qplot( x=date, y=rate, data=subset(bc1, a==2), colour=province, geom="line", main=paste("????=",2))
# qplot( x=date, y=rate, data=subset(bc1, a==3), colour=province, geom="line", main=paste("????=",3))
# qplot( x=date, y=rate, data=subset(bc1, a==4), colour=province, geom="line", main=paste("????=",4))
# qplot( x=date, y=rate, data=subset(bc1, a==5), colour=province, geom="line", main=paste("????=",5))
# qplot( x=date, y=rate, data=subset(bc1, a==6), colour=province, geom="line", main=paste("a=",a))
# qplot( x=date, y=rate, data=subset(bc1, a==7), colour=province, geom="line", main=paste("a=",a))
#=============province rate ccf========================
t3[is.na(t3)]<-0
provRatet<-removeSeasonalDF(standardizationDF(t3, id.vars=c("d")), 10, id.vars=c("d"), rt=c("t"))
cor(provRatet[, -1])
calmcor(provRatet[, -1], provRatet[, -1], "../../data/sanyIndex/result/provRateT.csv")

#============merge bc and gdp==============
bc<-subset(bc, month>2)
# write.csv(y, "../../data/sanyIndex/mid/y.csv", row.names=FALSE)
yb<-merge(bc, y, by=c("province", "year", "month"))
yb<-yb[order(yb[,1],yb[,2], yb[,3]),]
yb<-subset(yb, select=c(-4,-6 ))

yb$xiangmushu[yb$month==12]<-NA
# yb$diffxms[yb$month==12]<-NA
# yb$diffzfmj[yb$month==3]<-NA
# yb$diffzftz[yb$month==3]<-NA
# yb$diffxms[yb$month==3]<-NA
# x<-subset(yb, province=="北京")
# rcor<-ddply(yb, .(province), function(x) {
#   prov<-x$province[1]
#   x[is.na(x)]<-0
#   x$rateCumSum<-cumsum(x$rate)
#   x<-removeSeasonalDF(standardizationDF(x, id.vars=c("province", "year", "month")), 10, id.vars=c("province", "year", "month"), rt=c("t", "e"))
#   calmcor(x[,c("xiangmushu", "zhufangmianji", "zhufangtouzi")], x[, c("rate", "rateCumSum")], paste("../../data/sanyIndex/result/", prov, "cor.csv", sep=""), FALSE)
# #   calCCF(x[,c("xiangmushu", "zhufangmianji", "zhufangtouzi")], x[, c("rate", "rateCumSum")], paste("../../data/sanyIndex/result/", prov, "ccf.csv", sep=""))
# })
# write.csv(rcor, "../../data/sanyIndex/result/rcor.csv")

rccf<-ddply(yb, .(province), function(x) {
  prov<-x$province[1]
  x[is.na(x)]<-0
  x<-removeSeasonalDF(standardizationDF(x, id.vars=c("province", "year", "month")), 10, id.vars=c("province", "year", "month"), rt=c("t", "e"))
  x$rateCumSum<-ts(cumsum(x$rate), frequency=10)
#   x$diffxms<-c(NA, diff(x$xiangmushu))
#   x$diffzftz<-ts(c(NA, diff(x$zhufangmianji)), frequency=10)
#   x$diffzfmj<-ts(c(NA, diff(x$zhufangtouzi)), frequency=10)
#   plot(x$rateCumSum)
  calCCF(x[,c("xiangmushu", "zhufangmianji", "zhufangtouzi", "newStartWork")], x[, c("rate", "rateCumSum")], paste("../../data/sanyIndex/result/", prov, "ccf.csv", sep=""), FALSE)
})
write.csv(rccf, "../../data/sanyIndex/result/rccf.csv")

# rccf1<-rccf
# rccf1$ccfName<-paste(rccf1$Index1, rccf1$Index2, sep="_")
# rccf2<-dcast(province~ccfName, value.var="ccfVal", data=rccf1)
# final<-merge(yb, rccf2, by=c("province"), all.x=TRUE)
# write.csv(final, "../../data/sanyIndex/mid/final.csv")
