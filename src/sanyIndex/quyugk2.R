library(plyr)
library(reshape2)
library(ggplot2)
source("functions2.R")

#=========3 gdp==============
# xiangmushu<-read.csv("data/xiangmushu.csv", header=TRUE)
# zhufangmianji<-read.csv("data/zhufangmianji.csv", header=TRUE)
# zhufangtouzi<-read.csv("data/zhufangtuozi.csv", header=TRUE)
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
# write.csv(econ, "data/mid/econ.csv", row.names=FALSE)

#==============read new econs================
newStartWork<-read.csv("../../data/sanyIndex/newStartWork.csv", header=TRUE)
colnames(newStartWork)[4:35]<-substr(colnames(newStartWork)[4:35], 17,20)
newStartWorkM<-melt(newStartWork, id.vars=c("date", "year", "month"))
newStartWorkM<-subset(newStartWorkM, select=-1)
colnames(newStartWorkM)<-c("year", "month", "province", "newStartWork")

houseindex<-read.csv("../../data/sanyIndex/houseindex.csv", header=TRUE)
colnames(houseindex)[4:35]<-substr(colnames(houseindex)[4:35], 25,27)
houseindexM<-melt(houseindex, id.vars=c("date", "year", "month"))
houseindexM<-subset(houseindexM, select=-1)
colnames(houseindexM)<-c("year", "month", "province", "houseindex")

shigongmianji<-read.csv("../../data/sanyIndex/shigongmianji.csv", header=TRUE)
colnames(shigongmianji)[4:35]<-substr(colnames(shigongmianji)[4:35], 15,17)
shigongmianjiM<-melt(shigongmianji, id.vars=c("date", "year", "month"))
shigongmianjiM<-subset(shigongmianjiM, select=-1)
colnames(shigongmianjiM)<-c("year", "month", "province", "shigongmianji")


econ<-read.csv("../../data/sanyIndex/econ.csv", header=TRUE)

mapping<-read.csv("../../data/sanyIndex/provc.csv", header=TRUE)
y<-econ
y$province<-as.character(y$province)
for(i in 1:length(mapping[,1])){
  m<-mapping[i,]
  y$province[which(y$province==m$c1)]<-as.character(m$c2)
}


y<-merge(y, newStartWorkM, by=c("province", "year", "month"), all.x=TRUE)
y$newStartWork[is.na(y$newStartWork)]<-0
y<-merge(y, houseindexM, by=c("province", "year", "month"), all.x=TRUE)
y$houseindex[is.na(y$houseindex)]<-0
y<-merge(y, shigongmianjiM, by=c("province", "year", "month"), all.x=TRUE)
y$shigongmianji[is.na(y$shigongmianji)]<-0

y<-ddply(y, .(province), function(x){
  for (i in c(5:7)){
    x[,i]<-c(0,diff(x[,i],1))
  }
  x
})
y$diffShigongmianji<-c(0, diff(y$shigongmianji, 1))
y$diffNewStartWork<-c(0, diff(y$newStartWork, 1))
y<-subset(y,month>2)
y<-subset(y, !(year==2009&month==10))

y<-subset(y, select=-4)

#===========1 operate rate===================
# bc_or<-read.csv("../../data/sanyIndex/bc_operator_rate.csv", header=TRUE)
# colnames(bc_or)[1]<-"rate"
# d<-colnames(bc_or)[-1]
# d<-substr(d, 2, 5)
# t1<-t(data.frame(bc_or, row.names=1))
# t2<-as.data.frame(t1, row.names=F)
# #rete data and provicne
# t3<-cbind(d, t2)

#---------read from file- old -------------
# t3<-read.csv("../../data/sanyIndex/t3.csv", header=TRUE)

#=============province rate ccf========================
# t3[is.na(t3)]<-0
# provRatet<-removeSeasonalDF(standardizationDF(t3, id.vars=c("d")), 10, id.vars=c("d"), rt=c("t"))
# cor(provRatet[, -1])
# calmcor(provRatet[, -1], provRatet[, -1], "../../data/sanyIndex/result/provRateT.csv")

#=====================
# bc<-melt(t3, id.vars=c("d"))
# bc$year<-as.numeric(paste("20",substr(bc$d, 1,2), sep=""))
# bc$month<-as.numeric(substr(bc$d, 3, 4))
# colnames(bc)<-c("date", "province", "rate", "year", "month")
# write.csv(bc, "data/mid/bcrate.csv", row.names=FALSE)
# write.csv(t3, "data/mid/t3.csv", row.names=FALSE)


#-----------use bc for waji and bengche--11.18-------
# waji<-read.csv("../../data/sanyIndex/waji.csv", header=TRUE)
# waji$province<-as.character(waji$province)
# for(i in 1:length(mapping[,1])){
#   m<-mapping[i,]
#   waji$province[which(waji$province==m$c3)]<-as.character(m$c2)
# }
# write.csv(waji, "../../data/sanyIndex/mid/waji.csv")
# bcnumber<-read.csv("../../data/sanyIndex/bengchenumber.csv", header=TRUE)
# bcworktime<-read.csv("../../data/sanyIndex/bengcheshigongliang.csv", header=TRUE)
# bc<-merge(bcnumber, bcworktime, by=c("province", "year", "month"))
# bc<-merge(bc, waji, by=c("province", "year", "month"))
# bc<-subset(bc, !(year==2015&month>8), select=c(1,2,3,5,7,8,10,11))
# colnames(bc)<-c("province", "year", "month", "bcAvgWorkhour", "bcAllNo",
#                 "bcEffectiveNo", "wjMonthlyTime", "wjEffectveNo")

#===================read new bc===11.18===================
bc<-read.csv("../../data/sanyIndex/bc.csv", header=TRUE)
bc<-subset(bc, month>2)


#============merge bc and gdp==============
gcCol<-c("车载泵_all_worktime", "搅拌车_all_worktime", "泵车_all_worktime",
         "拖泵_all_worktime", "压路机_all_worktime", "挖机_all_worktime",
         "搅拌车_avgworktime", "拖泵_avgworktime", "车载泵_avgworktime", 
         "压路机_avgworktime", "bcAvgWorkhour", 
         "wjMonthlyTime", "monthly_worktime", "车载泵_all_qty", "搅拌车_all_qty",
         "拖泵_all_qty", "压路机_all_qty", "bcAllNo", "bcEffectiveNo", "wjEffectveNo")
gdpCol<-c("zhufangmianji", "zhufangtouzi", "newStartWork", "houseindex", "shigongmianji", "diffShigongmianji", "diffNewStartWork")

yb<-merge(y, bc, by=c("province", "year", "month"), all.x=TRUE)
yb<-yb[order(yb[,1],yb[,2], yb[,3]),]
# yb<-subset(yb, year<2015&(!(year==2014&month==12))&year>2012)
yb<-subset(yb, year>2012)

yb<-yb[, c(gdpCol, gcCol, "year", "month", "province")]
yb<-subset(yb, province!="西藏")
# yb[is.na(yb)]<--1
# which(is.na(yb))
# length(which(yb==-1))

# yb$xiangmushu[yb$month==12]<-NA
# yb$houseindex[yb$houseindex==0]<-NA

# x<-subset(yb, province=="北京")

#print to pdf
# tmp<-ddply(yb, .(province), function(x) {
#   prov<-x$province[1]
#   ssd<-standardizationDF(x, id.vars=c("province", "year", "month"))
#   ssd
# })
# 
# write.csv(yb, "../../data/sanyIndex/mid/standarded.csv", row.names=FALSE)
# write.csv(tmp, "../../data/sanyIndex/mid/standarded.csv", row.names=FALSE)
# 
# tmp<-subset(tmp, select=c(-2,-3))
# 
# for(i in 2:length(tmp[1,])) {
#   pdfFile<-paste("../../data/sanyIndex/result/pdf/rcor", names(tmp)[i], ".pdf", sep="")
#   pdf(pdfFile, family="GB1")
#   hehe<-ddply(tmp[,c(1,i)], .(province), function(x) {
#     pro<-x$province[1]
#     x<-subset(x, select=-1)
#     xxx<-removeSeasonal(x[,1], 10, rt=c("t", "e"), title=paste(pro, colnames(x)[1]), paint=TRUE)
#     xxx
#   })
#   dev.off()
# }



# pdf(pdfFile, family="GB1")
rcor<-ddply(yb, .(province), function(x) {
  prov<-x$province[1]
  #   x$rateCumSum<-cumsum(x$rate)
  x<-removeSeasonalDF(standardizationDF(x, id.vars=c("province", "year", "month")), 10, id.vars=c("province", "year", "month"), rt=c("t", "e"), title=prov)
  calmcor(x[,gdpCol], x[, gcCol], paste("data/result/", prov, "cor.csv", sep=""), FALSE)
  #   calmcor(x[,c("xiangmushu", "zhufangmianji", "zhufangtouzi")], x[, c("rate", "rateCumSum")], paste("data/result/", prov, "cor.csv", sep=""), FALSE)
  #   calCCF(x[,c("xiangmushu", "zhufangmianji", "zhufangtouzi")], x[, c("rate", "rateCumSum")], paste("data/result/", prov, "ccf.csv", sep=""))
})
write.csv(rcor, "../../data/sanyIndex/result/rcor.csv")


rccf<-ddply(yb, .(province), function(x) {
  prov<-x$province[1]
  #   x$rateCumSum<-cumsum(x$rate)
  x<-removeSeasonalDF(standardizationDF(x, id.vars=c("province", "year", "month")), 10, id.vars=c("province", "year", "month"), rt=c("t", "e"), title=prov)
  #   calmcor(x[,c("xiangmushu", "zhufangmianji", "zhufangtouzi")], x[, c("rate", "rateCumSum")], paste("data/result/", prov, "cor.csv", sep=""))
  #   calCCF(x[,c("xiangmushu", "zhufangmianji", "zhufangtouzi")], x[, c("rate", "rateCumSum")], paste("data/result/", prov, "ccf.csv", sep=""), FALSE)
  calCCF(x[,gdpCol], x[, gcCol], paste("data/result/", prov, "ccf.csv", sep=""), FALSE)
})
write.csv(rccf, "../../data/sanyIndex/result/rccf.csv")

#======================results=====================
result<-read.csv("../../data/sanyIndex/result/rccf.csv", header=TRUE)
ss<-ddply(result, .(Index1, Index2), function(d){
  t<-subset(d, abs(ccfVal)>=0.8)
  n<-length(t$province)
  if(n>=10)
    t
})

write.csv(ss, "../../data/sanyIndex/result/rccfss.csv")
