
# names(sdata)
# [1] "province"            "year"                "month"               "zhufangmianji"       "zhufangtouzi"        "newStartWork"       
# [7] "houseindex"          "shigongmianji"       "���ر�_all_worktime" "���賵_all_worktime" "�ó�_all_worktime"   "�ϱ�_all_worktime"  
# [13] "ѹ·��_all_worktime" "�ڻ�_all_worktime"   "���賵_avgworktime"  "�ϱ�_avgworktime"    "���ر�_avgworktime"  "ѹ·��_avgworktime" 
# [19] "bcAvgWorkhour"       "wjMonthlyTime"       "monthly_worktime"    "���ر�_all_qty"      "���賵_all_qty"      "�ϱ�_all_qty"       
# [25] "ѹ·��_all_qty"      "bcAllNo"             "bcEffectiveNo" 

sdata<-read.csv("../../data/sanyIndex/mid/standarded.csv", header=TRUE)
gdpIndex<-subset(sdata, select=c("province", "year", "month", "zhufangmianji", "zhufangtouzi", "houseindex", "shigongmianji", "diffShigongmianji", "diffNewStartWork", "newStartWork"))

allWorkTime<-subset(sdata, select=c("province", "year", "month", "���ر�_all_worktime", "���賵_all_worktime",
                                  "�ó�_all_worktime", "ѹ·��_all_worktime", "�ڻ�_all_worktime", "�ϱ�_all_worktime"))
allAvgworktime<-subset(sdata, select=c("province", "year", "month", "���ر�_avgworktime", "���賵_avgworktime",
                                  "bcAvgWorkhour",  "ѹ·��_avgworktime",  "wjMonthlyTime", "�ϱ�_avgworktime"))
all_qty<-subset(sdata, select=c("province", "year", "month", "���ر�_all_qty", "���賵_all_qty",
                                "bcAllNo", "ѹ·��_all_qty", "wjEffectveNo", "�ϱ�_all_qty"))

all_qty_percent<-all_qty[, c(-1,-2,-3)]/rowSums(all_qty[, c(-1,-2,-3)])

all_qty_percent[is.na(all_qty_percent)]<-0

finalQty<-rowSums(all_qty[, c(-1,-2,-3)]*all_qty_percent)

finalWT<-rowSums(allWorkTime[, c(-1,-2,-3)]*all_qty_percent)

finalAvgworktime<-rowSums(allAvgworktime[, c(-1,-2,-3)]*all_qty_percent)

finald<-cbind(allAvgworktime[,c(1,2,3)], finalWT, finalAvgworktime, finalQty, gdpIndex[, c(-1,-2,-3)])

rcor<-ddply(finald, .(province), function(x) {
  prov<-x$province[1]
  x<-removeSeasonalDF(standardizationDF(x, id.vars=c("province", "year", "month"))
                      , 10, id.vars=c("province", "year", "month"), rt=c("t", "e"), title=prov)
  calmcor(x[,c("zhufangmianji", "zhufangtouzi", "houseindex", "shigongmianji", 
               "diffShigongmianji", "diffNewStartWork", "newStartWork")],
          x[, c("finalWT","finalAvgworktime","finalQty")], 
          paste("data/result/", prov, "corfinal.csv", sep=""), FALSE)
})

write.csv(rcor, "../../data/sanyIndex/result/rcorfinal.csv")

rccf<-ddply(finald, .(province), function(x) {
  prov<-x$province[1]
  x<-removeSeasonalDF(standardizationDF(x, id.vars=c("province", "year", "month")), 10, id.vars=c("province", "year", "month"), rt=c("t", "e"), title=prov)
  calCCF(x[,c("zhufangmianji", "zhufangtouzi", "houseindex", "shigongmianji", 
              "diffShigongmianji", "diffNewStartWork", "newStartWork")],
         x[, c("finalWT","finalAvgworktime","finalQty")], 
         paste("data/result/", prov, "ccf.csv", sep=""), FALSE)
})
write.csv(rccf, "../../data/sanyIndex/result/rccffinal.csv")
