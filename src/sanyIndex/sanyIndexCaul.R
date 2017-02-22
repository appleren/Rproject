
# names(sdata)
# [1] "province"            "year"                "month"               "zhufangmianji"       "zhufangtouzi"        "newStartWork"       
# [7] "houseindex"          "shigongmianji"       "车载泵_all_worktime" "搅拌车_all_worktime" "泵车_all_worktime"   "拖泵_all_worktime"  
# [13] "压路机_all_worktime" "挖机_all_worktime"   "搅拌车_avgworktime"  "拖泵_avgworktime"    "车载泵_avgworktime"  "压路机_avgworktime" 
# [19] "bcAvgWorkhour"       "wjMonthlyTime"       "monthly_worktime"    "车载泵_all_qty"      "搅拌车_all_qty"      "拖泵_all_qty"       
# [25] "压路机_all_qty"      "bcAllNo"             "bcEffectiveNo" 

sdata<-read.csv("../../data/sanyIndex/mid/standarded.csv", header=TRUE)
gdpIndex<-subset(sdata, select=c("province", "year", "month", "zhufangmianji", "zhufangtouzi", "houseindex", "shigongmianji", "diffShigongmianji", "diffNewStartWork", "newStartWork"))

allWorkTime<-subset(sdata, select=c("province", "year", "month", "车载泵_all_worktime", "搅拌车_all_worktime",
                                  "泵车_all_worktime", "压路机_all_worktime", "挖机_all_worktime", "拖泵_all_worktime"))
allAvgworktime<-subset(sdata, select=c("province", "year", "month", "车载泵_avgworktime", "搅拌车_avgworktime",
                                  "bcAvgWorkhour",  "压路机_avgworktime",  "wjMonthlyTime", "拖泵_avgworktime"))
all_qty<-subset(sdata, select=c("province", "year", "month", "车载泵_all_qty", "搅拌车_all_qty",
                                "bcAllNo", "压路机_all_qty", "wjEffectveNo", "拖泵_all_qty"))

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
