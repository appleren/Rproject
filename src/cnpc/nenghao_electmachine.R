library(XLConnect)
library(reshape2)
library(zoo)
library(plyr)

#read data from excel files
datapath<-"../../data/cnpc/nenghao/"
filenames<-list.files(path=paste(datapath, "cz1/", sep=""), pattern=".xls")
stationRunningParam<-data.frame()
clientOutput<-data.frame()
compressorParam<-data.frame()
consumption<-data.frame()
gasCondition<-data.frame()

for(filename in filenames) {
  d<-as.Date(ISOdate(substr(filename, 1, 4), substr(filename, 5,6), substr(filename, 7,8)))
  #read xls file
  xls <- loadWorkbook(paste(datapath, "cz1/", filename, sep=""),create=FALSE)
  
  #场站运行参数
  s<-readWorksheet(xls, "Sheet1", region="c4:d8", header=FALSE)
  colnames(s)<-c("para_name", "value")
  s<-dcast(s, .~para_name)
  s<-subset(s, select=-1)
  colnames(s)<-paste("场站", colnames(s), sep="_")
  s$date<-rep(d, form=1, to=length(s$linename))
  stationRunningParam<-rbind(stationRunningParam, s)
  
  #客户分输
  s<-readWorksheet(xls, "Sheet1", region="c10:C10", header=FALSE)
  clientName<-s[1,1]
  s<-readWorksheet(xls, "Sheet1", region="c11:d13", header=FALSE)
  colnames(s)<-c("para_name", "value")
  s<-dcast(s, .~para_name)
  s<-subset(s, select=-1)
  colnames(s)<-paste("客户分输", colnames(s), sep="_")
  s$client_name<-clientName
  s$date<-rep(d, form=1, to=length(s$linename))
  clientOutput<-rbind(clientOutput, s)
  
  s<-readWorksheet(xls, "Sheet1", region="c14:C14", header=FALSE)
  clientName<-s[1,1]
  s<-readWorksheet(xls, "Sheet1", region="c15:d17", header=FALSE)
  colnames(s)<-c("para_name", "value")
  s<-dcast(s, .~para_name)
  s<-subset(s, select=-1)
  colnames(s)<-paste("客户分输", colnames(s), sep="_")
  s$client_name<-clientName
  s$date<-rep(d, form=1, to=length(s$linename))
  clientOutput<-rbind(clientOutput, s)
  
  s<-readWorksheet(xls, "Sheet1", region="c18:C18", header=FALSE)
  clientName<-s[1,1]
  s<-readWorksheet(xls, "Sheet1", region="c19:d21", header=FALSE)
  colnames(s)<-c("para_name", "value")
  s<-dcast(s, .~para_name)
  s<-subset(s, select=-1)
  colnames(s)<-paste("客户分输", colnames(s), sep="_")
  s$client_name<-clientName
  s$date<-rep(d, form=1, to=length(s$linename))
  clientOutput<-rbind(clientOutput, s)
  
  #压缩机运行参数
  s<-readWorksheet(xls, "Sheet1", region="b23:d57", header=FALSE)
  colnames(s)<-c("comp_no", "para_name", "value")
  s$comp_no<-na.locf(s$comp_no)
  s<-subset(s, !is.na(para_name))
  s<-dcast(s, comp_no~para_name)
  colnames(s)<-paste("压缩机", colnames(s), sep="_")
  s$date<-rep(d, form=1, to=length(s$linename))
  compressorParam<-rbind(compressorParam, s)
  
  #能耗
  s<-readWorksheet(xls, "Sheet1", region="c59:d66", header=FALSE)
  colnames(s)<-c("para_name", "value")
  s<-dcast(s, .~para_name)
  s<-subset(s, select=-1)
  s$date<-rep(d, form=1, to=length(s$linename))
  consumption<-rbind(consumption, s)
  
  #气质报告
  s<-readWorksheet(xls, "Sheet1", region="c68:d82", header=FALSE)
  colnames(s)<-c("para_name", "value")
  s<-dcast(s, .~para_name)
  s<-subset(s, select=-1)
  s$date<-rep(d, form=1, to=length(s$linename))
  gasCondition<-rbind(gasCondition, s)
}

# stationRunningParam<-data.frame() 场站运行参数
# clientOutput<-data.frame() 客户分输
# compressorParam<-data.frame() 压缩机运行参数
# consumption<-data.frame() 能耗
# gasCondition<-data.frame() 气质分析报告

colnames(stationRunningParam)<-c("cz_outtemp", "cz_outP", "cz_earthtemp", "cz_intemp", "cz_inP", "date")
colnames(clientOutput)<-c("fs_qty", "fs_temp", "fs_p", "client_name", "date")
colnames(compressorParam)<-c("comp_no", "comp_outtemp", "comp_outP", "comp_intemp", "comp_inP", "comp_time", "comp_speed", "date")
colnames(consumption)<-c("other", "no1", "no2", "no3", "no4", "no5", "all", "all_p", "date")
colnames(gasCondition)<-c("C2H6", "C3H8", "C6", "CH4", "CO2", "H2S", "iC4H10", "iC5H12", "nC4H10", "nC5H12", "N2", "low_heat", "high_heat", "water_dew_point", "hydrocarbon_dew_point", "date")

write.csv(gasCondition, paste(datapath, "cz1_mid/气质分析报告_tmp.csv", sep=""), row.names=FALSE)
gasCondition<-read.csv(paste(datapath, "cz1_mid/气质分析报告_tmp.csv", sep=""))

tmp<-data.frame(CH4=rep(0, 366),C2H4=rep(0, 366),C2H6=rep(0, 366),
                C3H6=rep(0, 366),C3H8=rep(0, 366),
                iC4H10=rep(0, 366),nC4H10=rep(0, 366),
                iC5H12=rep(0, 366),
                nC5H12=rep(0, 366),C6H14=rep(0, 366),
                C7H16=rep(0, 366),C8H18=rep(0, 366),
                C9H20=rep(0, 366),C10H22=rep(0, 366),
                C11H24=rep(0, 366),N2=rep(0, 366),
                CO2=rep(0, 366),H2S=rep(0, 366))
tmp$date<-gasCondition$date
tmp[,which(colnames(tmp)%in%colnames(gasCondition))]<-gasCondition[, colnames(tmp)[which(colnames(tmp)%in%colnames(gasCondition))]]

tmp$high_heat<-gasCondition$high_heat
tmp$water_dew_point<-gasCondition$water_dew_point

gasCondition<-tmp
gasCondition$date<-as.Date(gasCondition$date)

#气体组分,先后顺序为CH4,C2H4,C2H6,C3H6,C3H8,iC4H10,nC4H10,iC5H12,nC5H12,C6H14,C7H16,C8H18,C9H20,C10H22,C11H24,N2,CO2,H2S

gasCondition<-merge(gasCondition, stationRunningParam, by="date", all=TRUE)

write.csv(stationRunningParam, paste(datapath, "cz1_mid/场站运行参数.csv", sep=""))
write.csv(clientOutput, paste(datapath, "cz1_mid/客户分输.csv", sep=""))
write.csv(compressorParam, paste(datapath, "cz1_mid/压缩机运行参数.csv", sep=""))
write.csv(consumption, paste(datapath, "cz1_mid/能耗.csv", sep=""))
write.csv(gasCondition, paste(datapath, "cz1_mid/气质分析报告.csv", sep=""))



#----------------------Read data-------------------
consumption<-read.csv(paste(datapath, "cz1_mid/能耗.csv", sep=""))
stationRunningParam<-read.csv(paste(datapath, "cz1_mid/场站运行参数.csv", sep=""))
clientOutput<-read.csv(paste(datapath, "cz1_mid/客户分输.csv", sep=""))
compressorParam<-read.csv(paste(datapath, "cz1_mid/压缩机运行参数.csv", sep=""))
gasCondition<-read.csv(paste(datapath, "cz1_mid/气质分析报告.csv", sep=""))

# 总能耗分析  
# 
# 压比  进出站气压比
# 压差  出进站气压差
# 工作量	转速乘以时间乘以压比
# 进站温度	进站温度
# 出站温度	出站温度
# 地温      地温
# 分输量	总分输量
# 气质物性	根据气质报告计算BWRS
# 
# 总压缩机能耗	五个压缩机能耗和
# 总耗气	总耗气


#总压缩机能耗 allCons
consumption$allCons<-with(consumption, no1+no2+no3+no4+no5)
# 分输量  总分输量 fs_qty
clientOutput<-aggregate(fs_qty~date, data=clientOutput, FUN=sum)
#进出站压比 p_percent
stationRunningParam<-subset(stationRunningParam, cz_outP!=0)
stationRunningParam$cz_inP[which(stationRunningParam$cz_inP==0)]<-0.0001
stationRunningParam$p_percent<-stationRunningParam$cz_inP/stationRunningParam$cz_outP
#进出站压比 p_diff
stationRunningParam$p_diff<-stationRunningParam$cz_outP-stationRunningParam$cz_inP
# 工作量  转速乘以时间乘以压比（压比稍后再算） workload
compressorParam$workload<-compressorParam$comp_speed*compressorParam$comp_time
compressorParam1<-aggregate(workload~date, data=compressorParam, FUN=sum)

#气质
gasCondition<-subset(gasCondition, select=c("date", "CH4", "C2H6", "high_heat", "water_dew_point"))

fd<-merge(consumption, clientOutput, by="date", all=TRUE)
fd<-merge(fd, stationRunningParam, by="date", all=TRUE)
fd<-merge(fd, compressorParam1, by="date", all=TRUE)
fd<-merge(fd, gasCondition, by="date", x.all=TRUE)
fd$workload<-fd$workload*fd$p_percent

fd<-subset(fd, all_p>0)
fd<-cbind(fd, colsplit(fd$date,"-", c("year", "month", "day")))

write.csv(fd, paste(datapath, "cz1_mid/fd.csv", sep=""))


# 单位时间能耗分析  
# 
# 气质物性  根据气质报告计算BWRS
# 工作量  压比乘以转速
# 压比	进出口气压比
# 压差  出进口气压差
# 
# 单位时间能耗	单机能耗除以时间

compressorParam<-read.csv(paste(datapath, "cz1_mid/压缩机运行参数.csv", sep=""))
gasCondition<-read.csv(paste(datapath, "cz1_mid/气质分析报告.csv", sep=""))

compressorParam<-ddply(compressorParam, .(comp_no), function(d) {
  c<-paste("no", substr(d[1,1], 1, 1), sep="")
  csp<-subset(consumption, select=c("date", c))
  colnames(csp)<-c("date", "nh")
  m<-merge(d, csp, by="date", all=TRUE)
  m
})
#clean data
compressorParam<-subset(compressorParam, nh!=0 & comp_time!=0 & comp_speed!=0 &comp_outP!=0 &!is.na(comp_outP))
#单位时间能耗  单机能耗除以时间 nh1
compressorParam$nh1<-compressorParam$nh/compressorParam$comp_time
# 压比  进出口气压比 p_percent
compressorParam$comp_inP[which(compressorParam$comp_inP==0)]<-0.0001
compressorParam$p_percent<-compressorParam$comp_inP/compressorParam$comp_outP
# 压差  出进口气压差 p_diff
compressorParam$p_diff<-compressorParam$comp_outP-compressorParam$comp_inP
# 工作量  压比乘以转速 workload
compressorParam$workload<-compressorParam$p_percent*compressorParam$comp_speed

#气质
gasCondition<-subset(gasCondition, select=c("date", "CH4", "C2H6", "high_heat", "water_dew_point"))

fd1<-compressorParam
fd1<-merge(fd1, gasCondition, by="date", x.all=TRUE)
fd1<-subset(fd1, comp_outtemp>=comp_intemp)
write.csv(fd1, paste(datapath, "cz1_mid/fd1.csv", sep=""))

