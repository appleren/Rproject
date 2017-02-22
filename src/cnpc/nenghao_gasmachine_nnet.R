
library(nnet)
library(rpart)
library(reshape2)
library(zoo)
library(plyr)
library(rpart.plot)
library(forecast)
library(reshape2)
#=================functions=====================
source("accuracy.R")
#=============data prepare=========================
datapath<-"../../data/cnpc/qihao/"
#转供量 transProvide
#运行参数 runningParams
#压缩机运行参数 compressorParam
#场站能耗 consumption
#放空 emptying

# transProvide<-read.csv(paste(datapath, "cz1_mid/转供量.csv", sep=""), header=TRUE)
# runningParams<-read.csv(paste(datapath, "cz1_mid/运行参数.csv", sep=""), header=TRUE)
# consumption<-read.csv(paste(datapath, "cz1_mid/场站能耗.csv", sep=""), header=TRUE)
# emptying<-read.csv(paste(datapath, "cz1_mid/放空.csv", sep=""), header=TRUE)

# > colnames(compressorParam)
# [1] "com_gas_consumption" "com_in_pressure"     "com_in_temperature"  "com_out_pressure"   
# [5] "com_out_temperature" "speed"               "working_time"        "date"               
# [9] "com_no"  
#
#单压缩机能耗
#预测目标：
#单位时间耗气：耗气量/运行时间  com_gas_csmt_phour
#
#参数：
#工作强度（转速）：转速         speed
#压差：出口压力-进口压力        com_pressure_diff
#压比：进口压力/出口压力        com_pressure_ratio
#温差：出口温度-进口温度        com_temperature_diff


compressorParam<-read.csv(paste(datapath, "cz1_mid/压缩机运行参数.csv", sep=""), header=TRUE)

#cleandata
compressorParam<-subset(compressorParam, working_time!=0)
compressorParam<-subset(compressorParam, speed!=0)
compressorParam<-subset(compressorParam, com_out_pressure!=0)
compressorParam$com_gas_csmt_phour<-compressorParam$com_gas_consumption/compressorParam$working_time
compressorParam$com_pressure_diff<-compressorParam$com_out_pressure-compressorParam$com_in_pressure
compressorParam$com_pressure_ratio<-compressorParam$com_in_pressure/compressorParam$com_out_pressure
compressorParam$com_temperature_diff<-compressorParam$com_out_temperature-compressorParam$com_in_temperature

#-------------------------nnet--------------------------------
fd<-subset(compressorParam, 
           com_no==1, 
           select=c("com_gas_csmt_phour", "speed", "com_pressure_diff", 
                    "com_pressure_ratio", "com_temperature_diff"))
wholeData<-data.frame(scale(fd))
calnnet(wholeData, com_gas_csmt_phour~., fd$com_gas_csmt_phour, "压缩机#1耗气预测")
fd<-subset(compressorParam, 
           com_no==2, 
           select=c("com_gas_csmt_phour", "speed", "com_pressure_diff", 
                    "com_pressure_ratio", "com_temperature_diff"))
wholeData<-data.frame(scale(fd))
calnnet(wholeData, com_gas_csmt_phour~., fd$com_gas_csmt_phour, "压缩机#2耗气预测")
fd<-subset(compressorParam,
           select=c("com_gas_csmt_phour", "speed", "com_pressure_diff", 
                    "com_pressure_ratio", "com_temperature_diff"))
wholeData<-data.frame(scale(fd))
calnnet(wholeData, com_gas_csmt_phour~., fd$com_gas_csmt_phour, "燃气压缩机耗气预测")

#总能耗
#预测目标：
#总耗电：总耗电 daily_electric_consumption
#总耗气：日总耗气 daily_gas_consumption
#
#参数：
#工作量：sum(工作时间*转速)     cz_sum_workload    //总方量不可用
#压差：出站压力-进站压力        cz_pressure_diff
#压比：进站压力/出站压力        cz_pressure_ratio
#温差：出站温度-出站温度        cz_temperature_diff

# > colnames(consumption)
# [1] "daily_electric_consumption" "daily_gas_consumption"      "other_gas_consumption"      "date" 

compressorParam<-read.csv(paste(datapath, "cz1_mid/压缩机运行参数.csv", sep=""), header=TRUE)
consumption<-read.csv(paste(datapath, "cz1_mid/场站能耗.csv", sep=""), header=TRUE)
runningParams<-read.csv(paste(datapath, "cz1_mid/运行参数.csv", sep=""), header=TRUE)

#cleandata
compressorParam<-subset(compressorParam, working_time!=0)
compressorParam<-subset(compressorParam, speed!=0)
compressorParam<-subset(compressorParam, com_out_pressure!=0)
compressorParam$workload<-compressorParam$working_time*compressorParam$speed
compressorParam<-ddply(compressorParam, .(date), function(d){
  d2<-data.frame(date=d$date, cz_sum_workload=sum(d$workload))
  d2
})
runningParams<-subset(runningParams, out_pressure!=0)
runningParams$cz_pressure_diff<-runningParams$out_pressure-runningParams$in_pressure
runningParams$cz_pressure_ratio<-runningParams$in_pressure/runningParams$out_pressure
runningParams$cz_temperature_diff<-runningParams$out_temperature-runningParams$in_temperature

consumption<-subset(consumption, daily_gas_consumption!=other_gas_consumption)
consumption<-merge(consumption, a, by="date", all.x=FALSE)
consumption<-merge(consumption, runningParams, by="date", all.x=FALSE, all.y=FALSE)

#-----------------------nnet-----------------------
fd<-subset(consumption,
           select=c("daily_electric_consumption", "cz_sum_workload", "cz_pressure_diff", 
                    "cz_pressure_ratio", "cz_temperature_diff"))
wholeData<-data.frame(scale(fd))
calnnet(wholeData, daily_electric_consumption~., fd$daily_electric_consumption, "日用电量预测")

consumption$daily_gas_consumption<-consumption$daily_gas_consumption-consumption$other_gas_consumption
fd<-subset(consumption,
           select=c("daily_gas_consumption", "cz_sum_workload", "cz_pressure_diff", 
                    "cz_pressure_ratio", "cz_temperature_diff"))
wholeData<-data.frame(scale(fd))
calnnet(wholeData, daily_gas_consumption~., fd$daily_gas_consumption, "压缩机总耗气预测")