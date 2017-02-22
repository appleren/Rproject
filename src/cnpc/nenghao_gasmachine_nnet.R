
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
#ת���� transProvide
#���в��� runningParams
#ѹ�������в��� compressorParam
#��վ�ܺ� consumption
#�ſ� emptying

# transProvide<-read.csv(paste(datapath, "cz1_mid/ת����.csv", sep=""), header=TRUE)
# runningParams<-read.csv(paste(datapath, "cz1_mid/���в���.csv", sep=""), header=TRUE)
# consumption<-read.csv(paste(datapath, "cz1_mid/��վ�ܺ�.csv", sep=""), header=TRUE)
# emptying<-read.csv(paste(datapath, "cz1_mid/�ſ�.csv", sep=""), header=TRUE)

# > colnames(compressorParam)
# [1] "com_gas_consumption" "com_in_pressure"     "com_in_temperature"  "com_out_pressure"   
# [5] "com_out_temperature" "speed"               "working_time"        "date"               
# [9] "com_no"  
#
#��ѹ�����ܺ�
#Ԥ��Ŀ�꣺
#��λʱ�������������/����ʱ��  com_gas_csmt_phour
#
#������
#����ǿ�ȣ�ת�٣���ת��         speed
#ѹ�����ѹ��-����ѹ��        com_pressure_diff
#ѹ�ȣ�����ѹ��/����ѹ��        com_pressure_ratio
#�²�����¶�-�����¶�        com_temperature_diff


compressorParam<-read.csv(paste(datapath, "cz1_mid/ѹ�������в���.csv", sep=""), header=TRUE)

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
calnnet(wholeData, com_gas_csmt_phour~., fd$com_gas_csmt_phour, "ѹ����#1����Ԥ��")
fd<-subset(compressorParam, 
           com_no==2, 
           select=c("com_gas_csmt_phour", "speed", "com_pressure_diff", 
                    "com_pressure_ratio", "com_temperature_diff"))
wholeData<-data.frame(scale(fd))
calnnet(wholeData, com_gas_csmt_phour~., fd$com_gas_csmt_phour, "ѹ����#2����Ԥ��")
fd<-subset(compressorParam,
           select=c("com_gas_csmt_phour", "speed", "com_pressure_diff", 
                    "com_pressure_ratio", "com_temperature_diff"))
wholeData<-data.frame(scale(fd))
calnnet(wholeData, com_gas_csmt_phour~., fd$com_gas_csmt_phour, "ȼ��ѹ��������Ԥ��")

#���ܺ�
#Ԥ��Ŀ�꣺
#�ܺĵ磺�ܺĵ� daily_electric_consumption
#�ܺ��������ܺ��� daily_gas_consumption
#
#������
#��������sum(����ʱ��*ת��)     cz_sum_workload    //�ܷ���������
#ѹ���վѹ��-��վѹ��        cz_pressure_diff
#ѹ�ȣ���վѹ��/��վѹ��        cz_pressure_ratio
#�²��վ�¶�-��վ�¶�        cz_temperature_diff

# > colnames(consumption)
# [1] "daily_electric_consumption" "daily_gas_consumption"      "other_gas_consumption"      "date" 

compressorParam<-read.csv(paste(datapath, "cz1_mid/ѹ�������в���.csv", sep=""), header=TRUE)
consumption<-read.csv(paste(datapath, "cz1_mid/��վ�ܺ�.csv", sep=""), header=TRUE)
runningParams<-read.csv(paste(datapath, "cz1_mid/���в���.csv", sep=""), header=TRUE)

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
calnnet(wholeData, daily_electric_consumption~., fd$daily_electric_consumption, "���õ���Ԥ��")

consumption$daily_gas_consumption<-consumption$daily_gas_consumption-consumption$other_gas_consumption
fd<-subset(consumption,
           select=c("daily_gas_consumption", "cz_sum_workload", "cz_pressure_diff", 
                    "cz_pressure_ratio", "cz_temperature_diff"))
wholeData<-data.frame(scale(fd))
calnnet(wholeData, daily_gas_consumption~., fd$daily_gas_consumption, "ѹ�����ܺ���Ԥ��")