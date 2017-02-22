library(XLConnect)
library(reshape2)
library(zoo)
library(plyr)


datapath<-"../../data/cnpc/qihao/"
filenames<-list.files(path=paste(datapath, "cz1/", sep=""), pattern=".xls")

transProvide<-data.frame()
runningParams<-data.frame()
compressorParam<-data.frame()
consumption<-data.frame()
emptying<-data.frame()

for(filename in filenames) {
  d<-as.Date(ISOdate(substr(filename, 1, 4), substr(filename, 5,6), substr(filename, 7,8)))
  
  xls <- loadWorkbook(paste(datapath, "cz1/", filename, sep=""),create=FALSE)
  
  #ת���� transProvide
  s<-readWorksheet(xls, "Sheet1", region="e3:g3", header=FALSE)
  colnames(s)<-c("trans_ressure", "trans_temperature", "daily_completion")
  s$date<-d
  transProvide<-rbind(transProvide, s)
  
  #���в��� runningParams
  s<-readWorksheet(xls, "Sheet1", region="b6:c11", header=FALSE)
  colnames(s)<-c("para_name", "value")
  s[,1]<-c("in_pressure", "in_temperature", "out_pressure", "out_temperature", "earth_temperature", "env_temperature")
  s<-dcast(s, .~para_name)
  s<-subset(s, select=-1)
  s$date<-d
  runningParams<-rbind(runningParams, s)
  
  #ѹ�������в��� compressorParam
  s<-readWorksheet(xls, "Sheet1", region="b14:c20", header=FALSE)
  colnames(s)<-c("para_name", "value")
  s[,1]<-c("working_time", "speed", "com_in_pressure", "com_in_temperature", "com_out_pressure", "com_out_temperature", "com_gas_consumption")
  s<-dcast(s, .~para_name)
  s<-subset(s, select=-1)
  s$date<-d
  s$com_no<-1
  compressorParam<-rbind(compressorParam, s)
  
  s<-readWorksheet(xls, "Sheet1", region="b22:c28", header=FALSE)
  colnames(s)<-c("para_name", "value")
  s[,1]<-c("working_time", "speed", "com_in_pressure", "com_in_temperature", "com_out_pressure", "com_out_temperature", "com_gas_consumption")
  s<-dcast(s, .~para_name)
  s<-subset(s, select=-1)
  s$date<-d
  s$com_no<-2
  compressorParam<-rbind(compressorParam, s)
  
  #��վ�ܺ� consumption
  s<-readWorksheet(xls, "Sheet1", region="b31:c33", header=FALSE)
  colnames(s)<-c("para_name", "value")
  s[,1]<-c("daily_gas_consumption", "daily_electric_consumption", "other_gas_consumption")
  s<-dcast(s, .~para_name)
  s<-subset(s, select=-1)
  s$date<-d
  consumption<-rbind(consumption, s)
  
  #�ſ� emptying
  s<-readWorksheet(xls, "Sheet1", region="b36:c36", header=FALSE)
  colnames(s)<-c("para_name", "value")
  s[,1]<-c("emptying")
  s<-dcast(s, .~para_name)
  s<-subset(s, select=-1)
  s$date<-d
  emptying<-rbind(emptying, s)
}

#ת���� transProvide
#���в��� runningParams
#ѹ�������в��� compressorParam
#��վ�ܺ� consumption
#�ſ� emptying
write.csv(transProvide, paste(datapath, "cz1_mid/ת����.csv", sep=""), row.names=FALSE)
write.csv(runningParams, paste(datapath, "cz1_mid/���в���.csv", sep=""), row.names=FALSE)
write.csv(compressorParam, paste(datapath, "cz1_mid/ѹ�������в���.csv", sep=""), row.names=FALSE)
write.csv(consumption, paste(datapath, "cz1_mid/��վ�ܺ�.csv", sep=""), row.names=FALSE)
write.csv(emptying, paste(datapath, "cz1_mid/�ſ�.csv", sep=""), row.names=FALSE)

#======================================================

