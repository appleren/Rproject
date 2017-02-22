library(reshape2)
library(zoo)
library(plyr)

#=======functions===============
formatDate<-function(dateString){
  as.POSIXct(dateString)
}

#===============code==============

datapath<-"../../data/PetroChina/"

filePath<-paste0(datapath, "pipe pressure & temperature/")

schema<-read.csv(paste0(filePath, "schema.csv"), header=TRUE, as.is=TRUE)

filenames<-list.files(path=paste0(filePath, "scada1/"), pattern=".csv")
# filename<-filenames[1]

# schema<-subset(schema, LINE=="西气东输二线西段干线"&STATIONNAME=="武威分输站")
# d<-schema

ddply(schema, .(LINE, STATIONNAME), function(d){
  scadas<-data.frame()
  line<-d$LINE[1]
  station<-d$STATIONNAME[1]
  print(paste("get a new d, ", line, station, dim(d)))
  for(i in 1:length(d$TYPE)) {
    filename<-paste0(filePath, "scada1/", d$SCADAID[i], ".csv")
    type<-as.character(d$TYPE[i])
    scada<-data.frame()
    try(scada<-read.csv(filename, header=TRUE, as.is=TRUE), TRUE)
    print(paste0("scada read ", line, "_", station, ", ", dim(scada)))
    if(length(scada)>0){
      print(paste0("scada is not null, ", dim(scada)))
      scada$time<-formatDate(scada$time)
      scada<-subset(scada, status==0, select=c("time", "value"))
      colnames(scada)<-c("time", type)
      if(length(scadas)==0) {
        scadas<-scada
      } else {
        scadas<-merge(scadas, scada, by="time", all=TRUE)
      }
    }
  }
  print(dim(scadas))
  scadas$line<-rep(line, length(scadas$time))
  scadas$station<-rep(station, length(scadas$time))
  try(write.csv(scadas, paste0(datapath, "mid/", line, "_", station, ".csv"), row.names=FALSE), TRUE)
  print("done one")
})



#===========find bad data========
s<-subset(schema, as.character(SCADAID)%in%
c("XDE00C015PT1103","XDE00C028PT1101","XDE00C076PT1103","XDE00D007PT1201",
"XDE00C015TT1001","XDE00C022TT5301","XDE00C028TT1201","XDE00C041TT1101",
"XDE00C076TT1101","XDE14C002TT1101","XDE08C016TT1201", "XDE08C016PT1201",
"XDE08C011TT1101"))
write.csv(s, paste0(datapath, "mid/", "repeat_scada", ".csv"), row.names=FALSE)
