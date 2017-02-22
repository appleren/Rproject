library(plyr)

cleandata<-function(rollSpeed) {
  rollSpeed$date<-as.Date(rollSpeed$time)
  rollSpeed$time<-as.POSIXct(rollSpeed$time)
  rollSpeed<-ddply(rollSpeed, "date", function(d){
    d[order(d$time),]
  })
  rollSpeed
}

getSlices<-function(rollSpeed, speed=1000) {
  rollSpeed$a<-rep(0, length(rollSpeed$value))
  rollSpeed$a[which(rollSpeed$value>speed)]<-1
  rollSpeed$b<-c(1, diff(rollSpeed$a))
  rollSpeed$b<-as.integer(rollSpeed$b>0)
  rollSpeed$c<-cumsum(rollSpeed$b)
  rollSpeed$pid<-rollSpeed$a*rollSpeed$c
  rollSpeed<-subset(rollSpeed, pid>0)
  if(rollSpeed$pid[1]==2){
    rollSpeed$pid<-rollSpeed$pid-1
  }
  
  result<-ddply(rollSpeed, "pid", function(d){
    m<-c(tag=as.character(d$tag[1]), 
         endTime=as.character(max(d$time)), 
         startTime=as.character(min(d$time)),
         minValue=as.numeric(max(d$value)),
         maxValue=as.numeric(max(d$value)),
         length=length(d$time), pid=d$pid[1])
    m
  })
  result
}

formatDate<-function(dateString){
  as.POSIXct(dateString)
}

mainfunction<-function(inputFile, outputFile) {
  rollSpeed<-read.csv(inputFile, header=TRUE)
  rollSpeed<-subset(rollSpeed, status==0)
  rollSpeed<-cleandata(rollSpeed)
  rollSpeed$time<-formatDate(rollSpeed$time)
  rollSpeed$date<-as.Date(rollSpeed$date)
  r1000<-getSlices(rollSpeed, 1000)
  write.csv(r1000, outputFile, row.names=FALSE)
}