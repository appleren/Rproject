library(reshape2)
library(plyr)
library(plotrix)

datapath<-"../../data/cnpc/压缩机转速/"

formatDate<-function(dateString){
  as.POSIXct(dateString)
}
monthDays<-function(time){
  time <- as.POSIXlt(time)
  time$mday[] <- time$sec[] <- time$min <- time$hour <- 0
  time$mon <- time$mon + 1
  return(as.POSIXlt(as.POSIXct(time))$mday)
}

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
  result<-data.frame(tag=character(0),
                     startTime=character(0),
                     endTime=character(0),
                     date=character(0),
                     last=numeric(0),
                     length=integer(0))
  if(length(rollSpeed$pid)>0){
    if(rollSpeed$pid[1]==2){
      rollSpeed$pid<-rollSpeed$pid-1
    }
    result<-ddply(rollSpeed, "pid", function(d){
      m<-c(tag=as.character(d$tag[1]), 
           startTime=as.character(min(d$time)),
           endTime=as.character(max(d$time)), 
           date=as.character(d$date[1]),
           last=difftime(max(d$time), min(d$time), units=c("hours")),
           length=length(d$time), pid=d$pid[1])
      m
    })
  }
  result
}

plotGantt<-function(dfList, startDate, endDate, startTimeCol, endTimeCol, labelCol, gridNo=3, datapath) {
  for(filename in dfList) {
    df<-read.csv(paste0(datapath, "result/", filename), header=TRUE)
    df[[startTimeCol]]<-formatDate(df[[startTimeCol]])
    df[[endTimeCol]]<-formatDate(df[[endTimeCol]])
    df<-subset(df, df[[endTimeCol]]>startDate&df[[startTimeCol]]<endDate)
    df[[startTimeCol]][which(df[[startTimeCol]]<startDate)]<-rep(formatDate(startDate), length(which(df[[startTimeCol]]<formatDate(startDate))))
    df[[endTimeCol]][which(df[[endTimeCol]]>formatDate(endDate))]<-rep(formatDate(endDate), length(which(df[[endTimeCol]]>formatDate(endDate))))
    
    if(!exists("plotLabel")){
      plotLabel<-as.character(df[[labelCol]])
    } else {
      plotLabel<-c(plotLabel, as.character(df[[labelCol]]))
    }
    if(!exists("starts")){
      starts<-df[[startTimeCol]]
    } else {
      starts<-c(starts, df[[startTimeCol]])
    }
    if(!exists("ends")){
      ends<-df[[endTimeCol]]
    } else {
      ends<-c(ends, df[[endTimeCol]])
    }
    if(!exists("priorities")){
      priorities=rep(1, length(df[[labelCol]]))
    } else {
      priorities<-c(priorities, rep(1, length(df[[labelCol]])))
    }
  }
  
  gantt.info<-list(labels=plotLabel, 
                   starts=starts,
                   ends=ends,
                   priorities=priorities)
  
  diffDate<-as.integer(as.Date(endDate)-1-as.Date(startDate))%/%gridNo
  Ymd.format<-"%Y-%m-%d"
  vgridpos<-formatDate(strptime(as.character(as.Date(startDate)+c(0:gridNo)*diffDate), format=Ymd.format))
#   vgridpos<-c(vgridpos, formatDate(as.character(as.Date(endDate)-1)))
  vgridlab<-format(vgridpos, format = Ymd.format)
  
  mycolor <- rainbow(2, alpha=0.2)
  gantt.chart(gantt.info,
              priority.legend=TRUE,vgridpos=vgridpos,vgridlab=vgridlab,
              hgrid=TRUE, taskcolors=mycolor, label.cex=0.6)
}

#============================主调过程====================
# datapath<-"../../data/cnpc/speed/压缩机转速/"
# 
#=====================pre====================
# filenames<-list.files(path=datapath, pattern=".csv")
# 
# #clean data
# for(filename in filenames) {
#   rollSpeed<-read.csv(paste0(datapath, filename), header=TRUE)
#   rollSpeed<-subset(rollSpeed, status==0)
#   
#   rollSpeed<-cleandata(rollSpeed)
#   write.csv(rollSpeed, paste0(datapath, "mid/", "s1_", filename), row.names=FALSE)
# }
# 
# #===slice on whole data===============
# filenames<-list.files(path=paste0(datapath, "mid"), pattern=".csv")
# for(filename in filenames) {
#   rollSpeed<-read.csv(paste0(datapath, "mid/", filename), header=TRUE)
#   rollSpeed$time<-formatDate(rollSpeed$time)
#   rollSpeed$date<-as.Date(rollSpeed$date)
#   r1000<-getSlices(rollSpeed, 1000)
#   write.csv(r1000, paste0(datapath, "result/", "slice_", filename), row.names=FALSE)
# }

mainFunction<-function(datapath, index) {
  filenames<-list.files(path=datapath, pattern=".csv")
  
  #clean data
  for(filename in filenames) {
    rollSpeed<-read.csv(paste0(datapath, filename), header=TRUE)
    rollSpeed<-subset(rollSpeed, status==0)
    
    rollSpeed<-cleandata(rollSpeed)
    write.csv(rollSpeed, paste0(datapath, "mid/", "s1_", index, filename), row.names=FALSE)
  }
  
  #sum data
  filenames<-list.files(path=paste0(datapath, "mid"), pattern=".csv")
  for(filename in filenames) {
    rollSpeed<-read.csv(paste0(datapath, "mid/", filename), header=TRUE)
    rollSpeed$time<-formatDate(rollSpeed$time)
    rollSpeed$date<-as.Date(rollSpeed$date)
    r1000<-getSlices(rollSpeed, 1000)
    write.csv(r1000, paste0(datapath, "result/", "slice_", index, filename), row.names=FALSE)
  }
}

#plot
# filenames<-list.files(path=paste0(datapath, "result"), pattern=".csv")
# 
# startDate<-"2015/10/01"
# endDate<-"2015/11/01"
# startTimeCol<-"startTime"
# endTimeCol<-"endTime"
# labelCol<-"tag"
# rm(plotLabel, starts, ends, priorities)
# 
# plotGantt(filenames, startDate, endDate, startTimeCol, endTimeCol, labelCol, gridNo=10)
# #================plotGantt=============================
filenames<-list.files(path=paste0(datapath, "result"), pattern=".csv")

startDate<-"2014/10/1"
endDate<-"2014/11/2"
startTimeCol<-"startTime"
endTimeCol<-"endTime"
labelCol<-"tag"
rm(plotLabel, starts, ends, priorities)
plotGantt(filenames, startDate, endDate, startTimeCol, endTimeCol, labelCol, gridNo=10, datapath)

#==========slice by day===============
filenames<-list.files(path=paste0(datapath, "mid"), pattern=".csv")
# filename<-filenames[1]
for(filename in filenames) {
  rollSpeed<-read.csv(paste0(datapath, "mid/", filename), header=TRUE, as.is=TRUE)
  rollSpeed$time<-formatDate(rollSpeed$time)
  r<-ddply(rollSpeed, .(date), function(d) {
    r1000<-getSlices(d, 1000)
      return (r1000)
  })
  r$date<-as.POSIXlt(r$date)
  r$year<-r$date$year+1900
  r$month<-r$date$mon+1
  r$day<-r$date$mday
  write.csv(r, paste0(datapath, "result/day/", "dayslice_", filename), row.names=FALSE)
}

#=====表：月开工率、开工小时数、每天开机小时数线图===========
y<-2014
mon<-11
ym<-as.yearmon(paste(y, mon, sep="-"))
filenames<-list.files(path=paste0(datapath, "result/day"), pattern=".csv")
filename<-filenames[1]
r<-data.frame()
for(filename in filenames) {
  slide<-read.csv(paste0(datapath, "result/day/", filename), header=TRUE, as.is=TRUE)
  slide$last[slide$last>23.5]<-24
  ss<-subset(slide, year==y&month==mon)
  sumLast<-sum(ss$last)
  sumLastR<-sumLast/(monthDays(ym)*24)
  m<-data.frame(tag=ss$tag[1],year=ss$year[1], month=ss$month[1], sumLast=sumLast, sumLastR=sumLastR)
  if(length(r)==0){r<-m}
  else{r<-rbind(r, m)}
}
write.csv(r, paste0(datapath, "result/sum/", "sum.csv"), row.names=FALSE)

#=====选择几台机器，每天转速变化曲线============

startDate<-formatDate("2014/10/01")
endDate<-formatDate("2014/11/01")

filenames<-list.files(path=paste0(datapath, "mid"), pattern=".csv")
# filename<-filenames[2]
r<-data.frame(date=character(0), value=numeric(0))
for(filename in filenames) {
  rollSpeed<-read.csv(paste0(datapath, "mid/", filename), header=TRUE, as.is=TRUE)
  tag<-rollSpeed$tag[1]
  rs<-aggregate(value~date, data=rollSpeed, mean)
  rs$date<-formatDate(rs$date)
  rs<-subset(rs, date>=startDate&date<endDate)
  colnames(rs)<-c("date", tag)
  r<-merge(r, rs, by="date", all=TRUE)
}

mycolor <- rainbow(6)
tt<-xts(x=subset(r, select=c(3:8)), order.by=r$date) 
l<-colnames(r)[3:8]
plot(as.zoo(tt), screens=1, col=mycolor,ylab="转速", xlab="时间")
# legend("topleft", inset=.05, legend=l, pch=1, col=mycolor, horiz=FALSE)

#=====选择一个月，几台机器，每天开工小时数===========
y<-2014
mon<-11

filenames<-list.files(path=paste0(datapath, "result"), pattern=".csv")
filename<-filenames[1]
r<-data.frame(date=character(0), value=numeric(0))
for(filename in filenames) {
  slide<-read.csv(paste0(datapath, "result/", filename), header=TRUE, as.is=TRUE)
  slide$last[slide$last>23.5]<-24
  tag<-slide$tag[1]
  ss<-subset(slide, year==y&month==mon,select=c("date", "last"))
  ss$date<-as.Date(ss$date)
  if(length(ss[,1]>0)){ss<-aggregate(last~date, data=ss, sum)}
  colnames(ss)<-c("date", tag)
  r<-merge(r, ss, by="date", all=TRUE)
}
r[is.na(r)]<-0

mycolor <- rainbow(6)
tt<-xts(x=subset(r, select=c(3:8)), order.by=r$date) 
l<-colnames(r)[3:8]
plot(as.zoo(tt), screens=1, col=mycolor,ylab="工作时间", xlab="时间")
# legend("topleft", inset=.05, legend=l, pch=1, col=mycolor, horiz=FALSE)
