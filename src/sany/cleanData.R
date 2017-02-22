#import
library(plyr)
library(reshape2)
source("setCurrentProject.R")
#----------function to generate year/month/decad------
getYMD<-function(dt) {
  p<-as.POSIXlt(dt)
  n<-length(p)
  r<-data.frame(year=numeric(n), month=numeric(n), day=numeric(n), decad=numeric(n))
  for(i in 1:length(p)) {
    r$year[i]<-p[i]$year+1900
    r$month[i]<-p[i]$mon+1
    r$day[i]<-p[i]$mday
    r$decad[i]<-min(3, ceiling(p[i]$mday/10))
  }
  r
}

#---------------2012 to 2015 with 2011--------
setCurrentProject("SANY")
custFilter<-read.csv("../../data/sany/0708/cust_filter_0708.csv", header=TRUE)
whSubCom<-read.csv("../../data/sany/0708/wh_subCom_0708.csv", header=TRUE)
partGroup<-read.csv("../../data/sany/0708/part_group_0708.csv", header=TRUE)
sales<-read.csv("../../data/sany/0708/sales_0709.csv", header=TRUE)

#filter sales
subSales<-subset(sales, !(cust_name %in% custFilter$subCom))
subSales<-subset(subSales, !(cust_name %in% custFilter$name_wh))
subSales<-subset(subSales, !(cust_name %in% custFilter$name_31))
subSales<-subset(subSales, !(wh_name %in% custFilter$vendor_list))
subSales<-subset(subSales, !(cust_name %in% custFilter$cust_filter4))


for(whName in whSubCom$wh_name){
  subSales$province[subSales$wh_name==whName]<-as.character(whSubCom$Province[whSubCom$wh_name==whName])
}
#-----------------------2011 data-------------
oldSales<-read.csv("../../data/sany/0708/xiaoshoudingdan.csv", header=TRUE)
colnames(oldSales)<-c("ddlx","subCom","cust_name","part_no","order_qty", "closeDate", 
                      "openDate", "year", "month", "decad", "a", "b")

oldSales<-subset(oldSales, !as.character(cust_name) %in% as.character(custFilter$subCom))
oldSales<-subset(oldSales,  subCom!="经营计划总部")
oldSales<-subset(oldSales,  year==2011)
oldSales<-subset(oldSales,  !part_no %in% c("A820403000033", "A810301032047"))
oldSales$province<-substr(oldSales$subCom, 1, 2)
oldSales<-subset(oldSales,  province!="泵送")

#------------------Union 2011 to 2015-----------
oldSales<-subset(oldSales, select=c("part_no", "province", "year", "month", "decad", "order_qty"))
subSales<-subset(subSales, select=c("part_no", "province", "year", "month", "decad", "order_qty"))
allSales<-rbind(oldSales, subSales)

for(partNo in partGroup$part_no) {
  allSales$part_name[allSales$part_no==partNo]<-as.character(partGroup$part_name[partGroup$part_no==partNo])
  allSales$part_groupName[allSales$part_no==partNo]<-as.character(partGroup$note[partGroup$part_no==partNo])
  allSales$part_group[allSales$part_no==partNo]<-as.character(partGroup$group[partGroup$part_no==partNo])
}

sumSale<-aggregate(order_qty~part_no+decad+month+year+province, data=allSales, sum)

for(partNo in partGroup$part_no) {
  sumSale$part_name[sumSale$part_no==partNo]<-as.character(partGroup$part_name[partGroup$part_no==partNo])
  sumSale$part_groupName[sumSale$part_no==partNo]<-as.character(partGroup$note[partGroup$part_no==partNo])
  sumSale$part_group[sumSale$part_no==partNo]<-as.character(partGroup$group[partGroup$part_no==partNo])
}
#write.csv(sumSale, "../../data/sany/mid/sumSales.csv")
#--remove data before 2011-9 and after 2015-6
sumSale<-subset(sumSale, !(year==2011 & month<9))
sumSale<-subset(sumSale, !(year==2015 & month>6))
#---group
sumGroupSale<-aggregate(order_qty~part_group+decad+month+year+province, data=sumSale, sum)
for(group in partGroup$group) {
  sumGroupSale$part_groupName[sumGroupSale$part_group==group]<-as.character(partGroup$note[partGroup$group==group][1])
}
#write.csv(sumGroupSale, "../../data/sany/mid/sumGroupSale.csv")
#--------------generate seq---------------------
dcount<-46*3
seq<-seq(from=1, to=dcount, by=1)
year<-c(rep(2011, times=4*3), rep(2012, times=12*3), rep(2013, times=12*3), 
            rep(2014, times=12*3), rep(2015, times=6*3))
month<-seq(from=1, to=dcount, by=1)
month[1:12]<-(month[1:12]-1)%/%3+9
month[13:(13+12*3-1)]<-(month[13:(13+12*3-1)]-13)%/%3+1
month[49:(49+12*3-1)]<-(month[49:(49+12*3-1)]-49)%/%3+1
month[85:(85+12*3-1)]<-(month[85:(85+12*3-1)]-85)%/%3+1
month[121:length(month)]<-(month[121:length(month)]-121)%/%3+1
d<-c(1,2,3)
decad<-rep(d, times=46)
seq<-cbind(seq, decad, month, year)
#-----------
#seqSales<-ddply(sumSale, .(province,part_no), function(d){
#  partSale<-merge(seq, d, by=c("year", "month", "decad"), all.x=TRUE)
#  partSale$part_no[is.na(partSale$part_no)]<-d$part_no[1]
#  partSale$province[is.na(partSale$province)]<-d$province[1]
#  partSale$part_name[is.na(partSale$part_name)]<-d$part_name[1]
#  partSale$part_groupName[is.na(partSale$part_groupName)]<-d$part_groupName[1]
#  partSale$part_group[is.na(partSale$part_group)]<-d$part_group[1]
#  partSale$order_qty[is.na(partSale$order_qty)]<-0
#  partSale
#})
#seq<-1:length(seqSales[,1])
#seqSales<-cbind(seq, seqSales)
#write.csv(seqSales, "../../data/sany/mid/seqSales.csv")

#--------group------------
seqGroupSale<-ddply(sumGroupS ale, .(part_group, province), function(d){
  partSale<-merge(seq, d, by=c("year", "month", "decad"), all.x=TRUE)
  partSale$province[is.na(partSale$province)]<-d$province[1]
  partSale$part_groupName[is.na(partSale$part_groupName)]<-d$part_groupName[1]
  partSale$part_group[is.na(partSale$part_group)]<-d$part_group[1]
  partSale$order_qty[is.na(partSale$order_qty)]<-0
  partSale
})
seqGroupSale$date_YMD<-as.Date(ISOdate(seqGroupSale$year, seqGroupSale$month, seqGroupSale$decad*8))
write.csv(seqGroupSale, "../../data/sany/mid/seqGroupSale.csv")
