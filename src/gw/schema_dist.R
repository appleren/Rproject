library(plyr)
library(stringdist)
library(tidyr)
library(dplyr)
library(tibble)
library(stringr)

datapath<-"../../data/gw/gw-rt-data-schm/data"
resultpath<-"../../data/gw/gw-rt-data-schm/result"
tmp<-read.csv(paste0(resultpath, "/sortedResult.csv"))
d<-tmp
d$fid<-substr(d$tid, 1, 6)
d$up<-gsub(pattern="[^A-Za-z0-9,]", replacement="_", toupper(d$colnames))
# length(levels(factor(d$up)))

#===============functions================
getAllcolumn<-function(ltcols) {
  ltcols<-llply(tcols, function(x){
    strsplit(x, ",")
  })
  n<-length(ltcols)
  u<-as.vector(unlist(ltcols[[1]]))
  for(i in 2:n) {
    u<-union(u, as.vector(unlist(ltcols[[i]])))
  }
  u
}

#===================按照schema变化情况分类==============================
#计算如下矩阵：
#  tid, fid, from, to, s1, s2, s3, ...
#  xxx, xxx, xxxx, xx,  1,  1,  0, ...
#

#计算所有列名
tcols<-as.character(d$up)
u<-getAllcolumn(tcols)
u<-levels(factor(u))
up<-levels(factor(d$up))
d$num<-rep(1, times=length(d$up))

countUp<-aggregate(num~up, data=d, sum)
countUp1<-ddply(d, .(up), function(x){
  data.frame(up=x$up[1], num<-length(levels(factor(x$tid))))
})
countUp1<-aggregate(tid~up, data=d, function(x){length(levels(factor(x)))})
colnames(countUp1)<-c("up", "num")

#一共有149个schema
#一共有4171个不同的列名
df<-merge(u, up) %>% mutate(value=str_detect(as.character(y), as.character(x)) %>% as.integer()) %>% spread(x, value)
colnames(df)[1]<-"up"


#列名出现频率排序
dfmc<-merge(countUp1, df, by="up")
# dfmc[,c(-1,-2)]<-dfmc[,c(-1,-2)]*dfmc$num
dfmc<-dfmc[order(-dfmc$num),]
dfmc[,c(-1,-2)]<-dfmc$num*dfmc[,c(-1,-2)]
colCount<-colSums(dfmc[,c(-1,-2)])
highestColnames<-colCount[order(-colCount)]
# cnamehigh<-data.frame(highestColnames[highestColnames>10])
#每个列在不同schema中出现的次数
write.csv(highestColnames, file="colnames.csv")

#shcema频率排名
write.csv(dfmc[,c(2,1)], file="schemas.csv", row.names = FALSE)

#每个tid内schema变化规律
calChange<-function(up1, up2) {
  u1<-unlist(strsplit(up1,","))
  u2<-unlist(strsplit(up2,","))
  u1Notu2<-setdiff(u1, u2)
  u2Notu1<-setdiff(u2, u1)
  if(length(u1Notu2)>0) {
    # rmv<-TRUE
    rmv<-length(u1Notu2)
    rmv_f<-paste(u1Notu2, collapse=";")
  } else {
    # rmv<-FALSE
    rmv<-0
    rmv_f<-""
  }
  if(length(length(u2Notu1)>0)) {
    # ad<-TRUE
    ad<-length(u2Notu1)
    ad_f<-paste(u2Notu1, collapse=";")
  } else {
    # ad<-FALSE
    ad<-0
    ad_f<-""
  }
  data.frame(removed=rmv, added=ad,
             removed_fields=rmv_f, added_fields=ad_f)
}
calUsed<-function(upx, up1) {
  length(intersect(upx, up1))>0
}

dchange<-ddply(d, .(tid), function(x) {
  x<-x[order(x$from),]
  result<-data.frame(tid=x$tid[1], colcount=x$colcount[1], used=FALSE, removed=FALSE, added=FALSE, 
                     removed_fields="", added_fields="",
                     from=x$from[1], to=x$to[1], up=x$up[1])
  if(length(x$tid)==1) {
    return (result)
  } else {
    for(i in 1:(length(x$tid)-1)) {
      result<-rbind(result, cbind(tid=x$tid[1], colcount=x$colcount[i+1], used=calUsed(x$up[1:i], x$up[i+1]), calChange(x$up[i], x$up[i+1]), from=x$from[i+1], to=x$to[i+1], up=x$up[i+1]))
    }
    return (result)
  }
})
write.csv(dchange, file="draft_changes_with_count.csv", row.names = FALSE)
dchanges<-dchange
# write.csv(dchanges, file="draft_changes.csv", row.names = FALSE)
dchange<-read.csv("draft_changes_with_count.csv")
dchange$removed<-as.numeric(dchange$removed)
dchange$added<-as.numeric(dchange$added)
dchange$used<-as.numeric(dchange$used)


#没变过1707
nochange<-ddply(dchange, .(tid), function(x){
  if(length(x$tid)==1) {
    return (x)
  }
})
write.csv(nochange, file="nochange.csv", row.names = FALSE)

changed<-ddply(dchange, .(tid), function(x){
  if(length(x$tid)>1){
    return (x)
  }
})
write.csv(changed, file="changed.csv", row.names = FALSE)


#只增加过列391
addonly<-ddply(dchange, .(tid), function(x){
  if(length(which(x$added>=1))>0 && length(which(x$removed>=1))==0 && length(which(x$used==1))==0) {
    return (x)
  }
})
write.csv(addonly, file="addonly.csv", row.names = FALSE)

#只减少过列0
removeonly<-ddply(dchange, .(tid), function(x){
  if(length(which(x$added==1))>=0 & length(which(x$removed>=1))>0 & length(which(x$used==1))==0) {
    return (x)
  }
})
write.csv(removeonly, file="removeonly.csv", row.names = FALSE)

#同时加又减13479
addAndRemove<-ddply(dchange, .(tid), function(x){
  for(i in 1:length(x$tid)) {
    if(x$removed[i]>=1 & x$added[i]>=1) {
      return (x)
    }
  }
})
write.csv(addAndRemove, file="addAndRemove.csv", row.names = FALSE)

#变回去过
changeback<-ddply(dchange, .(tid), function(x){
  if(length(which(x$used==1))>0) {
    return (x)
  }
})
write.csv(changeback, file="changeback.csv", row.names = FALSE)

#求每种变化的风机和风场数量

count_tid_fid<-function(df, type) {
  tid<-df$tid
  fid<-substr(as.character(tid), 1, 6)
  tcount<-length(levels(factor(tid)))
  fcount<-length(levels(factor(fid)))
  print(paste0("info of ", type, ": tcount=", tcount, "; fcount=", fcount))
}

count_tid_fid(addonly, "addonly")
count_tid_fid(addAndRemove, "addAndRemove")
count_tid_fid(removeonly, "removeonly")
count_tid_fid(changeback, "changeback")

#changeback去重
changeback1<-ddply(changeback, .(tid), function(t) {
  t$eq<-c(1, diff(t$from))
  r<-subset(t, eq!=0)
  if(length(r$tid)>1)
    return (r)
})
  
# dfmc1<-c("sum", colSums(dfmc[,c(-1)]))
# dfmc2<-rbind(dfmc1, dfmc)
# 
# tdfmc2<-t(dfmc2)
# write.csv(tdfmc2, file="wordcount.csv", col.names = FALSE)

# dfmc1<-dfmc[1,c(1,2,order(-dfmc[1, c(-1,-2)]))]

##t
# tdf<-data.frame(t(dfmc))
# cnames<-as.character(tdf[1,])
# tdf<-tdf[-1,]
# tdf<-cbind(row.names(tdf), as.integer(tdf))

#########

tdf<-t(df)

colnames(tdf)<-tdf[1,]
tdf<-data.frame(tdf[-1,])
tdf$wordcount<-rowSums(tdf)
tdf$word<-row.names(tdf)


#=========================计算bad sensor name===================
# otdata<-subset(d, tid==652211072) #以一个风机为例
# otdata<-subset(d, (tid>370504000&tid<370505000)|(tid>652217000&tid<652218000)|(tid>622126000&tid<622127000)
#                |(tid>622126000&tid<622127000)|(tid>320902000&tid<320903000)|(tid>652210000&tid<652211000)
#                |(tid>652211000&tid<652212000)|(tid>652211000&tid<652212000)|(tid>140213000&tid<140214000)
#                |(tid>411201000&tid<411202000)|(tid>622128000&tid<622129000)|(tid>210703000&tid<210704000))
otdata<-tmp
tcols<-as.character(otdata$colnames)
# ltcols<-llply(tcols, function(x){
#   strsplit(x, ",")
# })
# #所有列名
# n<-length(ltcols)
# u<-as.vector(unlist(ltcols[[1]]))
# for(i in 2:n) {
#   u<-union(u, as.vector(unlist(ltcols[[i]])))
# }
u<-getAllcolumn(tcols)

#列名只有大小写或者特殊字符不一样
du<-data.frame(u=u, upU<-gsub(pattern="[^A-Za-z0-9]", replacement="_", toupper(u)), no=rep(1, length(u)))
dur<-aggregate(no~upU, data=du, sum)
durd<-subset(dur, no>1)
dud<-subset(du, upU%in%durd$upU, select=-3)
colnames(dud)<-c("u", "upU")
dud<-dud[order(dud$upU),]
write.csv(dud, file="bad_sensor_name.csv", row.names = FALSE)

#bad sensor name所在的风场/风机
otdata$upU<-gsub(pattern="[^A-Za-z0-9]", replacement="_", toupper(otdata$colnames))
# pat<-paste(levels(factor(dud$upU)), collapse = "&")
# colno<-grepl(pattern=pat, otdata$upU)
# otr<-otdata[which(colno==TRUE),]

#所有bad value都有的风场，不存在
upUl<-levels(factor(dud$upU))
colnor<-grepl(pattern=upUl[1], otdata$upU, fixed = TRUE)
for(i in 2:length(upUl)) {
  colno<-grepl(pattern=upUl[i], otdata$upU, fixed = TRUE)
  colnor<-colnor&colno
}
otr<-otdata[which(colnor==TRUE),]

#具有bad value的风机数据以及bad value=============
ul<-levels(factor(dud$u))
otrall<-data.frame()
#对每个bad alue求其风机记录
for(i in 1:length(ul)){
  colno<-grepl(pattern=ul[i], otdata$colnames, fixed = TRUE)
  otr<-otdata[which(colno==TRUE),]
  otr$badvalue<-ul[i]
  otr$badvalue1<-gsub(pattern="[^A-Za-z0-9]", replacement="_", toupper(ul[i]))
  otrall<-rbind(otrall, otr)
}
#归集到风机-from-to
otrallr<-ddply(otrall, .(tid, from, to), function(d){
  len<-length(d$badvalue)
  data.frame(tid=d$tid[1], from=d$from[1], to=d$to[1], len=len, badvalue=paste(d$badvalue, collapse = ";"), 
             colcount=d$colcount[1], datacount=d$datacount[1], colnames=d$colnames[1])
})
write.csv(otrallr, file="bad_sensor_name_byturbine.csv", row.names = FALSE)
#归集到风场-from-to
otrall$fid<-substr(as.character(otrall$tid), 1, 6)
otrallr1<-ddply(otrall, .(fid, from, to), function(d){
  badv<-levels(factor(d$badvalue))
  len<-length(badv)
  r<-data.frame(fid=d$fid[1], from=d$from[1], to=d$to[1], len=len, badvalue=paste(badv, collapse = ";"), 
             tids=paste(d$tid, collapse = ";"),colcount=d$colcount[1], datacount=d$datacount[1], colnames=d$colnames[1])
  r
})
write.csv(otrallr1, file="bad_sensor_name_byfarm.csv", row.names = FALSE)
#otrall<-otrall[order(otrall$tid, otrall$from, otrall$badvalue1),c("badvalue1", "badvalue", "tid", "from", "to", "colcount", "datacount", "colnames")]


#================================根据协议解决版本问题====================================
# compath and ascflg

propath<-"../../data/gw/gw-rt-data-schm/schemas/goldwind"
propaths1<-read.csv(paste0(propath, "/propaths1.csv"))
propaths2<-read.csv(paste0(propath, "/propaths2.csv"))
propaths2<-subset(propaths2, !protocolid %in% c("",")", "Mvar", ") 初值", ") 末值"))
propaths2$protocolid<-as.integer(as.character(propaths2$protocolid))
colnames<-read.csv(paste0(propath, "/colname.csv"), header = FALSE)
colnames(propaths2)[2]<-"datapath"
propaths1<-subset(propaths1, select=colnames$V1)
propaths2<-subset(propaths2, select=colnames$V1)

allSchema<-colnames(dfmc)[c(-1,-2)]
allpathSchema<-union(propaths1$datapath, propaths2$datapath)
allpathSchema1<-gsub(pattern="[^A-Za-z0-9,]", replacement="_", toupper(allpathSchema))
allpathSchema1<-levels(as.factor(allpathSchema1))
diffPath<-setdiff(allSchema, allpathSchema1)

hc<-read.csv("colnames.csv")
hc1<-subset(hc, X %in% diffPath)
