library("plyr")
library(digest)
datapath<-"../../data/gw/gw-rt-data-schm/data"
resultpath<-"../../data/gw/gw-rt-data-schm/result"
folders<-list.dirs(path=datapath)

finalresult<-data.frame()
for(i in 1:length(folders)) {
  filenames<-list.files(path=folders[i], pattern=".txt", full.names=TRUE)
  if(length(filenames)>0) {
    result<-data.frame()
    for(j in 1:length(filenames)) {
      data<-read.table(filenames[j], sep=";")
      colnames(data)<-c("id", "date", "tid", "datacount", "colcount", "colnames")
      # data$up<-factor(gsub(pattern="[^A-Za-z0-9,]", replacement="_", toupper(data$colnames)))
      data$c<-c(1,diff(data$id))
      data$c[which(data$c!=0)]<-1
      data$c<-cumsum(data$c)
      r<-ddply(data, .(tid,c), function(d) {
        data.frame(tid=d$tid[1], id=d$id[1], from=min(d$date), to=max(d$date), datacount=sum(d$datacount), colcount=d$colcount[1], colnames=d$colnames[1])
      })
      result<-rbind(result, r)
    }
  }
  if(exists("result")) {
    finalresult<-rbind(finalresult, result)
  }
}

finalresult<-finalresult[,-1]
write.csv(finalresult, paste0(resultpath, "/result.csv"), row.names = FALSE)

# sorted<-result[ order(result$tid, result$from), ][, c("tid","from","to","datacount","colcount","id","colnames")]

#merge in case that records with same tid and same id are in different lines -- no such result in our case
sorted_result<-ddply(finalresult, .(tid), function(d) {
  if(length(d$tid)==1) {
    return (d)
  } else {
    colname<-colnames(d)
    d<-d[order(d$from),]
    d$diff<-c(1, diff(d$id))
    d$diff[which(d$diff!=0)]<-1
    d$diff<-cumsum(d$diff)
    r<-ddply(d, .(diff), function(m) {
      data.frame(tid=m$tid[1], from=min(m$from), to=max(m$to), datacount=sum(m$datacount), colcount=m$colcount, id=m$id[1], colnames=m$colnames[1])
    })
    return (r[,-1])
  }
})

write.csv(sorted_result, paste0(resultpath, "/sortedResult.csv"), row.names=FALSE)

datestat<-ddply(sorted_result, .(tid), function(d) {
  data.frame(tid=d$tid[1], from=min(d$from), to=max(d$to), schemacount=length(levels(factor(d$from))))
})

write.csv(datestat, paste0(resultpath, "/TidFromTo.csv"), row.names=FALSE)

muti_schema_t<-subset(sorted_result, tid %in% datestat$tid[which(datestat$schemacount.freq>1)])

write.csv(muti_schema_t, paste0(resultpath, "/muti_schema_t.csv"), row.names = FALSE)

################################################################################
one_schema_t<-subset(sorted_result, tid %in% datestat$tid[which(datestat$schemacount.freq==1)])
write.csv(one_schema_t, paste0(resultpath, "/one_schema_t.csv"), row.names = FALSE)

tmp<-read.csv(paste0(resultpath, "/sortedResult.csv"))
tmp1<-subset(tmp, (tid>370504000&tid<370505000)|(tid>652217000&tid<652218000)|(tid>622126000&tid<622127000)
            |(tid>622126000&tid<622127000)|(tid>320902000&tid<320903000)|(tid>652210000&tid<652211000)
            |(tid>652211000&tid<652212000)|(tid>652211000&tid<652212000)|(tid>140213000&tid<140214000)
            |(tid>411201000&tid<411202000)|(tid>622128000&tid<622129000)|(tid>210703000&tid<210704000))
write.table(tmp1, paste0(resultpath, "/sortedResult_new1.csv"), row.names=FALSE, sep=";", quote=FALSE)

tmp2<-subset(tmp, (tid>140804000&tid<140805000))
tmp2$fid<-substr(tmp2$tid, 1, 6)
write.table(tmp2, paste0(resultpath, "/sortedResult_new1.csv"), row.names=FALSE, sep=";", quote=FALSE)



tmp3<-subset(tmp, tid==652211072)
write.table(tmp3, paste0(resultpath, "/sortedResult_new1.csv"), row.names=FALSE, sep=";", quote=FALSE)
# write.table(sorted_result, paste0(resultpath, "/sortedResult_new.csv"), row.names=FALSE, sep=";", quote=FALSE)
