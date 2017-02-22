library(stringdist)
library(tidyr)
library(dplyr)
library(tibble)
library(stringr)



datapath<-"../../data/gw/import/gw"
resultpath<-"../../data/gw/gaoshan/result/fenxi"
resultpath1<-"../../data/gw/gaoshan/result"

#================工具函数===========================
#分开列名和data type
# col<-as.character(tmp_type$colnames)[1]
splitCol<-function(col, splitor1=",", splitor2=" ") {
  name_types<-unlist(str_split(col, splitor1))
  test<-unlist(llply(name_types, str_split, " "))
  # test<-llply(name_types, str_split, " ")
  if(length(test)%%2==0) {
    t<-length(test)-1
  } else {
    t<-length(test)
  }
  names<-test[seq(from=1, to=t, by=2)]
  data_types<-test[seq(from=2, to=(length(test)%/%2)*2, by=2)]
  data.frame(names=to_fieldid(names), data_types=tolower(data_types))
}

to_fieldid<-function(name) {
  l<-grep("[0123456789]", substr(name, 1, 1))
  if(length(l)==0 || l!=1) {
    gsub(pattern="[^A-Za-z0-9,]", replacement="_", tolower(as.character(name)))
  } else {
    gsub(pattern="[^A-Za-z0-9,]", replacement="_", tolower(as.character(paste0("d_", name))))
  }
}

#============================预处理原始数据==============================
# folders<-list.dirs(path=datapath)
# finalresult<-data.frame()
# for(i in 1:length(folders)) {
#   filenames<-list.files(path=folders[i], pattern=".schm.txt", full.names=TRUE)
#   if(length(filenames)>0) {
#     result<-data.frame()
#     for(j in 1:length(filenames)) {
#       data<-read.table(filenames[j], sep=";")
#       colnames(data)<-c("id", "date", "tid", "datacount", "colcount", "colnames")
#       data$c<-c(1,diff(data$id))
#       data$c[which(data$c!=0)]<-1
#       data$c<-cumsum(data$c)
#       r<-ddply(data, .(tid,c), function(d) {
#         data.frame(tid=d$tid[1], id=d$id[1], from=min(d$date), to=max(d$date), datacount=sum(d$datacount), colcount=d$colcount[1], colnames=d$colnames[1])
#       })
#       result<-rbind(result, r)
#     }
#   }
#   if(exists("result")) {
#     finalresult<-rbind(finalresult, result)
#   }
# }
# 
# finalresult<-finalresult[,-1]
# write.csv(finalresult, paste0(resultpath, "/result.csv"), row.names = FALSE)

# sorted_result<-ddply(finalresult, .(tid), function(d) {
#   if(length(d$tid)==1) {
#     return (d)
#   } else {
#     colname<-colnames(d)
#     d<-d[order(d$from),]
#     d$diff<-c(1, diff(d$id))
#     d$diff[which(d$diff!=0)]<-1
#     d$diff<-cumsum(d$diff)
#     r<-ddply(d, .(diff), function(m) {
#       data.frame(tid=m$tid[1], from=min(m$from), to=max(m$to), datacount=sum(m$datacount), colcount=m$colcount, id=m$id[1], colnames=m$colnames[1])
#     })
#     return (r[,-1])
#   }
# })
# sorted_result<-subset(sorted_result, tid!="desc.txt~032")
# write.csv(sorted_result, paste0(resultpath, "/sortedResult.csv"), row.names=FALSE)

#============分析schema================================
sorted_result<-read.csv(paste0(resultpath, "/sortedResult.csv"))
propaths_merged<-read.csv(paste0(resultpath1, "/propaths_withgroup.csv"))
tmp_type_good<-read.csv(paste0(resultpath1, "/tmp_type_good.csv"))
tmp_type_selected<-unique(subset(tmp_type_good, id %in% levels(sorted_result$id), select=c("id", "type")))
tmp_type_selected<-tmp_type_selected[order(tmp_type_selected$type),]
tmp_type_selected<-subset(tmp_type_selected, type=="1.5MW_Freqcon_Vensys_风冷")
tmp_merge<-merge(sorted_result, tmp_type_selected, by=c("id"), all.x = TRUE)
tmp_merge$fid<-as.integer(substr(as.character(tmp_merge$tid), 1, 6))

write.csv(tmp_merge, paste0(resultpath, "/fenxi_tid_type.csv"), row.names=FALSE)

#=====================生成asset定义====================
# tmp_merge_type<-merge(tmp_merge, field_group, by.x="type", by.y="name", all.x = TRUE)
len<-length(tmp_merge$id)
assets<-unique(data.frame(fieldGroupId=rep("fg_7s_1dot5mw_freqcon_vensys_air_cooling_1", times=len), name=paste(tmp_merge$tid, "7s", sep="_"),
                   type="7s", fid=tmp_merge$fid, tid=tmp_merge$tid, description=rep("", times=length(tmp_merge$id)),
                   from=rep("", times=length(tmp_merge$id)), stringsAsFactors=FALSE))
write("<<<<<asset<<<<<<<<<", paste0(resultpath, "/assets_import_fenxi.csv"))
write.table(assets, file=paste0(resultpath, "/assets_import_fenxi.csv"), quote = FALSE, sep=",", na="", row.names = FALSE, fileEncoding = "utf-8", append=TRUE)
