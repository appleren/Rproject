#gw metadata functions
library("plyr")
library(stringdist)
library(tidyr)
library(dplyr)
library(tibble)
library(stringr)
library(digest)

datapath<-"../../data/gw/gw-rt-data-schm/data"
resultpath<-"../../data/gw/gw-rt-data-schm/result"

#����׼��
tmp<-read.csv(paste0(resultpath, "/sortedResult.csv"))
alldp<-readProto()
# write.table(alldp, file="alldp.csv", row.names = FALSE, fileEncoding="UTF-8", quote=TRUE, sep=",", na="")

tmp1<-subset(tmp, (tid>370504000&tid<370505000)|(tid>652217000&tid<652218000)|(tid>622126000&tid<622127000)
             |(tid>622126000&tid<622127000)|(tid>320902000&tid<320903000)|(tid>652210000&tid<652211000)
             |(tid>652211000&tid<652212000)|(tid>652211000&tid<652212000)|(tid>140213000&tid<140214000)
             |(tid>411201000&tid<411202000)|(tid>622128000&tid<622129000)|(tid>210703000&tid<210704000))

tmp2<-subset(tmp, (tid>210703000&tid<210704000))

tmp<-subset(tmp, from>20141000 | (from<20141000 & to>20160000))

d<-tmp
d<-tmp1
d<-tmp2

#����������===============================
dfu<-addFidAndUp(d)
dfu<-mergeBytime(dfu)
dfubf<-schmChangeByfid(dfu)
# write.csv(dfubf, "alldatadfubf.csv")

#ƥ��schema
recSchm_dfubf<-recSchm(dfubf, alldp)

#һЩ�쳣����==================================
d$tid_end<-as.integer(substr(d$tid, 7, 9))
tidToobig<-subset(d, tid_end>300)
colcountBad<-subset(dfubf, (colcount>300 | colcount<50) & tidCount<20)
write.csv(tidToobig, "tidToobig.csv")
write.csv(colcountBad, "colcountBad.csv")

#============�о��仯===================
change_detail<-calChanges(dfu)
write.csv(change_detail, "change_detail.csv", row.names = FALSE)

change_detail<-read.csv("change_detail.csv", header=TRUE)
changed<-findChanged(change_detail)
write.csv(changed, "changed.csv", row.names = FALSE)

#�仯�İٷֱȺ��Ƿ�������ϵ����
#change_back<-findChangeback(changed)
#write.csv(change_back, "change_back.csv", row.names = FALSE)

#unchange_back<-findUnchangeback(changed)
#write.csv(unchange_back, "unchange_back.csv", row.names = FALSE)

cgd_tmp<-subset(changed, (tid>370504000&tid<370505000)|(tid>652217000&tid<652218000)|(tid>622126000&tid<622127000)
                |(tid>622126000&tid<622127000)|(tid>320902000&tid<320903000)|(tid>652210000&tid<652211000)
                |(tid>652211000&tid<652212000)|(tid>652211000&tid<652212000)|(tid>140213000&tid<140214000)
                |(tid>411201000&tid<411202000)|(tid>622128000&tid<622129000)|(tid>210703000&tid<210704000))
write.csv(cgd_tmp, file="changed_9farm.csv", row.names = FALSE)

#���룺 �����ļ� tid, id, from, to, datacount, colcount, colnames
#����� 
#fid, schema, up, inpoto, other

#functions=======================================

#Ԥ�������ݣ�����������fid, up����Ϣ
addFidAndUp<-function(d) {
  d$fid<-substr(d$tid, 1, 6)
  d$up<-sortUp(gsub(pattern="[^A-Za-z0-9,]", replacement="_", toupper(d$colnames)))
  d
}
#sort up
sortUp<-function(upList) {
  for(i in 1:length(upList)) {
    ups<-unlist(str_split(upList[i], pattern=","))
    ups<-str_sort(ups)
    upList[i]<-paste(ups, collapse = ",")
  }
  upList
}

#Ԥ�������ݣ���һ���ϲ�up��ͬ������������¼
mergeBytime<-function(d) {
  #order by from
  d<-d[order(d$tid, d$from), ]
  d$up<-as.factor(d$up)
  d$c<-c(1,diff(d$up))
  d$c[which(d$c!=0)]<-1
  d$c<-cumsum(d$c)
  r<-ddply(d, .(tid,c), function(dt) {
    data.frame(tid=dt$tid[1], id=digest(as.character(dt$up[1]), algo="md5", serialize=FALSE), from=min(dt$from), to=max(dt$to), datacount=sum(dt$datacount), colcount=dt$colcount[1], colnames=dt$colnames[1], fid=dt$fid[1], up=dt$up[1])
  })
  r[, -1]
}
#���糡schema���
schmChangeByfid<-function(d) {
  d$id<-as.character(d$id)
  r<-ddply(d, .(fid, id), function(dt) {
    data.frame(fid=dt$fid[1], id=dt$id[1], from=min(dt$from), to=max(dt$to), tidCount=length(levels(as.factor(dt$tid))), colcount=dt$colcount[1], colnames=dt$colnames[1], up=dt$up[1], tid=paste(levels(as.factor(dt$tid)), collapse = ","))
  })
  #����ÿ���糡��ʷ�����е�������
  r<-ddply(r, .(fid), function(dt) {
    t<-as.character(dt$up)
    tr<-unlist(llply(t, str_split, ","))
    dt$schmSumcount=length(levels(as.factor(tr)))
    dt
  })
  r
}

#ʶ��schema
recSchm<-function(d, alldp) {
  env<-new.env(hash = TRUE, size = NA)
  for(i in 1:length(alldp$upname)) {
    assign(alldp$upname[i], as.character(alldp$descrcn[i]), envir = env)
  }
  d<-subset(d, select=c("id", "up", "colnames", "colcount"))
  r<-ddply(d, .(id), function(dt){
    up<-dt$up[1]
    ups<-unlist(str_split(up, ","))
    changedups<-mget(ups, env, ifnotfound = "NULL")
    data.frame(id=dt$id[1], colcount=dt$colcount[1], unchgcount=length(which(changedups=="NULL")),
               up=dt$up[1], colnames=dt$colnames[1], chgups=paste(unlist(changedups), collapse = ","),
               badname=paste(ups[which(changedups=="NULL")], collapse = ","))
  })
  r
}

#����ÿ���������ʷ��shcema�仯ϸ��
calChanges<-function(d) {
  d$up<-as.character(d$up)
  dchange<-ddply(d, .(tid), function(x) {
    x<-x[order(x$from),]
    result<-data.frame(tid=x$tid[1], from=x$from[1], to=x$to[1], colcount=x$colcount[1], used=0, removed=FALSE, rmv=0, added=FALSE, ad=0,
                       removed_fields="", added_fields="",
                       up=x$up[1])
    if(length(x$tid)==1) {
      return (result)
    } else {
      for(i in 1:(length(x$tid)-1)) {
        result<-rbind(result, cbind(tid=x$tid[1], colcount=x$colcount[i+1], used=calUsed(x$up[1:i], x$up[i+1]), calChange(x$up[i], x$up[i+1], x$colcount[i]), from=x$from[i+1], to=x$to[i+1], up=x$up[i+1]))
      }
      return (result)
    }
  })
}
#ÿ��tid��schema�仯����
calChange<-function(up1, up2, colcount) {
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
  data.frame(removed=rmv, rmv=rmv/colcount, added=ad, ad=ad/colcount,
             removed_fields=rmv_f, added_fields=ad_f)
}
calUsed<-function(upx, up1) {
  if(length(intersect(upx, up1))>0){
    return (1)
  } else {
    return (0)
  }
}


#�仯��
findChanged<-function(d) {
  ddply(d, .(tid), function(x){
    if(length(x$tid)>1){
      return (x)
    }
  })
}
#���ȥ��
findChangeback<-function(d){
  ddply(d, .(tid), function(x){
    if(length(which(x$used==1))>0) {
      return (x)
    }
  })
}
#û�б��ȥ��
findUnchangeback<-function(d){
  ddply(d, .(tid), function(x){
    if(length(which(x$used==1))==0) {
      return (x)
    }
  })
}


#Э������
readProto<-function() {
  propath<-"../../data/gw/gw-rt-data-schm/schemas/goldwind"
  propaths1<-read.csv(paste0(propath, "/propaths1.csv"))
  # propaths2<-read.csv(paste0(propath, "/propaths.csv"))
  propaths2<-read.table(paste0(propath, "/propaths.csv"), header=TRUE, sep=",", encoding = "UTF-8", fileEncoding = "UTF-8", fill=TRUE)
  compath1<-read.csv(paste0(propath, "/aptliscmd.txt"), header=FALSE)
  propaths1<-subset(propaths1, compath %in% compath1$V2)
  propaths2<-subset(propaths2, compath %in% compath1$V2)
  propaths2<-subset(propaths2, !protocolid %in% c("",")", "Mvar", ") ��ֵ", ") ĩֵ"))
  propaths2$protocolid<-as.integer(as.character(propaths2$protocolid))
  # cn<-read.csv(paste0(propath, "/colname.csv"), header = FALSE)
  colnames(propaths2)[2]<-"datapath"
  cn<-c("datapath", "datatype", "descrcn", "protocolid", "compath", "ascflg")
  propaths1<-subset(propaths1, select=cn)
  propaths2<-subset(propaths2, select=cn)
  allpropath<-rbind(propaths1, propaths2)
  allpropath$upname<-gsub(pattern="[^A-Za-z0-9,]", replacement="_", toupper(as.character(allpropath$datapath)))
  allpropath
  # dp<-union(propaths1$datapath, propaths2$datapath)
#   alldp<-data.frame(name=dp, upname=gsub(pattern="[^A-Za-z0-9,]", replacement="_", toupper(dp)), )
#   alldp
}

#������Э��
rr<-ddply(alldp, .(compath, ascflg), function(dt){
  data.frame(protocolcount=length(levels(as.factor(dt$protocolcount))), )
})

