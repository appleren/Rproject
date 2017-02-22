library(plyr)
library(stringdist)
library(tidyr)
library(dplyr)
library(tibble)
library(stringr)


datapath<-"../../data/gw/gaoshan"
resultpath<-"../../data/gw/gaoshan/result"

#================functions===========================
#data type
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
  data.frame(names=to_fieldid(names), data_types=tolower(data_types), iecpath=names, colname=col)
}

to_fieldid<-function(name) {
  l<-grep("[0123456789]", substr(name, 1, 1))
  if(length(l)==0 || l!=1) {
    gsub(pattern="[^A-Za-z0-9,]", replacement="_", tolower(as.character(name)))
  } else {
    gsub(pattern="[^A-Za-z0-9,]", replacement="_", tolower(as.character(paste0("d_", name))))
  }
}

#============================deal with data files==============================
folders<-list.dirs(path=paste0(datapath, "/schm"))
finalresult<-data.frame()
for(i in 1:length(folders)) {
# for(i in 1:7) {
  p<-folders[i]
  if(!(substr(p, str_locate_all(p, "/")[[1]][length(str_locate_all(p, "/")[[1]])]+1, nchar(p)) %in% c("652208","150922","220804","140212","150930","140611"))){
    filenames<-list.files(path=p, pattern=".txt", full.names=TRUE)
    if(length(filenames)>0) {
      result<-data.frame()
      for(j in 1:length(filenames)) {
        data<-read.table(filenames[j], sep=";")
        colnames(data)<-c("id", "date", "tid", "datacount", "colcount", "colnames")
        data$c<-c(1,diff(data$id))
        data$c[which(data$c!=0)]<-1
        data$c<-cumsum(data$c)
        r<-ddply(data, .(tid,c), function(d) {
          # d<-subset(d, date>20141200)
          if(length(d$date)>0) {
            data.frame(tid=d$tid[1], id=d$id[1], from=min(d$date), to=max(d$date), datacount=sum(d$datacount), colcount=d$colcount[1], colnames=d$colnames[1])
          }
        })
        if(length(r$tid)>0) {
          result<-rbind(result, r)
        }
      }
    }
  }
  if(exists("result")) {
    finalresult<-rbind(finalresult, result)
  }
}

finalresult<-finalresult[,-1]
write.csv(finalresult, paste0(resultpath, "/result_good.csv"), row.names = FALSE)

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

write.csv(sorted_result, paste0(resultpath, "/sortedResult_less.csv"), row.names=FALSE)

#========================propaths==================
propaths_2<-read.csv(paste0(datapath, "/propaths_wtinfo/propaths_2.csv"))
propaths_3<-read.csv(paste0(datapath, "/propaths_wtinfo/propaths_3.csv"))

sub_2<-subset(propaths_2, select = c("protocolid", "datapath", "descrcn", "compath", "ascflg"))
colnames(sub_2)<-c("protocolid", "iecpath", "descrcn", "compath", "ascflg")
sub_2$iecpath<-as.character(sub_2$iecpath)
sub_2$descrcn<-as.character(sub_2$descrcn)
sub_3<-subset(propaths_3, select = c("protocolid", "iecpath", "descrcn", "compath", "ascflg"))
sub_3$iecpath<-as.character(sub_3$iecpath)
sub_3$descrcn<-as.character(sub_3$descrcn)
propaths<-rbind(sub_2, sub_3)

#================deal with some files======================
#=======protocol_type================
# pro_type<-read.csv(paste0(datapath, "/propaths_wtinfo/protocol_type.csv"))
# pro_new<-read.csv(paste0(datapath, "/propaths_wtinfo/protocol_new.csv"))
# pro_all<-merge(pro_type, pro_new, by="protocolid", all=TRUE)
# write.csv(pro_all, paste0(resultpath, "/protocol_all.csv"), row.names = FALSE, fileEncoding = "utf-8")
pro_type<-read.csv(paste0(datapath, "/propaths_wtinfo/protocol_all.csv"))
pro_type<-subset(pro_type, select=c("protocolid", "version", "type", "year", "group", "desc"))
# length(levels(as.factor(pro_type$protocolid))) #121
# length(levels(as.factor(propaths$protocolid))) #131
length(levels(as.factor(wtinf$protocolid)))
pro_type$group<-as.character(pro_type$group)
propaths_merged<-merge(propaths, pro_type, by="protocolid", all.x=TRUE)
wtinf<-read.csv(paste0(datapath, "/propaths_wtinfo/wtinf.csv"))
# wtinf$wtid<-as.integer(str_replace(as.character(wtinf$wtid), "140828", "140626"))
wtinf_merged<-merge(wtinf, pro_type, by="protocolid", all.x=TRUE)
colnames(wtinf_merged)[2:3]<-c("fid", "tid")

wtinf_2<-read.csv(paste0(datapath, "/propaths_wtinfo/wtinf_2nd.csv"), header=FALSE)
colnames(wtinf_2)<-c("fid", "fid_name", "tid", "wtnarrate", "protocolid")
wtinf_2_merged<-merge(wtinf_2, pro_type, by="protocolid", all.x = TRUE)
wtinf_2_merged<-subset(wtinf_2_merged, select=c("protocolid", "fid", "tid", "wtnarrate", "version", "type", "year", "group", "desc"))

# wtinf_merged$wtnarrate<-as.character(wtinf_merged$wtnarrate)
# wtinf_merged$type<-as.character(wtinf_merged$type)
# wtinf_merged$desc<-as.character(wtinf_merged$desc)
# wtinf_2_merged$wtnarrate<-as.character(wtinf_2_merged$wtnarrate)
# wtinf_2_merged$type<-as.character(wtinf_2_merged$type)
# wtinf_2_merged$desc<-as.character(wtinf_2_merged$desc)
wtinf_merged<-rbind(wtinf_merged, wtinf_2_merged)

write.csv(wtinf_merged, paste0(resultpath, "/wtinf_withgroup.csv"), row.names = FALSE)
write.csv(propaths_merged, paste0(resultpath, "/propaths_withgroup.csv"), row.names = FALSE)

#=====================schema====================
# tmp<-read.csv(paste0(resultpath, "/sortedResult.csv"))
tmp<-read.csv(paste0(resultpath, "/sortedResult_less.csv"))
tmp$fid<-as.integer(substr(as.character(tmp$tid), 1, 6))
tmp$tidtype<-as.integer(substr(as.character(tmp$tid), 7, 9))
tmp$tid[which(tmp$fid==140626&tmp$tidtype<34)]<-as.integer(str_replace(as.character(tmp$tid[which(tmp$fid==140626&tmp$tidtype<34)]), "140626", "140828"))
# a<-tmp$tid[which(tmp$fid==140626)]

wtinf_merged<-read.csv(paste0(resultpath, "/wtinf_withgroup.csv"))
# a<-subset(wtinf_merged, fid==140626)
# b<-subset(tmp, fid==140828 & tidtype<34)
# b<-subset(tmp, fid==140626 & tidtype<34)
propaths_merged<-read.csv(paste0(resultpath, "/propaths_withgroup.csv"))

# length(levels(as.factor(tmp$tid))) #6179
# length(levels(as.factor(wtinf_merged$wtid))) #12613
tmp_type<-merge(tmp, wtinf_merged, by=c("tid"), all.x = TRUE)   ###########lulululu
tmp_type<-subset(tmp_type, tidtype<600)

#tid with no protocol
tid_no_protocol<-subset(tmp_type, is.na(protocolid))
write.csv(tid_no_protocol, paste0(resultpath, "/tid_no_protocol.csv"), row.names = FALSE)
a<-data.frame(levels(as.factor(as.character(tid_no_protocol$tid))))
write.csv(data.frame(levels(as.factor(as.character(tid_no_protocol$tid)))), paste0(resultpath, "/tid_no_protocol_1.csv"))

#good data
tmp_type_good<-subset(tmp_type, !is.na(protocolid) & !is.na(type))  #5740??????
write.csv(tmp_type_good, paste0(resultpath, "/tmp_type_good.csv"), row.names = FALSE)

cn<-data.frame(colnames=tmp_type_good$colnames)
cn$num<-rep.int(1, length(cn$colnames))
b<-aggregate(.~colnames, data=cn, sum)
b$colnames<-as.factor(as.character(b$colnames))

#===????schema????????????????????=====
# schm<-dlply(b, .(colnames), function(d) {
#   rst<-list()
#   rst$detail<-splitCol(as.character(d$colnames[1]))
#   rst$colname<-d$colnames[1]
#   rst$num<-d$num[1]
#   rst
# })

#===============generate import file================
#====================schema frequency==========
# fid                         types
# 1  140212        1.5MW_Switch_SSB_Hydac
# 2  140212  1.5MW_Switch_Vensys_Hydac,GL
# 3  150701     1.5MW_Freqcon_Vensys_????
# 4  150701  1.5MW_Switch_Vensys_Hydac,GL
# 5  520222     1.5MW_Freqcon_Vensys_????
# 6  520222                         2.5MW
# 7  620903     1.5MW_Freqcon_Vensys_????
# 8  620903                         2.5MW
# 9  620903                         3.0MW
# 10 640307     1.5MW_Freqcon_Vensys_????
# 11 640307  1.5MW_Switch_Vensys_Hydac,GL
# 12 652240 1.5MW_F??????????_Vensys_????
# 13 652240                         2.0MW
# 14 652240                         2.5MW
# aggregate_tmp_type<-ddply(tmp_type, .(fid), function(d){
#   l<-length(levels(as.factor(as.character(d$type))))
#   if(length(l)!=0) {
#     if(l>1) {
#       data.frame(fid=rep(d$fid[1], l), types=levels(as.factor(as.character(d$type))))
#     }
#   }
# })

#================order schema according to frequency====================
#d<-subset(tmp_type_good, type=="1.5MW_Freqcon_Vensys_????")
schema_version<-ddply(tmp_type_good, .(type, version), function(d){
  all_schm<-levels(as.factor(as.character(d$colnames)))
  all_schm_fm<-data.frame(sname=all_schm, num=seq(from=1, to=length(all_schm), by=1))
  
  #dd<-subset(all_schm_fm, num==1)
  all_schm_fm_version<-ddply(all_schm_fm, .(sname), function(dd){
    ype_schema<-splitCol(dd$sname[1])
    t<-as.character(d$type[1])
    propaths_type<-subset(propaths_merged, as.character(type)==t)
    propaths_type$names<-sapply(propaths_type$iecpath, to_fieldid)
    # td<-subset(ype_schema, names=="wcnv_hz_instmag_f")
    propaths_type_distinct<-ddply(ype_schema, .(names), function(td){
      ddd<-subset(propaths_type, names==td$names[1])
      if(length(ddd$year)>0) {
#         lv<-levels(as.factor(rbind(tolower(as.character(ddd$iecpath)), tolower(as.character(td$iecpath)))))
#         l<-length(lv)
        td1<-ddd[which.min(ddd$year),]
        # td1$iecpath<-as.character(td$iecpath[1])
      } else {
#         lv<-levels(as.factor(tolower(as.character(td$iecpath))))
#         l<-length(lv)
        td1 = data.frame(names=td$names[1], version="")
      }
      data.frame(names=td1$names, descrcn_version=td1$version)
    })
    dd$v<-rep(max(propaths_type_distinct$descrcn_version), times=length(dd$sname))
    dd
  })
})

schema_version<-schema_version[, c("type", "num", "v", "sname", "version")]
write.csv(schema_version, paste0(resultpath, "/schema_version.csv"), row.names = FALSE, fileEncoding = "utf-8")

#===========order schema according to frequency==================
tmp_up_time<-subset(tmp_type_good, select=c("tid", "from", "to", "colnames", "type"))
colnames(tmp_up_time)[4]<-"sname"
tmp_up_time_f<-merge(tmp_up_time, schema_version, by=c("type", "sname"), all.x = TRUE)
tid_version_up_time<-subset(tmp_up_time_f, select=c("tid", "type", "from", "to", "v"))
tid_version_up_time<-tid_version_up_time[order(tid_version_up_time$tid, tid_version_up_time$from),]
#d<-subset(tid_version_up_time, tid==130711007)
# ddply(tid_version_up_time, .(tid), function(d){
#   
# })
tid_version_up_time$d<-c(1, diff(as.integer(tid_version_up_time$v)))
tid_version_up_time$d[which(tid_version_up_time$d!=0)]<-1
tid_version_up_time$d<-cumsum(tid_version_up_time$d)
# d<-subset(tid_version_up_time, tid==130711007&d==80)
r<-ddply(tid_version_up_time, .(tid,d), function(d) {
  if(length(d$tid)==1) {
    frm<-d$from[1]
    t<-d$to[1]
  } else {
    frm<-min(d$from)
    t<-max(d$to)
  }
  data.frame(tid=d$tid[1], from=frm, to=t, v=d$v[1])
})
tid_version_up_time_f<-r[,-1]
write.csv(tid_version_up_time_f, paste0(resultpath, "/tid_version_up_time_f.csv"), row.names = FALSE, fileEncoding = "utf-8")

#==============chinese column name===========
# d<-subset(tmp_type_good, as.integer(type)==3)
type_schema_col_names<-ddply(tmp_type_good, .(type, version), function(d){
  colname_level<-levels(as.factor(as.character(d$colnames)))
  type_schema<-ldply(colname_level, splitCol)
  type_schema$colname<-paste(d$type[1], d$version[1], as.integer(type_schema$colname), sep="_")
  type_schema$count<-rep(1, length(type_schema$names))
  type_schema_count<-ddply(type_schema, .(names), function(td){
    l<-length(levels(as.factor(as.character(td$data_types))))
    if(length(l)!=0) {
      if(l>1) {
        data.frame(names=rep(td$names[1], l), iecpath=rep(tolower(td$iecpath[1]), l), data_types=levels(as.factor(as.character(td$data_types))), colname=td$colname[1])
      }
    }
  })
  type_schema_count$type<-rep(d$type[1], length(type_schema_count$names));
  write.csv(type_schema_count, paste0(resultpath, "/colname_types/", d$type[1], "_", d$version[1], ".csv"), row.names = FALSE, fileEncoding="utf-8")
  
  # td<-subset(type_schema_count, names=="wgen_pwrepd_instmag_i")
  type_schema1<-ddply(type_schema, .(names), function(td){
    lv<-levels(as.factor(as.character(td$data_types)))
    l<-length(lv)
    if(length(l)!=0) {
      if(l>0) {
        if("key" %in% lv) {
          data.frame(names=as.factor(td$names[1]), data_types="key", colname=paste(td$colname, collapse = ";"))
        } else if("varchar" %in% lv) {
          data.frame(names=as.factor(td$names[1]), data_types="varchar", colname=paste(td$colname, collapse = ";"))
        } else if("char(100)" %in% lv) {
          data.frame(names=as.factor(td$names[1]), data_types="char(100)", colname=paste(td$colname, collapse = ";"))
        } else if("real" %in% lv) {
          data.frame(names=as.factor(td$names[1]), data_types="real", colname=paste(td$colname, collapse = ";"))
        } else if("double" %in% lv) {
          data.frame(names=as.factor(td$names[1]), data_types="double", colname=paste(td$colname, collapse = ";"))
        } else if("float" %in% lv) {
          data.frame(names=as.factor(td$names[1]), data_types="float", colname=paste(td$colname, collapse = ";"))
        } else if("integer" %in% lv) {
          data.frame(names=as.factor(td$names[1]), data_types="integer", colname=paste(td$colname, collapse = ";"))
        } else if("smallint" %in% lv) {
          data.frame(names=as.factor(td$names[1]), data_types="smallint", colname=paste(td$colname, collapse = ";"))
        } else if("datetime" %in% lv) {
          data.frame(names=as.factor(td$names[1]), data_types="datetime", colname=paste(td$colname, collapse = ";"))
        } else if("timestamp" %in% lv) {
          data.frame(names=as.factor(td$names[1]), data_types="timestamp", colname=paste(td$colname, collapse = ";"))
        } else {
          data.frame(names=as.factor(td$names[1]), data_types=td$data_types[1], colname=paste(td$colname, collapse = ";"))
        }
      } else {
        data.frame(names=as.factor(td$names[1]), data_types=td$data_types[1], colname=paste(td$colname, collapse = ";"))
      }
    }
  })
  
  b<-ddply(type_schema, .(names), function(m){
    data.frame(count=length(m$names), iecpath=tolower(m$iecpath[1]), names=m$names[1])
  })
  b_m<-merge(b, type_schema1, by="names", all.x = TRUE)
  b_m<-b_m[order(b_m$count, decreasing=TRUE),]
  b_m$type<-rep(d$type[1], length(b_m$names));
  b_m$version<-rep(d$version[1], length(b_m$names));
  write.csv(b_m, paste0(resultpath, "/colnames/", d$type[1], "_", d$version[1], ".csv"), row.names = FALSE, fileEncoding="utf-8")
  b_m
})



write.csv(type_schema_col_names, paste0(resultpath, "/type_schema_col_names.csv"), row.names = FALSE, fileEncoding = "utf=8")

type_schema_col_names<-read.csv(paste0(resultpath, "/type_schema_col_names.csv"))

type_schema_col_names<-subset(type_schema_col_names, !data_types%in%c("key", "null"))
type_schema_col_names$data_types<-as.factor(as.character(type_schema_col_names$data_types))
levels(type_schema_col_names$data_types)[levels(type_schema_col_names$data_types) %in% c("char(100)", "varchar", "bit", "timestamp", "datetime")] <- "string"
levels(type_schema_col_names$data_types)[levels(type_schema_col_names$data_types) %in% c("smallint", "integer")] <- "int"
levels(type_schema_col_names$data_types)[levels(type_schema_col_names$data_types) %in% c("real")] <- "double"
levels(type_schema_col_names$data_types)[levels(type_schema_col_names$data_types) %in% c("float")] <- "float"

# type_schema_col_names$cc<-rep(1, length(type_schema_col_names$count))
# type_schema_col_names$haha<-paste(type_schema_col_names$names, type_schema_col_names$iecpath, sep = "_")
# c<-aggregate(cc~haha, data=type_schema_col_names, sum)

# b<-subset(tmp_type_good, grepl("wgen.sensor11tmp", as.character(colnames), ignore.case = TRUE))

#============chinese column name=================
# d<-subset(type_schema_col_names, as.integer(type)==3)
type_schema_col_names_chinese<-ddply(type_schema_col_names, .(type), function(d){
  t<-as.character(d$type[1])
  propaths_type<-subset(propaths_merged, as.character(type)==t)
  propaths_type$names<-sapply(propaths_type$iecpath, to_fieldid)
  # td<-subset(propaths_type, names=="wcnv_hz_instmag_f")
  propaths_type_distinct<-ddply(propaths_type, .(names), function(td){
    dd<-subset(d, names==td$names[1])
    lv<-levels(as.factor(rbind(tolower(as.character(dd$iecpath)), tolower(as.character(td$iecpath[1])))))
    l<-length(lv)
    td1<-td[which.max(td$year),]
    if(length(l)!=0) {
      if(l>0) {
        td1$iecpath<-paste(lv, collapse=";")
      }
    }
    data.frame(names=td1$names, iecpath=td1$iecpath, descrcn=td1$descrcn, descrcn_version=td1$version)
  })
  d_m<-merge(d, propaths_type_distinct, by="names", all.x = TRUE)
  d_m<-d_m[order(d_m$count, decreasing=TRUE),c("names", "count", "iecpath.x", "colname", "data_types", "type", "version", "descrcn", "descrcn_version")]
  colnames(d_m)[3]<-"iecpath"
  d_m
})

type_schema_col_names_chinese1<-type_schema_col_names_chinese[, c( "names", "count", "iecpath", "descrcn", "data_types", "type")]
write.csv(type_schema_col_names_chinese, paste0(resultpath, "/type_schema_col_names_chinese.csv"), row.names = FALSE, fileEncoding = "utf=8")
write.csv(type_schema_col_names_chinese1, paste0(resultpath, "/type_schema_col_names_chinese1.csv"), row.names = FALSE, fileEncoding = "utf=8")

#======================generate field group import file===================
# fieldGroupId,id,isIdField,name,description,valueType,intervals,unit
# fg1,f1,true,fname1,description1,Double,1|3|2,second
# fg1,f2,true,fname2,,Double,3,second
# fg1,f3,false,fname3,,Double,,second
# fg1,f4,false,fname4,,Double,,second
# fg2,f5,true,fname5,,Double,,second
# fg2,f6,false,fname6,,Double,,hour

#remove timestamp
type_schema_col_names_chinese<-read.csv(paste0(resultpath, "/type_schema_col_names_chinese.csv"))
type_schema_col_names_chinese<-subset(type_schema_col_names_chinese, !names%in%c("wman_tm", "wtur_tm_rw_dt"))


fg<-read.csv(paste0(datapath, "/propaths_wtinfo/fg.csv"))
field_group<-subset(fg, name %in% levels(as.factor(type_schema_col_names_chinese$type)))
len<-length(type_schema_col_names_chinese$type)
f<-data.frame(fieldGroupId=as.character(type_schema_col_names_chinese$type), id=as.character(type_schema_col_names_chinese$names),
                   isIdField=rep(FALSE, times=len), name=as.character(type_schema_col_names_chinese$descrcn),
                   description=as.character(type_schema_col_names_chinese$iecpath), valueType=as.character(type_schema_col_names_chinese$data_types),
                   intervals=rep(7000, times=len), unit=rep("",times=len), count=type_schema_col_names_chinese$count, stringsAsFactors=FALSE)
# d<-subset(f, fieldGroupId=="1.5MW_Freqcon_Vensys_????")
fields<-ddply(f, .(fieldGroupId), function(d){
  fg_id<-as.character(field_group$id[which(field_group$name==d$fieldGroupId[1])])
  tp<-c(fieldGroupId=fg_id,id="type",isIdField=TRUE,name="????",
         description="type",valueType="string",intervals=7000,unit="")
  tid<-c(fieldGroupId=fg_id,id="tid",isIdField=TRUE,name="????????",
        description="tid",valueType="string",intervals=7000,unit="")
  fid<-c(fieldGroupId=fg_id,id="fid",isIdField=TRUE,name="????????",
         description="fid",valueType="string",intervals=7000,unit="")
  d<-d[order(d$count, decreasing = FALSE),]
  d$fieldGroupId<-rep(fg_id,times=length(d$isIdField))
  rbind(tp, fid, tid, subset(d, select=c("fieldGroupId", "id", "isIdField", "name", "description", "valueType", "intervals", "unit")))
})

write.csv(fields, paste0(resultpath, "/fields.csv"), row.names = FALSE, fileEncoding = "utf=8")

# <<<<<fieldGroup<<<<<<<<<
#   id,name,description,tag,attr1,attr2, ...
# fg1,fg1,description1,tag1|tag2,A,
# fg2,fg2,,tag1|tag3,,B
# [1] "1.5MW_Freqcon_Vensys_????"     "1.5MW_F??????????_Vensys_????" "1.5MW_Switch_LUST_Hydac"       "1.5MW_Switch_SSB_Hydac"        "1.5MW_Switch_Vensys_Hydac,GL" 
# [6] "2.0MW"                         "2.5MW"                         "3.0MW"     

# fields<-merge(fields, field_group, by.x = "fieldGroupId", by.y = "name", all.x = TRUE)
# fields<-subset(fields, select=c("id.y", "id.x", "isIdField", "name", "description.x", "valueType", "intervals", "unit"))
# colnames(fields)<-c("fieldGroupId", "id", "isIdField", "name", "description", "valueType", "intervals", "unit")
#================generate file=================
write("<<<<<fieldGroup<<<<<<<<<", paste0(resultpath, "/fieldgroup_import.csv"))
write.table(field_group, file=paste0(resultpath, "/fieldgroup_import.csv"), quote = FALSE, sep=",", na="", row.names = FALSE, fileEncoding = "utf-8", append=TRUE)
write("<<<<<field<<<<<<<<<", paste0(resultpath, "/fieldgroup_import.csv"), append=TRUE)
write.table(fields, file=paste0(resultpath, "/fieldgroup_import.csv"), quote = FALSE, sep=",", na="", row.names = FALSE, fileEncoding = "utf-8", append=TRUE)

#==========generate asset import file=======================
# <<<<<<<asset<<<<<<<<<<<<<<<<<<<<<<
#   fieldGroupId,name,f1,f2,f5,description,from,tag,attr1,attr2
# fg1,asset1,A,B,,,2015-01-15T14:06:22.237Z,tag1|tag2,,attr2B
# fg1,asset1,A,B,,,2016-01-15T14:06:22.237Z,tag1|tag2,,attr2B
# fg1,asset2,A,C,,,2016-01-15T14:06:22.237Z,tag1,attr1a,,
# fg2,asset3,,,D,description4,,,attr1d,attr2d

tmp_type_good<-read.csv(paste0(resultpath, "/tmp_type_good.csv"))
tid_frm<-data.frame(tid=levels(as.factor(tmp_type_good$tid)))

wtinf_merged<-read.csv(paste0(resultpath, "/wtinf_withgroup.csv"))
colnames(wtinf_merged)[2:3]<-c("fid", "tid")
tid_type<-merge(tid_frm, wtinf_merged, by=c("tid"), all.x = TRUE)
tid_type<-merge(tid_type, field_group, by.x="type", by.y="name", all.x = TRUE)
tid_type$tag<-paste(tid_type$fid, tid_type$type, sep="|")
assets<-data.frame(fieldGroupId=tid_type$id, name=paste(tid_type$tid, "7s", sep="_"),
                   type="7s", fid=tid_type$fid, tid=tid_type$tid, description=rep("", times=length(tid_type$id)),
                   from=rep("", times=length(tid_type$id)), tag=tid_type$tag, stringsAsFactors=FALSE)
write.csv(assets, paste0(resultpath, "/assets.csv"))
write("<<<<<asset<<<<<<<<<", paste0(resultpath, "/assets_import.csv"))
write.table(assets, file=paste0(resultpath, "/assets_import.csv"), quote = FALSE, sep=",", na="", row.names = FALSE, fileEncoding = "utf-8", append=TRUE)

# assets$count=rep(1, times=length(assets$fid))
# a<-aggregate(count~fieldGroupId, data=assets, sum)

used_vars<-read.csv(paste0(datapath, "/propaths_wtinfo/used_vars.csv"))
vars<-levels(used_vars$var1)
names<-levels(used_vars$name1)
fields_sub<-subset(fields, id %in% vars | name %in% names)
length(vars)
found_vars<-levels(as.factor(fields_sub$id))
setdiff(vars, found_vars)
setdiff(found_vars, vars)
length(levels(as.factor(fields_sub$id)))
length(names)
length(levels(as.factor(fields_sub$name)))

# a<-assets[, c(1, 4,5)]
# write.csv(a, paste0(resultpath, "/tid_type.csv"), row.names = FALSE, fileEncoding = "utf=8")

#=====================generate unified_field==================
fields<-read.csv(paste0(resultpath, "/fields.csv"))
# f<-subset(fields, fieldGroupId=="fg_7s_1dot5mw_freqcon_vensys_air_cooling")
# f$fieldGroupId<-"fg_7s_1dot5mw_freqcon_vensys_air_cooling_1"
# fields<-rbind(fields, f)
#unified_filed id
unified_field<-read.csv(paste0(datapath, "/propaths_wtinfo/unified_field.csv"))
fields<-subset(fields, isIdField!=TRUE)
unified_field<-subset(unified_field, ufid!=12)
merged_fieles_unified<-merge(fields, unified_field, by="id", all.x = TRUE)
merged_fieles_unified<-subset(merged_fieles_unified, !is.na(ufid), select=c("fieldGroupId", "id", "preferId", "name.y", "valueType", "ufid"))
colnames(merged_fieles_unified)<-c("fieldGroupId", "fieldId", "id", "name", "valueType", "ufid")
merged_fieles_unified<-merged_fieles_unified[order(merged_fieles_unified$fieldGroupId, merged_fieles_unified$ufid),]

# <<<<<unifiedField<<<<<<<
# unified_field_id, unified_field_name,value_type, field_group_id.field_ids
# wind_spead,·çËÙ,double,fg.ws1|fg.ws2|fg.ws3

merged_fieles_unified_tmp<-subset(merged_fieles_unified, fieldGroupId=="fg_7s_1dot5mw_freqcon_vensys_air_cooling")
merged_fieles_unified_tmp$fieldGroupId<-"fg_7s_1dot5mw_freqcon_vensys_air_cooling_1"

merged_fieles_unified<-rbind(merged_fieles_unified,merged_fieles_unified_tmp)
# merged_fieles_unified<-subset(merged_fieles_unified, fieldGroupId!="fg_7s_3dot0mw_semi_direct_drive")

merged_fieles_unified$fields<-paste(merged_fieles_unified$fieldGroupId, merged_fieles_unified$fieldId, sep = ".")
unified_field_import<-ddply(merged_fieles_unified, .(ufid), function(d){
  id<-d$id[1]
  name<-d$name[1]
  vt<-"FLOAT"
  vts<-levels(as.factor(as.character(d$valueType)))
  if("string" %in% tolower(vts)) {
    vt<-"STRING"
  } else if("double" %in% tolower(vts)) {
    vt<-"DOUBLE"
  } else if("float" %in% tolower(vts)){
    vt<-"FLOAT"
  } else if("int" %in% tolower(vts)){
    vt<-"INT"
  }
  data.frame(unified_field_id=id, unified_field_name=name, value_type=vt, field_group_id.field_ids=paste(d$fields, collapse = "|"))
})

unified_field_import<-unified_field_import[,-1]
write("<<<<<unifiedField<<<<<<<<<", paste0(resultpath, "/unifiedField_import.csv"))
write.table(unified_field_import, file=paste0(resultpath, "/unifiedField_import.csv"), quote = FALSE, sep=",", na="", row.names = FALSE, fileEncoding = "utf-8", append=TRUE)

