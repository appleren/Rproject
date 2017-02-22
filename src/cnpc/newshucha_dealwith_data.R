#=========read data=============
source("newshucha_header.R")

oil_field_input<-read.csv(paste(datapath, 
                          "mid/陕京管道生产运行日报表-进气注采气/", "oil_field_input.csv", sep=""),
                          header=TRUE)
pipe_store<-read.csv(paste(datapath, 
                     "mid/陕京管道生产运行日报表-进气注采气/", "pipe_store.csv", sep=""),
                     header=TRUE)
self_used<-read.csv(paste(datapath, 
                    "mid/陕京管道生产运行日报表-进气注采气/", "self_used.csv", sep=""),
                    header=TRUE)
sub_line_input<-read.csv(paste(datapath, 
                         "mid/陕京管道生产运行日报表-进气注采气/", "sub_line_input.csv", sep=""),
                         header=TRUE)
oil_station_zhuqi<-read.csv(paste(datapath, 
                            "mid/陕京管道生产运行日报表-进气注采气/", "oil_station_zhuqi.csv", sep=""),
                            header=TRUE)
oil_station_caiqi<-read.csv(paste(datapath, 
                            "mid/陕京管道生产运行日报表-进气注采气/", "oil_station_caiqi.csv", sep=""),
                            header=TRUE)
stops_output<-read.csv(paste(datapath, 
                       "mid/陕京管道生产运行日报表-销气/", "stops_output.csv", sep=""),
                       header=TRUE)
line_self_used<-read.csv(paste(datapath, 
                         "mid/陕京输气线自耗气情况表/", "line_self_used.csv", sep=""),
                         header=TRUE)

#============filter=======================
stops_output<-subset(stops_output, line_no!="合计"&line_no!="总合计"&line_no!="大唐煤制气")
stops_output$stop_name<-na.locf(stops_output$stop_name)
stops_output<-subset(stops_output, !is.na(daily_stops_output))
stops_output<-subset(stops_output, select=-1)

write.csv(stops_output, paste(datapath, "mid/", "stops_output.csv", sep=""),row.names=FALSE)

line_self_used<-subset(line_self_used, line_type!="自耗气总合计")
line_self_used<-subset(line_self_used, gsub("([N ])", "", belongs_to)!="合计")
line_self_used<-subset(line_self_used, gsub("([N ])", "", belongs_to)!="小计")
line_self_used$line_name<-as.character(line_self_used$line_name)
line_self_used$line_name[which(is.na(line_self_used$line_name))]<-rep("noName", length(which(is.na(line_self_used$line_name))))
line_self_used<-subset(line_self_used, gsub("([N ])", "", line_name)!="小计")

write.csv(line_self_used, paste(datapath, "mid/", "line_self_used.csv", sep=""),row.names=FALSE)

oil_field_input$oil_field<-as.character(oil_field_input$oil_field)
oil_field_input$station_name<-as.character(oil_field_input$station_name)
oil_field_input<-subset(oil_field_input, grepl("合计", oil_field)==FALSE)
oil_field_input<-subset(oil_field_input, grepl("小计", station_name)==FALSE)

write.csv(oil_field_input, paste(datapath, "mid/", "oil_field_input.csv", sep=""),row.names=FALSE)

oil_station_caiqi<-subset(oil_station_caiqi, as.character(station_area)!="合计")
oil_station_caiqi<-subset(oil_station_caiqi, as.character(station_name)!="小计"&as.character(station_name)!="合计")
write.csv(oil_station_caiqi, paste(datapath, "mid/", "oil_station_caiqi.csv", sep=""),row.names=FALSE)

oil_station_zhuqi<-subset(oil_station_zhuqi, as.character(station_area)!="合计")
oil_station_zhuqi<-subset(oil_station_zhuqi, as.character(station_name)!="合计"&as.character(station_name)!="小计")
write.csv(oil_station_zhuqi, paste(datapath, "mid/", "oil_station_zhuqi.csv", sep=""),row.names=FALSE)

pipe_store<-subset(pipe_store, line!="合计")
write.csv(pipe_store, paste(datapath, "mid/", "pipe_store.csv", sep=""),row.names=FALSE)

write.csv(sub_line_input, paste(datapath, "mid/", "sub_line_input.csv", sep=""),row.names=FALSE)

write.csv(self_used, paste(datapath, "mid/", "self_used.csv", sep=""),row.names=FALSE)


#===============merge===================
oil_field_input<-read.csv(paste0(datapath, "mid/oil_field_input.csv"), header=TRUE, as.is=TRUE)
oil_field_input<-subset(oil_field_input, select=c("date", "daily_quantity"))
sum_oil_field_input <- aggregate(daily_quantity~date, data = oil_field_input, sum)
colnames(sum_oil_field_input)<-c("date", "oil_field_input")

oil_station_caiqi<-read.csv(paste0(datapath, "mid/oil_station_caiqi.csv"), header=TRUE, as.is=TRUE)
oil_station_caiqi<-subset(oil_station_caiqi, select=c("date", "daily_quantity"))
sum_oil_station_caiqi <- aggregate(daily_quantity~date, data = oil_station_caiqi, sum)
colnames(sum_oil_station_caiqi)<-c("date", "oil_station_caiqi")

oil_station_zhuqi<-read.csv(paste0(datapath, "mid/oil_station_zhuqi.csv"), header=TRUE, as.is=TRUE)
oil_station_zhuqi<-subset(oil_station_zhuqi, select=c("date", "daily_quantity"))
sum_oil_station_zhuqi <- aggregate(daily_quantity~date, data = oil_station_zhuqi, sum)
colnames(sum_oil_station_zhuqi)<-c("date", "oil_station_zhuqi")

sub_line_input<-read.csv(paste0(datapath, "mid/sub_line_input.csv"), header=TRUE, as.is=TRUE)
sub_line_input<-subset(sub_line_input, select=c("date", "daily_quantity"))
sum_sub_line_input <- aggregate(daily_quantity~date, data = sub_line_input, sum)
colnames(sum_sub_line_input)<-c("date", "sub_line_input")

stops_output<-read.csv(paste0(datapath, "mid/stops_output.csv"), header=TRUE, as.is=TRUE)
stops_output<-subset(stops_output, select=c("date", "daily_stops_output"))
sum_stops_output <- aggregate(daily_stops_output~date, data = stops_output, sum)
colnames(sum_stops_output)<-c("date", "stops_output")

line_self_used<-read.csv(paste0(datapath, "mid/line_self_used.csv"), header=TRUE, as.is=TRUE)
line_self_used$line_name_key<-paste0(line_self_used$belongs_to, line_self_used$line_name)
line_self_used$month<-as.POSIXlt(line_self_used$date)$mon+1
line_self_used$year<-as.POSIXlt(line_self_used$date)$year+1900
dd<-ddply(line_self_used, .(line_name_key, year, month), function(d){
  d$daily_value<-c(d$monthly_used[1], diff(d$monthly_used))
  d
})
# dd$date<-as.Date(dd$date)
sum_line_self_used<-aggregate(daily_value~date, data=dd, sum)
colnames(sum_line_self_used)<-c("date", "line_self_used")
# write.csv(dd, paste(datapath, "tmp/", "line_self_used_agg.csv", sep=""),row.names=FALSE)


self_used<-read.csv(paste0(datapath, "mid/self_used.csv"), header=TRUE, as.is=TRUE)
self_used<-subset(self_used, select=c("date", "daily_quantity"))
colnames(self_used)<-c("date", "self_used")

pipe_store<-read.csv(paste0(datapath, "mid/pipe_store.csv"), header=TRUE, as.is=TRUE)
pipe_store<-subset(pipe_store, line!="大唐煤制气", select=-1)
sum_pipe_store <- aggregate(.~date, data = pipe_store, sum)

mergedData<-merge(sum_oil_field_input, sum_oil_station_caiqi, by="date")
mergedData<-merge(mergedData, sum_oil_station_zhuqi, by="date")
mergedData<-merge(mergedData, sum_sub_line_input, by="date")
mergedData<-merge(mergedData, self_used, by="date")
mergedData<-merge(mergedData, sum_pipe_store, by="date")
mergedData<-merge(mergedData, sum_stops_output, by="date")
mergedData<-merge(mergedData, sum_line_self_used, by="date")

mergedData$Qr<-mergedData$oil_field_input+mergedData$oil_station_caiqi+mergedData$sub_line_input
mergedData$Qc<-mergedData$oil_station_zhuqi+mergedData$stops_output
mergedData$Qs<-mergedData$self_used+mergedData$line_self_used
mergedData$dtQ<-mergedData$Qr+mergedData$init_store-(mergedData$Qc+mergedData$end_store+mergedData$Qs)
mergedData$N<-mergedData$dtQ/(mergedData$Qr-(mergedData$end_store-mergedData$init_store))
mergedData$date<-as.Date(mergedData$date)
write.csv(mergedData, paste(datapath, "tmp/", "mergedData.csv", sep=""),row.names=FALSE)
plot(x=mergedData$date, y=mergedData$dtQ, type="l")
