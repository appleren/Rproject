source("newshucha_header.R")

#===============================陕京管道生产运行日报表-进气注采气=========================

filenames<-list.files(path=paste(datapath, "陕京管道生产运行日报表-进气注采气/", sep=""), pattern=".xls")

# filename<-filenames[1]

for(filename in filenames) {
  dateString<-substr(filename, 
                     nchar("中石油北京天然气管道有限公司_陕京管道生产运行日报表-进气、注采气_")+1, 
                     nchar(filename)-4)
  d<-as.Date(ISOdate(substr(dateString, 1, 4), substr(dateString, 5,6), substr(dateString, 7,8)))
  
  xls <- loadWorkbook(paste(datapath, "陕京管道生产运行日报表-进气注采气/", filename, sep=""),create=FALSE)
  
  #--------------oil_field_input----------------#
  s<-readWorksheet(xls, "Sheet1", region="a5:g12", header=FALSE)
  s<-na.locf(s)
  s$date<-rep(d, length(s[,1]))
  oil_field_input<-rbind(oil_field_input, s)
  s<-readWorksheet(xls, "Sheet1", region="a13:g13", header=FALSE)
  s$date<-rep(d, length(s[,1]))
  oil_field_input<-rbind(oil_field_input, s)
  
  s<-readWorksheet(xls, "Sheet1", region="a14:g15", header=FALSE)
  s<-na.locf(s)
  s$date<-rep(d, length(s[,1]))
  oil_field_input<-rbind(oil_field_input, s)
  
  s<-readWorksheet(xls, "Sheet1", region="a16:g24", header=FALSE)
  s$date<-rep(d, length(s[,1]))
  oil_field_input<-rbind(oil_field_input, s)

  #-------------------oil_station_caiqi-----------
  s<-readWorksheet(xls, "Sheet1", region="a26:g37", header=FALSE)
  s<-na.locf(s)
  s$date<-rep(d, length(s[,1]))
  oil_station_caiqi<-rbind(oil_station_caiqi, s)
  
  #--------------------oil_station_zhuqi-----------
  s<-readWorksheet(xls, "Sheet1", region="a38:g49", header=FALSE)
  s<-na.locf(s)
  s$date<-rep(d, length(s[,1]))
  oil_station_zhuqi<-rbind(oil_station_zhuqi, s)
  
  #----------------------sub_line_input---------------
  s<-readWorksheet(xls, "Sheet1", region="a50:g52", header=FALSE)
  s<-subset(s, select=c(-2, -3))
  s$date<-rep(d, length(s[,1]))
  sub_line_input<-rbind(sub_line_input, s)
  
  #-------------------self_used---------------------
  s<-readWorksheet(xls, "Sheet1", region="a74:g74", header=FALSE)
  s<-subset(s, select=c(-2, -3))
  s$date<-rep(d, length(s[,1]))
  self_used<-rbind(self_used, s)
  
  s<-readWorksheet(xls, "Sheet1", region="a77:f83", header=FALSE)
  s<-subset(s, select=c(-2, -3))
  s$date<-rep(d, length(s[,1]))
  pipe_store<-rbind(pipe_store, s)
}

colnames(oil_field_input)[1:length(oil_field_input)-1]<-oil_field_input_colnames
colnames(pipe_store)[1:length(pipe_store)-1]<-pipe_store_colnames
colnames(self_used)[1:length(self_used)-1]<-self_used_colnames
colnames(sub_line_input)[1:length(sub_line_input)-1]<-sub_line_input_colnames
colnames(oil_station_zhuqi)[1:length(oil_station_zhuqi)-1]<-oil_station_colnames
colnames(oil_station_caiqi)[1:length(oil_station_caiqi)-1]<-oil_station_colnames
# oil_field_input1<-oil_field_input
# oil_field_input<-oil_field_input1
# oil_field_input$station_name[which(is.na(oil_field_input$station_name))]<-oil_field_input$line_no[which(is.na(oil_field_input$station_name))]
# oil_field_input$station_name[which(is.na(oil_field_input$station_name))]<-oil_field_input$oil_field[which(is.na(oil_field_input$station_name))]

#-------------------------------write to files----------------------------------
write.csv(oil_field_input, paste(datapath, 
          "mid/陕京管道生产运行日报表-进气注采气/", "oil_field_input.csv", sep=""),
          row.names=FALSE)
write.csv(pipe_store, paste(datapath, 
          "mid/陕京管道生产运行日报表-进气注采气/", "pipe_store.csv", sep=""),
          row.names=FALSE)
write.csv(self_used, paste(datapath, 
          "mid/陕京管道生产运行日报表-进气注采气/", "self_used.csv", sep=""),
          row.names=FALSE)
write.csv(sub_line_input, paste(datapath, 
          "mid/陕京管道生产运行日报表-进气注采气/", "sub_line_input.csv", sep=""),
          row.names=FALSE)
write.csv(oil_station_zhuqi, paste(datapath, 
          "mid/陕京管道生产运行日报表-进气注采气/", "oil_station_zhuqi.csv", sep=""),
          row.names=FALSE)
write.csv(oil_station_caiqi, paste(datapath, 
          "mid/陕京管道生产运行日报表-进气注采气/", "oil_station_caiqi.csv", sep=""),
          row.names=FALSE)


#======================陕京管道生产运行日报表-销气=======
filenames<-list.files(path=paste(datapath, "陕京管道生产运行日报表-销气/", sep=""), pattern=".xls")

# filename<-filenames[1]

for(filename in filenames) {
  dateString<-substr(filename, 
                     nchar("中石油北京天然气管道有限公司_陕京管道生产运行日报表-销气_")+1, 
                     nchar(filename)-4)
  d<-as.Date(ISOdate(substr(dateString, 1, 4), substr(dateString, 5,6), substr(dateString, 7,8)))
  
  xls <- loadWorkbook(paste(datapath, "陕京管道生产运行日报表-销气/", filename, sep=""),create=FALSE)
  
  #--------------oil_field_input----------------#
  s<-readWorksheet(xls, "Sheet1", region="a5:i110", header=FALSE)
  colnames(s)<-stops_output_columns
  s$province<-na.locf(s$province)
  s$user_name<-na.locf(s$user_name)
  
  s$date<-rep(d, length(s[,1]))
  stops_output<-rbind(stops_output, s)
}

write.csv(stops_output, paste(datapath, 
          "mid/陕京管道生产运行日报表-销气/", "stops_output.csv", sep=""),
          row.names=FALSE)

#====================陕京输气线自耗气情况表=========
filenames<-list.files(path=paste(datapath, "陕京输气线自耗气情况表/", sep=""), pattern=".xls")

# filename<-filenames[1]
for(filename in filenames) {
  dateString<-substr(filename, 
                     nchar("中石油北京天然气管道有限公司_陕京输气线自耗气情况表_")+1, 
                     nchar(filename)-4)
  d<-as.Date(ISOdate(substr(dateString, 1, 4), substr(dateString, 5,6), substr(dateString, 7,8)))
  
  xls <- loadWorkbook(paste(datapath, "陕京输气线自耗气情况表/", filename, sep=""),create=FALSE)
  
  #--------------oil_field_input----------------#
  s<-readWorksheet(xls, "财务用表", region="a4:e45", header=FALSE)
  colnames(s)<-line_self_used_columns
  s$line_type<-na.locf(s$line_type)
  s$belongs_to<-na.locf(s$belongs_to)
  s$date<-rep(d, length(s[,1]))
  line_self_used<-rbind(line_self_used, s)
}

write.csv(line_self_used, paste(datapath, 
          "mid/陕京输气线自耗气情况表/", "line_self_used.csv", sep=""),
          row.names=FALSE)