library(XLConnect)
library(reshape2)
library(zoo)
library(plyr)

datapath<-"../../data/cnpc/newshucha/"
#===============================陕京管道生产运行日报表-进气注采气=========================
#气源进气量
oil_field_input<-data.frame()
oil_field_input_colnames<-c("oil_field", "line_no", "station_name", "daily_quantity", "monthly_quantity", "year_quantity", "history_quantity")
#地下储气库采气
oil_station_caiqi<-data.frame()
#地下储气库注气
oil_station_zhuqi<-data.frame()
oil_station_colnames<-c("type", "station_area", "station_name", "daily_quantity", "monthly_quantity", "year_quantity", "history_quantity")
#其他支线（地下储气库）进气
sub_line_input<-data.frame()
sub_line_input_colnames<-c("subline", "daily_quantity", "monthly_quantity", "year_quantity", "history_quantity")
#自耗
self_used<-data.frame()
self_used_colnames<-c("station", "daily_quantity", "monthly_quantity", "year_quantity", "history_quantity")
#管存
pipe_store<-data.frame()
pipe_store_colnames<-c("line", "init_store", "end_store", "changed")

#======================陕京管道生产运行日报表-销气=======
stops_output<-data.frame()
stop("hahaha")
stops_output_columns<-c("index", "line_no", "province", "stop_name",
                        "user_name", "daily_stops_output", "monthly_stops_output",
                        "year_stops_output", "all_stops_output")

#====================陕京输气线自耗气情况表=========
line_self_used<-data.frame()
line_self_used_columns<-c("line_type", "belongs_to", "line_name", "monthly_used", "year_used")
