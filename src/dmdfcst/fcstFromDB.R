initDBConn<-function(dbname, username, password) {
  conn<-dbConnect(MySQL(), dbname=dbname, username=username, password=password)
  dbSendQuery(conn, 'SET NAMES gbk')
  conn
}

readData<-function(conn, selectStmt) {
  d0<-dbGetQuery(conn, selectStmt)
  d0
}


training_STL_ARIMA<-function(trainingData, freq){
  y<-trainingData    #just because of legacy naming
  y_ts<-ts(y$vvalue,frequency=freq)
  predModel_trend_stl<-stlm(y_ts,s.window=7)
  remainder<-predModel_trend_stl$stl$time.series[,"remainder"] 
  remainder_ts<-ts(remainder,frequency=freq)
  predModel_remain_arima<-auto.arima(remainder_ts)
  
  return(list(trend=predModel_trend_stl,remain=predModel_remain_arima, data=y))  
}

getResult<-function(score_result){
  l<-length(score_result$fitValue$year)
  year<-score_result$fitValue$year[l]
  month<-score_result$fitValue$month[l]
  yml<-generateYM(year, month, length(score_result$predValue$pred))
  predVal<-data.frame(yml, predValue=as.vector(score_result$predValue$pred), 
                      lower=as.vector(score_result$predValue$lower), 
                      upper=as.vector(score_result$predValue$upper))
  return(predVal)
}

generateYM<-function(year, month, l) {
  y<-vector()
  m<-vector()
  label<-vector()
  pn<-vector()
  for(i in 1:l) {
    if(month==12){
      year<-year+1;
      month<-1
    } else {
      month=month+1
    }
    y<-rbind(y, year)
    m<-rbind(m, month)
    label<-rbind(label, paste0(year,"年",month,"月"))
    pn<-rbind(pn, i)
  }
  r<-data.frame(year=y, month=m, period_no=pn, period_lable=label, row.names=pn)
  r
}

scoring_STL_ARIMA<-function(predModelList_STL, fcstLength, outType="All"){
  
  predModel_trend_stl<-predModelList_STL[["trend"]]
  predModel_remain_arima<-predModelList_STL[["remain"]]
  train_data<-predModelList_STL[["data"]]
  
  
  fitValue_stl_arima<-fitted(predModel_trend_stl$model)+predModel_trend_stl$stl$time.series[,"seasonal"]
  fitValue_stl_arima<-fitValue_stl_arima+fitted(predModel_remain_arima)
  #prediction result over the testing dataset
  trendModel<-forecast(predModel_trend_stl,h=fcstLength)
  remainModel<-forecast(predModel_remain_arima,h=fcstLength)
  predValue_stl_arima<-trendModel$mean+remainModel$mean
  predValue_stl_arima_upper<-trendModel$upper+remainModel$upper
  predValue_stl_arima_lower<-trendModel$lower+remainModel$lower

  result<-list()
  result$fitValue<-data.frame(fitval=round(fitValue_stl_arima), period_no=train_data$period_no, 
                         period_label=train_data$period_label, real_data=train_data$vvalue,
                         year=train_data$year, month=train_data$month, area=train_data$area,
                         part_no=train_data$part_no, part_name=train_data$part_name,
                         model_entity_ID=train_data$model_entity_ID)
  result$predValue<-data.frame(pred=round(predValue_stl_arima), 
                               upper=as.data.frame(round(predValue_stl_arima_upper))[[1]], 
                               lower=as.data.frame(round(predValue_stl_arima_lower))[[1]])
  
  result$predValue<-getResult(result)
  return(result)
}

getFitValue<-function(score_result) {
  return(score_result$fitValue)
}
getPredValue<-function(score_result) {
  return(score_result$predValue)
}


#########################
# conn<-initDBConn("dmdfcst", "root", "passw0rd")
# tryCatch(ds<-readData(conn=conn, selectStmt='select * from demandvalue where area=\"北京\" and part_no=\"4\" 
#                       order by period_no'))
# stl_arima<-training_STL_ARIMA(ds, 12)
# score_result<-scoring_STL_ARIMA(stl_arima, 3)
# r<-score_result$fitValue
