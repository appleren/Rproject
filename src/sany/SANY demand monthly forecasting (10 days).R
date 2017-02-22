# Spare Part Demand Forecasting
# @Author: Chunhua Tian
# @Ver: July 13, 2015
options(warn=-1)
library(forecast)
library(EMD)
library(rpart)
library(Rssa)
library(plyr)

#==========================================================
#     Evaluation
#==========================================================
calRSquare<-function(prd_err, original) {
  SST<-sum((original-mean(original))^2)
  SSE<-sum(prd_err^2)  
  rsquare<-1-SSE/SST 
}
calMAE<-function(prd_err, original, line) {
  mean(abs(prd_err)/pmax(line,original))
}
calAccur<-function(prd_err, original,line) {
  sum(abs(prd_err)<=pmax(line,original*0.2))/length(original)
}
pred_evaluation<-function(y_original,pred_comb, method="ts", fcst_num, id, title, subtitle, line){ 
  pred_error<-y_original-pred_comb
  mae<-calMAE(pred_error, y_original, line)
  accuracy<-calAccur(pred_error, y_original, line)
  Rsquare<-calRSquare(pred_error, y_original)
  mae1<-calMAE(pred_error[-(length(pred_error)-fcst_num):0], 
               y_original[-(length(y_original)-fcst_num):0], line)
  accuracy1<-calAccur(pred_error[-(length(pred_error)-fcst_num):0], 
                      y_original[-(length(y_original)-fcst_num):0], line)
  Rsquare1<-calRSquare(pred_error[-(length(pred_error)-fcst_num):0], 
                       y_original[-(length(y_original)-fcst_num):0])
  
  plot(1:length(y_original),y_original,type="l", xlab="时间点", ylab="销量", sub=subtitle,
       main=paste(title,"m=",format(mae1,digits=2), ",a=",format(accuracy1,digits=2),
                  ",r=", format(Rsquare1,digits=2),
                  ",(",method,"):",id, "(M=",format(mae,digits=2),
                  ",A=",format(accuracy,digits=2),
                  ",R=", format(Rsquare,digits=2),")",sep=""))
  lines(1:length(y_original),pred_comb,col="red")
  grid()
  legend("topleft",inset=.05,legend=c("真实值","预测值"),lty=c(1,1),col=c("black","red"))
  result<-data.frame(MAPE_10=mae,Accuracy_10=accuracy,Rsquare=Rsquare, MAPE_T=mae1, 
                     Accuracy_T=accuracy1, Rsquare_T=Rsquare1)
  colnames(result)<-c(paste(title, "M", sep="_"), paste(title, "A", sep="_"), 
                      paste(title, "R", sep="_"), paste(title, "m", sep="_"), 
                      paste(title, "a", sep="_"), paste(title, "r", sep="_"))
  result 
}

pred_evaluationM<-function(y_original,pred_comb,method="ts", fcst_num, id, title, subtitle, line){ 
  # monthly comparison
  tmp<-y_original$order_qty
  dim(tmp)<-c(3,length(tmp)/3)
  y_month<-colSums(tmp)
  
  tmp<-pred_comb
  dim(tmp)<-c(3,length(tmp)/3)
  p_month<-colSums(tmp)
  
  pred_evaluation(y_original=y_month, pred_comb=p_month, method=method, fcst_num=fcst_num/3, id=id, title=title, subtitle=subtitle, line=line*3)
}

training_STL_ARIMA<-function(trainingData, companyID="", freq){
  y<-trainingData    #just because of legacy naming
  y_ts<-ts(y$order_qty,frequency=freq)
  predModel_trend_stl<-stlm(y_ts,s.window=7)
  remainder<-predModel_trend_stl$stl$time.series[,"remainder"] 
  remainder_ts<-ts(remainder,frequency=freq)
  predModel_remain_arima<-auto.arima(remainder_ts)
  
  return(list(trend=predModel_trend_stl,remain=predModel_remain_arima))  
}

scoring_STL_ARIMA<-function(predModelList_STL, fcstLength, outType="All"){
  
  predModel_trend_stl<-predModelList_STL[["trend"]]
  predModel_remain_arima<-predModelList_STL[["remain"]]
  
  # fitting result over training dataset 
  
  fitValue_stl_arima<-fitted(predModel_trend_stl$model)+predModel_trend_stl$stl$time.series[,"seasonal"]
  fitValue_stl_arima<-fitValue_stl_arima+fitted(predModel_remain_arima)
  
  if (outType=="Train"){
    return(list(predValue=pmax(0,round(fitValue_stl_arima))))    
  }
  
  #prediction result over the testing dataset
  predValue_stl_arima<-forecast(predModel_trend_stl,h=fcstLength)$mean+forecast(predModel_remain_arima,h=fcstLength)$mean
  
  
  if (outType=="Test"){    
    return(list(predValue=pmax(0,round(predValue_stl_arima))))    
  } else {
    pred_comb_stl_arima<-round(cbind(t(fitValue_stl_arima),t(predValue_stl_arima)))
    pred_comb_stl_arima<-pmax(pred_comb_stl_arima,0)        
    return(list(predValue=pred_comb_stl_arima))
  }  
}

training_STL_ARIMA2<-function(trainingData, companyID="", freq, years){
  
  y<-trainingData    #just because of legacy naming
  y_ts<-ts(y$order_qty,frequency=freq)
  predModel_trend_stl<-stlm(y_ts,s.window=7)
  seasonal<-predModel_trend_stl$stl$time.series[,"seasonal"] 
  remainder<-predModel_trend_stl$stl$time.series[,"remainder"] 
  remainder_ts<-ts(remainder,frequency=freq)
  
  l<-list()
  for(i in 1:length(years)) {#use 1 or 3 reg
   l[[paste("f",(1+(i-1)*3))]]<-data.frame(1*(seq(remainder_ts)==years[i]-1))
   l[[paste("f",(2+(i-1)*3))]]<-data.frame(1*(seq(remainder_ts)==years[i]))
   l[[paste("f",(3+(i-1)*3))]]<-data.frame(1*(seq(remainder_ts)==years[i]+1))
  }
  xreg<-do.call(cbind, l)
  
#   before<-at<-after<-numeric(length(remainder_ts))
#   before[years-1]<-1
#   at[years]<-1
#   after[years+1]<-1
#   xreg<-data.frame(before, at, after)
  
  predModel_remain_arima<-auto.arima(remainder_ts, xreg=xreg)
  
  return(list(trend=predModel_trend_stl,remain=predModel_remain_arima))  
}
scoring_STL_ARIMA2<-function(predModelList_STL, fcstLength, outType="All", testRound, years, weights) {
  if(length(years)>1) { #do not support forecasting more than one year
    return
  }
  
  predModel_trend_stl<-predModelList_STL[["trend"]]
  predModel_remain_arima<-predModelList_STL[["remain"]]
  
  # fitting result over training dataset 
  xreg<-NULL
  if(length(predModel_remain_arima$xreg)>0){
    l<-list()
    print("forecast with xreg")
    for(i in 1:length(weights)) {#use 1 or 3 reg
     l[[paste("f",(1+(i-1)*3))]]<-(seq(1:fcstLength)==years-(testRound-1)*fcstLength-1)
     l[[paste("f",(2+(i-1)*3))]]<-(seq(1:fcstLength)==years-(testRound-1)*fcstLength)
     l[[paste("f",(3+(i-1)*3))]]<-(seq(1:fcstLength)==years-(testRound-1)*fcstLength+1)
    }
    xreg<-do.call(cbind, l)
    
#     before<-at<-after<-numeric(fcstLength)
#     tmp<-years-(testRound-1)*fcstLength
#     
#     if((tmp-1)>0 & (tmp-1)<=fcstLength)  before[tmp-1]<-1
#     if(tmp>0 & tmp<=fcstLength)          at[tmp]<-1
#     if((tmp+1)>0 & (tmp+1)<=fcstLength)  after[tmp+1]<-1
#     xreg<-data.frame(before, at, after)
  }
  
  fitValue_stl_arima<-fitted(predModel_trend_stl$model)+predModel_trend_stl$stl$time.series[,"seasonal"]
  fitValue_stl_arima<-fitValue_stl_arima+fitted(predModel_remain_arima)
  
  if (outType=="Train"){
    return(list(predValue=pmax(0,round(fitValue_stl_arima))))    
  }
  
  #prediction result over the testing dataset
  predValue_stl_arima<-forecast(predModel_trend_stl,h=fcstLength)$mean+forecast(predModel_remain_arima,h=fcstLength,xreg=xreg)$mean
  
  
  if (outType=="Test"){    
    return(list(predValue=pmax(0,round(predValue_stl_arima))))    
  } else {
    pred_comb_stl_arima<-round(cbind(t(fitValue_stl_arima),t(predValue_stl_arima)))
    pred_comb_stl_arima<-pmax(pred_comb_stl_arima,0)        
    return(list(predValue=pred_comb_stl_arima))
  } 
}

training_SSA<-function(trainingData, companyID="", freq){
  y<-trainingData
  s1<-ssa(y$order_qty,L=3)
  res1<-reconstruct(s1,groups=list(1))
  res.trend<-residuals(res1)
  trend<-res1$F1
  s2<-ssa(res.trend,L=freq)
  
  return(list(trend=s1,remain=s2))  
}

scoring_SSA<-function(predModelList_SSA, fcstLength){
  s1<-predModelList_SSA$trend
  s2<-predModelList_SSA$remain
  
  if(sum(s1$sigma)==0){
    print("SSA trend abnormal: all sigma value are 0")
    return(list(predValue=(0*c(1:fcstLength))))
  } 
  tryCatch({
    trend<-forecast(s1,groups=list(1),len=fcstLength,only.new=FALSE)$mean
  }, error=function(e){
    print("SSA trend reconstruction abnormal")
  })
  
  if (!exists("trend")){
    print("SSA trend abnormal: return 0")
    return(list(predValue=(0*c(1:fcstLength))))
  }
  
  if (s2$sigma[3]==0){
    pred<- pmax(0,round(trend))
    return(list(predValue=pred))
  }
  
  tryCatch({
    remain<-forecast(s2,groups=list(1:9),len=fcstLength,only.new=FALSE)$mean
  }, error = function ( e ) {
    print("SSA remina forecast reduced to 3 components")
  })
  
  if (!exists("remain")){
    tryCatch({
      remain<-forecast(s2,groups=list(1:3),len=fcstLength,only.new=FALSE)$mean
    }, error = function ( e ) {
      print("SSA remina forecast using 3 components still does not work")      
    })
  }
  
  if (!exists("remain")){
    pred<- pmax(0,round(trend))
    return(list(predValue=pred))
  }
  
  pred<- pmax(0,round(trend+remain))
  return(list(predValue=pred))   
}

training_STL_RPART<-function(trainingData, companyID="", freq, wholeData){
  y<-trainingData    #just because of legacy naming
  y_ts<-ts(y$order_qty,frequency=freq)
  predModel_trend_stl<-stlm(y_ts,s.window=7)
  remainder<-predModel_trend_stl$stl$time.series[,"remainder"] 
  if(length(y$decad)>0){
    remainder_ts<-ts(cbind(order_qty=remainder, year=y$year, month=y$month, decad=y$decad),frequency=freq)
    predModel_remain_rpart<-rpart(order_qty~year+month+decad, remainder_ts, method="anova")
  } else{
    remainder_ts<-ts(cbind(order_qty=remainder, year=y$year, month=y$month),frequency=freq)
    predModel_remain_rpart<-rpart(order_qty~year+month, remainder_ts, method="anova")
  }
  return(list(trend=predModel_trend_stl,remain=predModel_remain_rpart, trainData=remainder_ts))  
}

scoring_STL_RPART<-function(predModelList_STL, fcstLength, outType="All", wholeData){
  
  predModel_trend_stl<-predModelList_STL[["trend"]]
  predModel_remain_rpart<-predModelList_STL[["remain"]]
  predModel_train_data<-predModelList_STL[["trainData"]]
  
  # fitting result over training dataset 
  
  fitValue_stl_rpart<-fitted(predModel_trend_stl$model)+predModel_trend_stl$stl$time.series[,"seasonal"]
  fitValue_stl_rpart<-fitValue_stl_rpart+predict(predModel_remain_rpart,newdata=predModel_train_data )
  
  
  if (outType=="Train"){
    return(list(predValue=pmax(0,round(fitValue_stl_rpart))))    
  }
  
  #prediction result over the testing dataset
  #predValue_stl_rpart<-forecast(predModel_trend_stl,h=fcstLength)$mean+forecast(predModel_remain_rpart,h=fcstLength)$mean
  predModel_test_trend<-predict(predModel_trend_stl, h=fcstLength)$mean
  predmodel_test_data<-wholeData[(fcstLength-length(wholeData[,1])):0,]
  predValue_stl_rpart<-predModel_test_trend+predict(predModel_remain_rpart,newdata=predmodel_test_data )
  
  if (outType=="Test"){    
    return(list(predValue=pmax(0,round(predValue_stl_rpart))))    
  } else {
    pred_comb_stl_rpart<-round(cbind(t(fitValue_stl_rpart),t(predValue_stl_rpart)))
    pred_comb_stl_rpart<-pmax(pred_comb_stl_rpart,0)        
    return(list(predValue=pred_comb_stl_rpart))
  }  
}


#------------------------------------------------------------------

demand_forecast<-function(dp) {
  if(class(dp)!="DataAndParams")
    stop("Wrong parameter type: dp must be a DataAndParams object")
  #data and parameters
  x<-dp$data
  toPDF<-dp$toPDF
  calMonth<-dp$calMonth
  subtitle<-dp$subtitle
  provinces<-dp$provinces
  
  fcstLength<-dp$fcstLength
  freq<-dp$freq
  testH<-dp$testH
  trainH<-dp$trainH
  years<-dp$years
  train_year<-dp$train_year
  test_year<-dp$test_year
  accLine<-dp$accLine
  file_metrics<-dp$file_metrics
  pdfFile<-dp$pdfFile
  rollForecast<-dp$rollForecast
  
  testing_result<-data.frame()
  fcst_metrics<-data.frame()
  
  if(toPDF) pdf(pdfFile,family="GB1")
  for (id in provinces){
    if (nchar(id)>2 | id=="青海") next
    y<-oy<-subset(x,province==id) 
    
    # Split training/testing dataset
    # forecasting horizon 
    fcstNum=round(testH/fcstLength)
    y_original<-y
    # currenly Year 2015 data is used for testing, other data for training
    for (testRound in 1:fcstNum){    
      y_test<-y_original[(trainH+fcstLength*(testRound-1)+1):(trainH+fcstLength*testRound),]
      y<-y_original[1:(trainH+fcstLength*(testRound-1)),]
      y_whole<-y_original[1:(trainH+fcstLength*testRound),]
      
      print(paste("now train over 1:", dim(y)[1],", test over",dim(y_test)[1]," cases",sep=""))
      
      # =====================================================
      #              Training
      # =====================================================
      
      # Method 3: STL directly
      predModelList_STL<-training_STL_ARIMA(y,id,freq)
      predModelList_STL2<-training_STL_RPART(y,id,freq)
      
      # Method 4: SSA method 
      predModelList_SSA<-training_SSA(y,id,freq)
      
      # =====================================================
      #              Testing
      # =====================================================
      pred_comb_stl_arima<-scoring_STL_ARIMA(predModelList_STL, fcstLength)
      pred_comb_stl_arima2<-scoring_STL_RPART(predModelList_STL2, fcstLength, wholeData=y_original)
      
      predResult<-pred_comb_stl_arima[[1]]
      predResult<-rbind(predResult,pred_comb_stl_arima2[[1]])
      
      pred_comb_SSA<-scoring_SSA(predModelList_SSA,fcstLength)
      predResult<-rbind(predResult,pred_comb_SSA[[1]])
      
      if (testRound==1){
        predResult_Final<-predResult     
      } else{
        predResult_Final<-cbind(predResult_Final,predResult[,(trainH+fcstLength*(testRound-1)+1):(trainH+fcstLength*testRound)])     
      } 
      
    }
    
    methodVec<-c("STL+ARIMA", "RPART", "SSA")
    
    par(mfrow=c(length(methodVec),1))
    n<-length(methodVec)
    fcst_metrics2<-data.frame()
    for (i in 1:length(methodVec)){
      test_rec<-predResult_Final[i,]
      if(calMonth)
        tmp<-pred_evaluation(oy$order_qty, test_rec, fcst_num=testH, method=methodVec[i], id=id, title="月销量预测", subtitle=subtitle, line=accLine)
      else
        tmp<-pred_evaluation(oy$order_qty, test_rec, fcst_num=testH, method=methodVec[i], id=id, title="旬销量预测", subtitle=subtitle, line=accLine)
      tmp$id<-id
      tmp$method<-methodVec[i]
      fcst_metrics2<-rbind(fcst_metrics2, tmp)
    }
    
    if(!calMonth) {
      fcst_metrics1<-data.frame()
      
      for (i in 1:length(methodVec)){
        test_rec<-predResult_Final[i,]
        tmp<-pred_evaluationM(oy,test_rec, fcst_num=testH, method=methodVec[i], id=id, title="月销量预测", subtitle=subtitle, line=accLine)
        tmp$id<-id
        tmp$method<-methodVec[i]
        fcst_metrics1<-rbind(fcst_metrics1, tmp) 
      }
      fcst_metrics2<-merge(fcst_metrics2, fcst_metrics1, by=c("id", "method"), all=TRUE)
    }
    fcst_metrics<-rbind(fcst_metrics, fcst_metrics2)
  } 
  if(toPDF) dev.off()
  write.csv(fcst_metrics,file=file_metrics)
}

getYears<-function(x, calMonth){
  if(!calMonth) years<-unique(x$seq[which(x$month==1 & x$decad==1)])
  else years<-unique(x$seq[which(x$month==1)])
  years
}
setSeq<-function(x){
  ddply(x, .(province), function(d){
    d$seq<-1:length(d[,1])
    d
  })
}

######
# provinces: Use this to run some of the provinces or get a better order in result. e.g. clustering.
#            If provinces is set to NULL, unique(x$province) will be used.
######
getDataAndParams<-function(x, toPDF, cutYear, calMonth, workDir, 
                           subtitle, provinces=NULL, remove2011, rollForecast) {
  r<-list()
  r$toPDF<-toPDF
  r$workDir<-workDir
  r$cutYear<-cutYear
  r$calMonth<-calMonth
  r$subtitle<-paste(subtitle, "从2011年9月开始")
  r$rollForecast<-rollForecast
  
  mid_filename<-""
  if(remove2011){
    x<-subset(x, year!=2011)
    x$seq<-x$seq-12
    years<-which(x$seq%in%getYears(x, FALSE))
    mid_filename<-"remove2011 "
    r$subtitle<-paste(subtitle, "从2012年1月开始")
  }
  if(cutYear) {
    for(year in years) {
      if((year+5)<=length(x$order_qty))
        x$order_qty[year:(year+5)]<-round(mean(x$order_qty[year:(year+5)]))*2
      else
        x$order_qty[year:length(x$order_qty)]<-round(mean(x$order_qty[year:length(x$order_qty)]))*2
    }
    x<-subset(x, month!=2)
    x<-setSeq(x)
  }
  if(!calMonth){
    if(rollForecast) r$fcstLength<-3
    else r$fcstLength<-15
    r$accLine<-5
    if(cutYear) {
      r$freq<-33
      r$file_metrics<-paste(workDir,mid_filename,"fcst_decad Metrics cutYear.csv",sep="")
      r$pdfFile<-paste(workDir,mid_filename,"fcst_decad cutYear.pdf",sep="")
    }
    else {
      r$freq<-36
      r$file_metrics<-paste(workDir,"fcst_decad Metrics.csv",sep="")
      r$pdfFile<-paste(workDir,"fcst_decad.pdf",sep="")
    }
  } else {
    x<-aggregate(order_qty~month+year+province, x, sum)
    x<-setSeq(x)
    if(rollForecast) r$fcstLength<-1
    else r$fcstLength<-5
    r$accLine<-15
    if(cutYear) {
      r$freq<-11
      r$file_metrics<-paste(workDir,"fcst_month Metrics cutYear.csv",sep="")
      r$pdfFile<-paste(workDir,"fcst_month cutYear.pdf",sep="")
    }
    else {
      r$freq<-12
      r$file_metrics<-paste(workDir,"fcst_month Metrics.csv",sep="")
      r$pdfFile<-paste(workDir,"fcst_month.pdf",sep="")
    }
  }
  
  r$data<-x
  r$provinces<-provinces
  if(length(provinces)==0) r$provinces<-unique(x$province)
  r$years<-getYears(x, calMonth)
  firstX<-x[q<-unique(x$seq),]
  r$testH<-length(which(firstX$year==2015)==TRUE)
  r$trainH<-length(firstX$seq)-r$testH
  r$train_year<-subset(r$years, r$years<=r$trainH)
  r$test_year<-subset(r$years, r$years>r$trainH)
  
  class(r)<-"DataAndParams"
  r
}

#########
# grp: Use this groupID to run against one or more group.
#      If grp is set to NULL, unique(gs$part_group) will be used
sanyDemandForecast<-function(toPDF, cutYear, calMonth, workDir, grp=NULL, remove2011, provinces=NULL) {
  gs<-read.csv(paste(workDir, "/seqGroupSale.csv",sep=""))
  if(length(grp)>0) 
    groups<-grp
  else
    groups<-unique(gs$part_group)
  for(group in groups){
    groupName<-gs$part_groupName[gs$part_group==group][1]
    path<-paste(workDir, "/", group, "/", sep="")
    if(!dir.exists(path))
      dir.create(path, mode = "0777")
    decadSale<-subset(gs,part_group==group)
    dp<-getDataAndParams(decadSale, toPDF=toPDF, cutYear=cutYear, calMonth=calMonth, workDir=path, provinces=provinces, subtitle=groupName, remove2011=remove2011, rollForecast=FALSE)
    
    demand_forecast(dp)
  }
}

#----run------------
workDir<-getwd()
path<-paste(workDir,"/../../data/sany/mid",sep="")

# sanyDemandForecast(toPDF=FALSE, cutYear=TRUE, calMonth=TRUE, workDir=path, remove2011=TRUE, grp=c(4,10), provinces="北京")

sanyDemandForecast(toPDF=FALSE, cutYear=TRUE, calMonth=TRUE, workDir=path, remove2011=TRUE, grp=4, provinces="北京")

