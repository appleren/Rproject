# install.packages('RMySQL',type='source')

library(forecast)
library(plyr)
library(pmml)

workdir<-"../../data/dmdfcst/"
dmdvalue<-read.csv(paste(workdir, "demandvalue.csv", sep=""), header=TRUE)
y<-subset(dmdvalue, PART_NO==4&AREA=="ÉÏº£")

result<-ddply(dmdvalue, .(MODEL_ENTITY_NAME), function(d){
  if(nchar(as.character(d$MODEL_ENTITY_NAME[1]))>0){
    ts<-ts(d$VVALUE, frequency=12)
    a<-auto.arima(ts)
    fit<-fitted(a)
    fcst<-forecast(a, h=6)
    m<-as.numeric(fcst$mean)
    h<-as.numeric(fcst$upper[,2])
    l<-as.numeric(fcst$lower[,2])
    r<-data.frame(result=m, higher=h, lower=l, is_fitted=rep(FALSE, times=6), 
                  MODEL_ENTITY_NAME=rep(d$MODEL_ENTITY_NAME[1], times=6),
                  PART_NO=rep(d$PART_NO[1], times=6),
                  PART_NAME=rep(d$PART_NAME[1], times=6),
                  AREA=rep(d$AREA[1], times=6),
                  PERIOD_NO=rep(d$PERIOD_NO[1], times=6),
                  PERIOD_LABEL=rep(d$PERIOD_LABEL[1], times=6))
    ll<-length(fit)
    r2<-data.frame(result=fit,higher=rep(NA, times=ll), 
                  lower=rep(NA, times=ll),
                  is_fitted=rep(TRUE, times=ll), 
                  MODEL_ENTITY_NAME=rep(d$MODEL_ENTITY_NAME[1], times=ll),
                  PART_NO=rep(d$PART_NO[1], times=ll),
                  PART_NAME=rep(d$PART_NAME[1], times=ll),
                  AREA=rep(d$AREA[1], times=ll),
                  PERIOD_NO=d$PERIOD_NO,
                  PERIOD_LABEL=d$PERIOD_LABEL)
    rr<-rbind(r, r2)
    rr
  }
})

write.csv(result, paste(workdir, "result.csv", sep=""))
