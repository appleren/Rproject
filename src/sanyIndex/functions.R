removeSeasonal<-function(ts, freq, rt=c("s", "t", "e"), paint=TRUE, title="") {
  ts<-ts(ts, frequency=freq)
  st<-stl(ts,s.window="periodic")
  if(paint) plot(st, main=title)
  t<-st$time.series[,"trend"]
  s<-st$time.series[,"seasonal"]
  e<-st$time.series[,"remainder"]
  r<-t+e
  if(length(rt)>0) {
    r<-rep(0, from=1, to=length(t))
    for(c in rt){
      if(c=="s") r<-r+s
      if(c=="t") r<-r+t
      if(c=="e") r<-r+e
    }
  }
  r
}

removeSeasonalDF<-function(df, freq, id.vars=NULL, rt=c("s", "t", "e")){
  cols<-NULL
  if(length(id.vars)>0) {
    cols<-subset(df, select=id.vars)
    df<-subset(df, select=!(colnames(df)%in%id.vars))
  }
  for(i in 1:length(df[1,])) {
    df[,i]<-removeSeasonal(df[,i], freq, rt=rt, title=names(df)[i], paint=FALSE)
  }
  if (length(cols)>0) df<-cbind(cols, df)
  df
}

standardization<-function(data) {
  min<-min(data)
  max<-max(data)
  r<-(data-min)/(max-min)
  r
}

standardizationDF<-function(df, id.vars=NULL) {
  cols<-NULL
  if(length(id.vars)>0) {
    cols<-subset(df, select=id.vars)
    df<-subset(df, select=!(colnames(df)%in%id.vars))
  }
  for(i in 1:length(df[1,])) {
    df[,i]<-standardization(df[,i])
  }
  if (length(cols)>0) df<-cbind(cols, df)
  df
}

calmcor<-function(gdp, gk, filename, writeFile=TRUE){
  mcor<-cor(gdp,gk,use="pairwise.complete.obs", method="spearman")
  tmp<-as.data.frame(mcor)
  tmp<-cbind(gdp=rownames(tmp), tmp)
  if(writeFile==TRUE) {
    write.csv(tmp,filename)
  }
  tmp
}

calCCF<-function(gdp, gk, filename, writeFile=TRUE) {
  ccfInfo<-data.frame()
  for (i in 1:length(gdp[1,])){
    iName<-names(gdp)[i]
    #     print(paste("i=",i))
    if(length(dim(gk))==0){
      jName<-"gk"
      tmp<-ccf(gdp[,i],gk,lag.max = 6, type = "correlation",na.action=na.pass,plot=FALSE)
      ccfVal<-unlist(tmp[[1]])
      ccfIndex<-unlist(tmp[[4]])
      if (all(is.na(ccfVal))) next
      ccfVal[is.na(ccfVal)]<-0
      ccfInfo1<-cbind(index1=rep(iName, times=length(ccfVal)), index2=rep(jName, times=length(ccfVal)), Lag=ccfIndex, ccfVal=ccfVal)
      ccfInfo<-rbind(ccfInfo, ccfInfo1)
#       k<-which(abs(ccfVal)==max(abs(ccfVal)))    
#       ccfInfo<-rbind(ccfInfo,data.frame(Index1=iName,Index2=jName,Lag=ccfIndex[k],ccfVal=ccfVal[k]))
    } else{
      for (j in 1:length(gk[1,])){
        jName<-names(gk)[j]
        #       print(paste("j=",j))
        tmp<-ccf(gdp[,i],gk[,j],lag.max = 6, type = "correlation",na.action=na.pass,plot=FALSE)
        ccfVal<-unlist(tmp[[1]])
        ccfIndex<-unlist(tmp[[4]])
        if (all(is.na(ccfVal))) next
        ccfVal[is.na(ccfVal)]<-0
        ccfInfo1<-cbind(index1=rep(iName, times=length(ccfVal)), index2=rep(jName, times=length(ccfVal)), Lag=ccfIndex, ccfVal=ccfVal)
        ccfInfo<-rbind(ccfInfo, ccfInfo1)
#         k<-which(abs(ccfVal)==max(abs(ccfVal)))    
#         ccfInfo<-rbind(ccfInfo,data.frame(Index1=iName,Index2=jName,Lag=ccfIndex[k],ccfVal=ccfVal[k]))
      }
    }
  }
  
  if(writeFile==TRUE) {
    write.csv(ccfInfo,filename)
  }
  
  ccfInfo
}