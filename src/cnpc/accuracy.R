calMAE<-function(prd_err, original, line) {
  if(line==0) {
    prd_err<-prd_err[which(original!=0)]
    original<-original[which(original!=0)]
  }
  format(mean(abs(prd_err)/pmax(line,original)),digits=2)
}
calAccur<-function(prd_err, original,line) {
  format(sum(abs(prd_err)<=pmax(line,original*0.1))/length(original), digits=2)
}
calAccur2<-function(p, original, line) {
  prd_err<-original-p
  format(1-(sum(abs(prd_err/pmax(p, original, 1)))/length(p)), digits=2)
}

calnnet<-function(wholeData, func, original, title){
  n<-length(wholeData[,1])
  trainLength<-(length(wholeData[,1])*80)%/%100
  
  nn<-nnet(func, wholeData[1:trainLength,], size=10, decay=0.0001, maxit=10000, linout=T, trace=F, MaxNWts=4000, na.action=na.omit)
  p<-predict(nn, wholeData)
  sdv<-sd(original)
  meanv<-mean(original) 
  p<-p*sdv + meanv
  pame<-calMAE(original[(trainLength+1):n]-p[(trainLength+1):n,1], original[(trainLength+1):n], 0)
  acc<-calAccur(original[(trainLength+1):n]-p[(trainLength+1):n,1], original[(trainLength+1):n], 0)
#   acc<-calAccur2(p[(trainLength+1):n,1], original[(trainLength+1):n], 0)
  plot(original, type="l", ylim=c(min(original, p), max(fd$all_p, p)), main=paste(title, "MAPE=",pame, ", accuracy=",acc, sep=""))
  lines(p, col="red")
}