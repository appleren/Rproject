library(reshape2)
library(zoo)
library(plyr)
library(caret)

datapath<-"../../data/cnpc/qihao/"
compressorParam<-read.csv(paste(datapath, "cz1_mid/压缩机运行参数.csv", sep=""), header=TRUE)

#cleandata
compressorParam<-subset(compressorParam, working_time!=0)
compressorParam<-subset(compressorParam, speed!=0)
compressorParam<-subset(compressorParam, com_out_pressure!=0)
compressorParam$com_gas_csmt_phour<-compressorParam$com_gas_consumption/compressorParam$working_time
compressorParam$com_pressure_diff<-compressorParam$com_out_pressure-compressorParam$com_in_pressure
compressorParam$com_pressure_ratio<-compressorParam$com_in_pressure/compressorParam$com_out_pressure
compressorParam$com_temperature_diff<-compressorParam$com_out_temperature-compressorParam$com_in_temperature

fd<-subset(compressorParam,
           select=c("com_gas_csmt_phour", "speed", "com_pressure_diff", 
                    "com_pressure_ratio", "com_temperature_diff"))
f1<-fd
inTrain<-createDataPartition(f1$speed, p=3/4, list=FALSE)
trainFd<-f1[inTrain,]
testFd<-f1[-inTrain,]

# trainClass<-fd[inTrain, 1]
# testClass<-fd[-inTrain, 1]

# prop.table(table(f1$speed))
descrCorr<-cor(f1)
highCorr<-findCorrelation(descrCorr, 0.9)
trainFd<-trainFd[, -highCorr]
testFd<-testFd[, -highCorr]
fd1<-f1[, -highCorr]
xtrans<-preProcess(fd1, method="scale")
trainFd<-predict(xtrans, trainFd)
testFd<-predict(xtrans, testFd)
bootControl<-trainControl(number=5)
set.seed(2)
nnetFit<-train(com_gas_csmt_phour~., data=trainFd, method="nnet", maxit=10000, 
               linout=T, trace=F, MaxNWts=4000, na.action=na.omit, trControl=bootControl)
rpartFit<-train(com_gas_csmt_phour~., data=trainFd, method="rpart", na.action=na.omit, trControl=bootControl)
# nnetFit$finalModel
# nn<-nnet(com_gas_csmt_phour~., trainFd, size=10, decay=0.0001, maxit=10000, linout=T, trace=F, MaxNWts=4000, na.action=na.omit)
predict(nnetFit$finalModel, newdata=testFd)[1:5]
predict(nnetFit, newdata=testFd)[1:5]
models<-list(nnet=nnetFit, rpart=rpartFit)
testPred<-predict(models, newdata=testFd)
lapply(testPred, function(x) x[1:5])

pred<-predict(nnetFit$finalModel, newdata=testFd)
predVals<-extractPrediction(list(nnet=nnetFit), testX=testFd, testY=testFd$com_gas_csmt_phour) 
# predVals1<-extractProb(list(nnet=nnetFit), testX=testFd, testY=testFd$com_gas_csmt_phour) 

testvals<-subset(predVals, dataType=="Test")
confusionMatrix(testvals$pred, testvals$obs)
