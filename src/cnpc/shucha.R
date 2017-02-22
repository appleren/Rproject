library(XLConnect)
library(reshape2)
library(zoo)

datapath<-"../../data/cnpc/shucha/"
filenames<-list.files(path=paste(datapath, "gx1/", sep=""), pattern=".xls")
pVendorIn<-data.frame() #�������
pStorageIn<-data.frame() #�������������Ӧ���뵱������
pVendorOut<-data.frame() #�ͻ�����
pStorageOut<-data.frame() #�������������Ӧ���뵱������
pSelfConsume<-data.frame() #�������� 
pPipeStore<-data.frame() #�ܴ�Vc[i]=pPipeStore[i], Vm[i]=pPipeStore[i+1]

for(filename in filenames) {
  d<-as.Date(ISOdate(substr(filename, 1, 4), substr(filename, 5,6), substr(filename, 7,8)))
  #read xls file
  xls <- loadWorkbook(paste(datapath, "gx1/", filename, sep=""),create=FALSE)
  
  #income, vendor
  s<-readWorksheet(xls, "����", region="B1:E3", header=TRUE)
  colnames(s)<-c("linename", "station", "vendor", "vendor_income")
  s$linename<-na.locf(s$linename)
  s$date<-rep(d, form=1, to=length(s$linename))
  pVendorIn<-rbind(pVendorIn, s)
  
  linename<-s$linename[1]
  
  #storage
  st<-readWorksheet(xls, "����", region="B5:E6", header=TRUE)
  colnames(st)<-c("linename", "station", "storagename", "storage_save")
  st$linename<-na.locf(st$linename)
  st$date<-rep(d, from=1, to=length(st$linename))
  pStorageIn<-rbind(pStorageIn, st)
  
  #output, client
  s<-readWorksheet(xls, "����", region="B1:E22", header=TRUE)
  colnames(s)<-c("linename", "station", "client", "client_use")
  s$linename<-na.locf(s$linename)
  s$station<-na.locf(s$station)
  s$date<-rep(d, form=1, to=length(s$linename))
  pVendorOut<-rbind(pVendorOut, s)
  
  #storage
  st<-readWorksheet(xls, "����", region="B25:E26", header=TRUE)
  colnames(st)<-c("linename", "station", "storagename", "storage_output")
  st$linename<-na.locf(st$linename)
  st$date<-rep(d, from=1, to=length(st$linename))
  pStorageOut<-rbind(pStorageOut, st)
  
  #self consume
  sc<-readWorksheet(xls, "�Ժ�", region="A1:D15", header=TRUE)
  colnames(sc)<-c("linename", "linename2", "station", "daily_consume")
  sc$linename<-na.locf(sc$linename)
  sc$linename2<-na.locf(sc$linename2)
  sc$date<-rep(d, from=1, to=length(sc$linename))
  pSelfConsume<-rbind(pSelfConsume, sc)
  
  #pipe store
  ps<-readWorksheet(xls, "�ܴ�", region="A1:B3", header=TRUE)
  colnames(ps)<-c("gx", "daily_pipestore")
  ps$gx<-sub(linename, paste(linename, "_", sep=""), ps$gx)
  ps<-cbind(colsplit(ps$gx, "_", c("linename", "subline")), ps)
  ps$date<-rep(d, from=1, to=length(ps$linename))
  pPipeStore<-rbind(pPipeStore, ps)
}

# pVendorIn<-data.frame() #�������
# pStorageIn<-data.frame() #�������������Ӧ���뵱������
# pVendorOut<-data.frame() #�ͻ�����
# pStorageOut<-data.frame() #�������������Ӧ���뵱������
# pSelfConsume<-data.frame() #�������� 
# pPipeStore<-data.frame() #�ܴ�Vc[i]=pPipeStore[i], Vm[i]=pPipeStore[i+1]

pVendorIn$vendor_income<-as.numeric(pVendorIn$vendor_income)
pStorageIn$storage_save<-as.numeric(pStorageIn$storage_save)
pVendorOut$client_use<-as.numeric(pVendorOut$client_use)
pStorageOut$storage_output<-as.numeric(pStorageOut$storage_output)
pSelfConsume$daily_consume<-as.numeric(pSelfConsume$daily_consume)
pPipeStore$daily_pipestore<-as.numeric(pPipeStore$daily_pipestore)


write.csv(pVendorIn, paste(datapath, "gx1_mid/�������.csv", sep=""))
write.csv(pStorageIn, paste(datapath, "gx1_mid/�����������.csv", sep=""))
write.csv(pVendorOut, paste(datapath, "gx1_mid/�ͻ�����.csv", sep=""))
write.csv(pStorageOut, paste(datapath, "gx1_mid/�����������.csv", sep=""))
write.csv(pSelfConsume, paste(datapath, "gx1_mid/��������.csv", sep=""))
write.csv(pPipeStore, paste(datapath, "gx1_mid/�ܴ�.csv", sep=""))

#---------
pVendorOut<-aggregate(client_use~date+linename+station, data=pVendorOut, FUN=sum)
pPipeStore<-aggregate(daily_pipestore~date+linename, data=pPipeStore, FUN=sum)

r<-merge(pVendorIn, pStorageIn, by=c("date", "linename", "station"), all=TRUE)
r<-merge(r, pVendorOut, by=c("date", "linename", "station"), all=TRUE)
r<-merge(r, pStorageOut, by=c("date", "linename", "station"), all=TRUE)
r<-merge(r, pSelfConsume, by=c("date", "linename", "station"), all=TRUE)
r[is.na(r)]<-0

write.csv(r, paste(datapath, "r.csv", sep=""))

rg<-aggregate(.~date+linename, data=subset(r, select=c("vendor_income","storage_output","storage_save","client_use","daily_consume","date","linename")), FUN=sum)
rg<-merge(rg, pPipeStore, by=c("date", "linename"), all=TRUE)
write.csv(rg, paste(datapath, "rg.csv", sep=""))


fdgc<-read.csv(paste(datapath, "fdgc.csv", sep=""))