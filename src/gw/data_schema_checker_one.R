library(RSQLite)
# require(compiler)
# enableJIT(3)  #to speed up using byte code compiler

#fileName<- "E:/hadoopWorkspace/JOBS_PAS/gw/Rsrc/targzDir/GW150001201410/GW15000120141002.goldwind"
bWindows<-FALSE
sysInfo <-Sys.info() 
if(sysInfo["sysname"]=="Windows"){
  bWindows<-TRUE
}

options(show.error.messages = TRUE)
curTarGz<-""

# GW15000120141101.goldwind GW15000120120613.goldwind GW15001020130928.goldwind
# fileName<-"E:/hadoopWorkspace/JOBS_PAS/testR/targzDir/GW150002201603/GW15000220160301.goldwind"
scadaDataReaderOld<-function(fileName){
  print(fileName)
  if(bWindows){
    dbFile<=""
    tryCatch(dbFile<-unzip(fileName,junkpaths = TRUE,exdir="unzipTest"), error=function(e) e)
    dir <- getwd()
    dbFile <- paste(dir, '/',dbFile, sep="" )
  }else{
    cmdStr <- paste('unzip -o -j ',fileName," -d unzipTest", sep="")
    print(cmdStr)
    try(system(cmdStr))
    name <- basename(fileName)
    name <- substr(name, 1, nchar(name)-9)
    dir <- getwd()
    dbFile <- paste(dir, '/unzipTest/',name,'.db', sep="" )
  }
  if(!file.exists(dbFile) || file.info(dbFile)$isdir) {
    print(paste("unzipFailed: ", fileName, sep=""))	
    return(data.frame())
  }
  
  #print(dbFile)
  con <- dbConnect(SQLite(), dbname=dbFile)
  
  #init 
  scadaData<-data.frame(WMAN.Tm = "", WNAC.WSpd.InstMag.f = 0, WTUR.PwrAt.InstMag.f = 0,
                        WROT.PtAngVal.Bl1=0,WROT.PtAngVal.Bl2=0,WROT.PtAngVal.Bl3=0,
                        WGEN.Spd.InstMag.i=0,WMAN.State=0,WNAC.ExlTmp.instMag.f=0)
  
  try(scadaData <- dbGetQuery(con, 'select "WMAN.Tm", "WNAC.WSpd.InstMag.f", "WTUR.PwrAt.InstMag.f","WROT.PtAngVal.Bl1", "WROT.PtAngVal.Bl2", "WROT.PtAngVal.Bl3", 
                              "WGEN.Spd.InstMag.i", "WMAN.State", "WNAC.ExlTmp.instMag.f" from RUNDATA')
  )
  dbDisconnect(con)
  if (dim(scadaData)[1]==0)  {
    print(paste("dbGetQuery:", fileName, sep=""))	
    return(data.frame())
  }
  scadaData$WMAN.Tm<-paste0(substr(scadaData$WMAN.Tm,1,15),"0:00")
  
  scadaData_10m<-aggregate(.~WMAN.Tm,data=scadaData,FUN=mean)  #select the 1st record every 10 minute
  
  #rm(scadaData)
  
  scadaData_10m$WMAN.State<-round(scadaData_10m$WMAN.State)
  
  
  #write.csv(scadaData_10m,file=paste0(substr(fileName,1,nchar(fileName)-8),"csv"))
  return(scadaData_10m)
  
}

scadaDataReader<-function(fileName){
  print(paste("curTarGz:scadaDataReader:",curTarGz))
  print(fileName)
  if(bWindows){
    dbFile<-""
    tryCatch(dbFile<-unzip(fileName,junkpaths = TRUE,exdir="unzipTest"), error=function(e) print(fileName))
    dir <- getwd()
    dbFile <- paste(dir, '/',dbFile, sep="" )
  }else{
    cmdStr <- paste('unzip -o -j ',fileName," -d unzipTest", sep="")
    print(cmdStr)
    try(system(cmdStr))
    name <- basename(fileName)
    name <- substr(name, 1, nchar(name)-9)
    dir <- getwd()
    dbFile <- paste(dir, '/unzipTest/',name,'.db', sep="" )
  }
  if(!file.exists(dbFile) || file.info(dbFile)$isdir) {
    print(paste("error:unzipFailed:",curTarGz, fileName, sep=" "))	
    return(c())
  }
  
  #print(dbFile)
  con <- dbConnect(SQLite(), dbname=dbFile)
  
  tryCatch(rs<-dbSendQuery(con, "select * from RUNDATA"), error=function(e) {
    print("dbListFields failed")
  })
  chunk<-data.frame()
  tryCatch(chunk<-dbFetch(rs, n=1), error=function(e) {
    print("dbListFields failed")
  })
  tryCatch(colInfo<-dbColumnInfo(rs, colnames(scadaData)), error=function(e) {
    print("dbListFields failed")
  })
  tryCatch(dbClearResult(rs), error=function(e) {
    print("dbListFields failed")
  })
  fieldList<-colInfo$name
  fieldType<-colInfo$Sclass
  
  scadaData<-data.frame()
  tryCatch(scadaData <- dbGetQuery(con, 'select * from RUNDATA'), error=function(e) {
    print("dbGetdata failed")
  })
  
  dbDisconnect(con)
  
  if(length(fieldList)==0){
    print(paste("error:dbListFieldsFailed", curTarGz, fileName, sep=" "))
  }
  if(length(scadaData)==0){
    print(paste("error:dbGetdataFailed", curTarGz, fileName, sep=" "))
  }
  
  rt<-list(fieldList=fieldList, fieldType=fieldType, scadaData=scadaData)
  
  return(rt)
  
}

testDB<-function(){
  dbFile<-"E:/GitHub/Rprojects/data/gw/gwTarGz/GW150001201306/GW15000120130611.db"
  con <- dbConnect(SQLite(), dbname=dbFile)
  #   scadaData<-dbGetQuery(con, 'select * from RUNDATA')
  #   fieldList<-dbListFields(con,"RUNDATA")
  rs<-dbSendQuery(con, "select * from RUNDATA")
  chunk<-fetch(rs, n=10)
  colInfo<-dbColumnInfo(rs, colnames(scadaData))
  dbClearResult(rs)
  dbDisconnect(con)
}

# multiple files string seperated by ","
# suppose: just use file names (under current working directory), there is no directory string
# inputFilesDir<-"E:/hadoopWorkspace/JOBS_PAS/testR/targzDir/GW150002201603"
# outDir<-"E:/hadoopWorkspace/JOBS_PAS/testR/output"
WindFarmStatistics<-function(inputFilesDir,outDir){ 
  print(paste("curTarGz:WindFarmStatistics:",curTarGz))
  
  files <- list.files(inputFilesDir, full.names = TRUE)
  if(length(files)==0){
    print(paste("error:noFile:",curTarGz, sep=" "))
  }
  fsize<-file.size(files)
  smF<-files[fsize<=1000]
  if(length(smF)>0){
    print(paste("error:smallFile:",curTarGz, smF, sep=" "))
  }
  
  files<-files[fsize>1000]
  if(length(files)==0){
    return("")
  }
  outFiles<-""
  outFiles1<-""
  
  bnames<-basename(files)
  turbines<-substr(bnames,1,8)
  
  rt<-scadaDataReader(files[1])
  
  scadaData<-rt$scadaData
  result<-rt$fieldList
  ft<-rt$fieldType
  
  numField<-length(result)
  startDate<-substr(bnames[1],9,nchar(bnames[1])-9)
  endDate<-startDate
  i<-1
  schemaCount<-1
  fieldDF<-data.frame(wtid=turbines[i],startDate,endDate, numField,fieldList=paste(result,collapse=";"),fieldType=paste(ft,collapse=";"),stringsAsFactors = FALSE)
  
  #   resultFileName1<-paste0(turbines[i],"(",
  #                           startDate,schemaCount,"_data.csv")
  #   resultFileName1<-paste(outDir,"/",resultFileName1,sep="")             
  #   write.csv(scadaData,file=resultFileName1,row.names = FALSE)
  #   outFiles<-paste(outFiles,",",resultFileName1,sep="")
  
  if(length(files)>=2){
    for (i in 2:length(files)){
      if(turbines[i]==turbines[i-1]){
        
        newrt<-scadaDataReader(files[i])
        
        
        newresult<-newrt$fieldList
        newft<-newrt$fieldType
        if(paste(newresult,collapse=";")==paste(result,collapse=";")){
          #schema is the same
          resultFileName1<-paste0(turbines[i-1],"(",
                                  startDate,schemaCount,"_data.csv")
          resultFileName1<-paste(outDir,"/",resultFileName1,sep="")             
          # write.table(newrt$scadaData,file=resultFileName1,row.names = FALSE, append=TRUE,col.names=FALSE)
          scadaData<-rbind(scadaData, newrt$scadaData)
          
          outFiles<-paste(outFiles,",",resultFileName1,sep="")
          endDate<-substr(bnames[i],9,nchar(bnames[1])-9)
        }else{
          #schema is not the same
          
          fieldDF[dim(fieldDF)[1],]$endDate<-endDate
          startDate<-substr(bnames[i],9,nchar(bnames[1])-9)
          endDate<-startDate
          result<-newresult
          ft<-newft
          numField<-length(result)
          fieldDF<-rbind(fieldDF,data.frame(wtid=turbines[i],startDate,endDate,numField,fieldList=paste(result,collapse=";"),fieldType=paste(ft,collapse=";"),stringsAsFactors = FALSE))
          
          #xinlu
          schemaCount<-schemaCount+1
          resultFileName1<-paste0(turbines[i-1],"(",
                                  startDate,schemaCount,"_data.csv")
          resultFileName1<-paste(outDir,"/",resultFileName1,sep="")             
          write.csv(scadaData,file=resultFileName1,row.names = FALSE)
          outFiles<-paste(outFiles,",",resultFileName1,sep="")
          
          
          # schemaCount<-schemaCount+1
          #           resultFileName1<-paste0(turbines[i-1],"(",
          #                                   startDate,schemaCount,"_data.csv")
          #           resultFileName1<-paste(outDir,"/",resultFileName1,sep="")   
          #           write.csv(newrt$scadaData,file=resultFileName1,row.names = FALSE)
          #           outFiles<-paste(outFiles,",",resultFileName1,sep="")
          scadaData<-newrt$scadaData
        }
      }else{
        resultFileName<-paste0(turbines[i-1],"(",
                               startDate,"-",
                               substr(bnames[i-1],9,nchar(bnames[i-1])-9),").csv")
        resultFileName<-paste(outDir,"/",resultFileName,sep="")					   
        fieldDF[dim(fieldDF)[1],]$endDate<-endDate
        write.csv(fieldDF,file=resultFileName)
        outFiles<-paste(outFiles,",",resultFileName,sep="")
        
        #xinlu
        schemaCount<-schemaCount+1
        resultFileName1<-paste0(turbines[i-1],"(",
                                startDate,schemaCount,"_data.csv")
        resultFileName1<-paste(outDir,"/",resultFileName1,sep="")             
        write.csv(scadaData,file=resultFileName1,row.names = FALSE)
        outFiles<-paste(outFiles,",",resultFileName1,sep="")
        
        rt<-scadaDataReader(files[i])
        
        scadaData<-rt$scadaData
        result<-rt$fieldList
        ft<-rt$fieldType
        
        numField<-length(result)
        startDate<-substr(bnames[i],9,nchar(bnames[i])-9)
        endDate<-startDate
        fieldDF<-data.frame(wtid=turbines[i],startDate,endDate,numField,fieldList=paste(result,collapse=";"),fieldType=paste(ft,collapse=";"),stringsAsFactors = FALSE)
        
        #         resultFileName1<-paste0(turbines[i],"(",
        #                                 startDate,schemaCount,"_data.csv")
        #         resultFileName1<-paste(outDir,"/",resultFileName1,sep="")        	   
        #         write.csv(scadaData,file=resultFileName1,row.names = FALSE)
        #         outFiles<-paste(outFiles,",",resultFileName1,sep="")
        
        
        #         
        #         startDate<-substr(bnames[i],9,nchar(bnames[i])-9)
        
        outFiles<-paste(outFiles,",",resultFileName1,sep="")
      }
    }
  }
  
  print(paste0("WindFarmStatistics:resDim:",dim(fieldDF)[1]))
  if(dim(fieldDF)[1]>0){
    resultFileName<-paste0(turbines[i],"(",
                           startDate,"-",
                           substr(bnames[i],9,nchar(bnames[i])-9),").csv")
    resultFileName<-paste(outDir,"/",resultFileName,sep="")	
    fieldDF[dim(fieldDF)[1],]$endDate<-endDate
    write.csv(fieldDF,file=resultFileName)
    outFiles<-paste(outFiles,",",resultFileName,sep="")
  }
  
  #xinlu
  if(dim(scadaData)[1]>0){
    resultFileName1<-paste0(turbines[i],"(",
                            startDate,"-",
                            substr(bnames[i],9,nchar(bnames[i])-9),")_data.csv")
    resultFileName1<-paste(outDir,"/",resultFileName1,sep="")  
    write.csv(scadaData,file=resultFileName1,row.names = FALSE)
    outFiles<-paste(outFiles,",",resultFileName1,sep="")
  }
  
  outFiles<-substr(outFiles,2,nchar(outFiles))
  outFiles
}

inputWindFarmTarZg<-"../../data/gw/gwTarGz/GW250152016022.tar.gz"
outputFiles<-"../../data/gw/gwTarGz/log/tt.txt"
mainfunction<-function(inputWindFarmTarZg, outputFiles){ 
  logf<-"/var/log/hadoop-httpfs/JOBS_PAS/gw/log/R_log.txt"
  #logf<-"err.log"
  if(!bWindows){
    #sink(logf,append = TRUE)
  }
  
  
  # get outDir
  
  step<-0
  outFiles<-unlist(strsplit(outputFiles,","))
  outDir<-dirname(outFiles[1])
  if(outDir=="."){
    outDir<-getwd()
  }
  outFiles<-""
  
  
  print(paste("startMainfunction: ",inputWindFarmTarZg,sep=""))
  
  #
  files<-unlist(strsplit(inputWindFarmTarZg,","))
  targzBnames<-basename(files)
  #GW150001201409
  wfmonth<-substr(targzBnames,1,14)
  step<-1
  targzDir<-paste(getwd(),"/targzDir",sep="");
  #tryCatch({
  if(!dir.exists(targzDir)){
    dir.create(targzDir)
  }
  
  #/home/fit/workspace/jimi/testR/GW150001201409
  gwTargzs<-paste(targzDir,"/",wfmonth,sep="")
  
  print("step1:gwTargzs:")
  step<-2
  #cmd 
  if(bWindows){
    untar(files,exdir=targzDir)
  }else{
    cmd<-paste("tar -zxvf ",files," -C ", targzDir,sep="")
    print(cmd)
    for(i in 1:length(cmd)){
      tryCatch(system(cmd[i]), error=function(e) print(paste("error:untarFailed:",cmd,sep=" ")))
    }
  }
  print("step2:")
  step<-3
  outFiles<-""
  print(gwTargzs)
  for(i in 1:length(gwTargzs)){
    curTarGz<<-files[i]
    inputFilesDir<-gwTargzs[i]
    #inputFilesDir<-"E:/hadoopWorkspace/JOBS_PAS/gw/Rsrc/targzDir/GW150001201410"
    print(paste("inputFilesDir: ",inputFilesDir,sep=""))
    outfile<-WindFarmStatistics(inputFilesDir,outDir)
    print(paste("*****WindFarmEnd*****",outfile,sep=""))
    if(nchar(outfile)>0){
      outFiles<-paste(outFiles,",",outfile,sep="")
    }
    
  }
  
  print("step3:")
  step<-4
  # delet unzipTest
  dir <- getwd()
  dir <- paste(dir, '/unzipTest',sep="")
  unlink(dir, recursive=TRUE, force = TRUE)    
  
  # delet unzipTest
  dir <- getwd()
  dir <- paste(dir, '/targzDir',sep="")
  unlink(dir, recursive=TRUE, force = TRUE)    
  
  print(paste("inputWindFarmTarZg: ",inputWindFarmTarZg, sep=""))
  if(nchar(outFiles)>0){
    outFiles<-substr(outFiles,2,nchar(outFiles))
    print(paste("resOutFiles: ",outFiles,sep=""))
    #sink()
    return(outFiles)
  }
  
  if(!bWindows){
    #sink()
  }
  #},error=function(e){print(paste(gwTargzs,"exception at step",step))},finally = {print("serious exception");sink()})
  
  
}

# intargz
# dir<-"E:/Rworkspace/rproject/goldwind/input"
# testMainfunction(dir)
testMainfunction<-function(dir){
  fileName<-"c:/test/GW150002201604.tar.gz"
  outdir<-paste0("c:/test/",Sys.Date(),".csv")
  mainfunction(fileName,outdir)
  #   fs<-list.files(dir, full.names = TRUE,recursive = TRUE)
  #   outdir<-paste(getwd(),"/output/a.txt",sep="")
  #   
  #   for(f in fs){
  #     print(f)
  #     info<-file.info(f)
  #     if(!info$isdir){
  #       mainfunction(f,outdir)
  #     }
  #     
  #   }
}

testScadaReader<-function(){
  fileName<-"c:/test/GW15001020131127.Goldwind"
  print(scadaDataReader(fileName))
}



testTimes <- function(times){
  for( i in 1:times){ 
    if(bWindows){
      inputWindFarmTarZg<-"E:/hadoopWorkspace/JOBS_PAS/gw/Rsrc/GW150001201409.tar.gz,E:/hadoopWorkspace/JOBS_PAS/gw/Rsrc/GW150001201410.tar.gz"
      outputFiles<-"E:/hadoopWorkspace/JOBS_PAS/gw/Rsrc/outputTmp/Rout_GW150001201410.tar.gz,E:/hadoopWorkspace/JOBS_PAS/gw/Rsrc/outputTmp/Rout_GW150001201409.tar.gz"
    }else{
      inputWindFarmTarZg<-"/home/fit/workspace/jimi/testR/GW150001201409.tar.gz,/home/fit/workspace/jimi/testR/GW150001201411.tar.gz"
      outputFiles<-"/home/fit/workspace/jimi/testR/outputTmp/Rout_GW150001201410.csv,/home/fit/workspace/jimi/testR/outputTmp/Rout_GW15000120.csv"
    }
    mainfunction(inputWindFarmTarZg, outputFiles)
  }
}

# system.time(testTimes(1))
dateSel<-function(x){
  x$startDate<-as.Date(x$startDate,"%Y%m%d")
  x$endDate<-as.Date(x$endDate,"%Y%m%d")
  x<-x[order(x$startDate),]  
}


mergefile<-function(){
  setwd("D:/Communication/E&U/GoldWind(R&D)/10minutes data processing/20160607040654581/PREPROCESSING")
  dirs<-list.dirs()
  y<-data.frame()
  
  #different wind farm
  for (dir in dirs){
    tmpStr<-unlist(strsplit(dir,split="/"))
    if(length(tmpStr)<3) next
    print(dir)
    farmID<-tmpStr[3]
    turbineID<-""
    
    for (fileName in list.files(dir)){
      wtd<-read.csv(paste0(dir,"/",fileName),stringsAsFactors = FALSE)
      td<-wtd[1,]
      if (td$wtid==turbineID){
        if (td$numField==pre_td$numField){
          
        }else{
          y<-rbind(y,data.frame(farmID,turbineID,startDate,endDate,numField=pre_td$numField,
                                fieldList=pre_td$fieldList))
          startDate<-td$startDate
        }
      }else{
        if(turbineID!=""){
          y<-rbind(y,data.frame(farmID,turbineID,startDate,endDate,numField=pre_td$numField,
                                fieldList=pre_td$fieldList))
        }
        startDate<-td$startDate
        turbineID<-td$wtid
      }
      pre_td<-td
      endDate<-td$endDate
      
      # if there are mutliple record within a file, we need include all the middle ones, and have a special 
      # handling with the last record
      m<-dim(wtd)[1]
      if (m>1){
        if(m>2){
          tmp<-wtd[2:(m-1),2:6]
          tmp$farmID<-farmID
          names(tmp)[1]<-"turbineID"
          y<-rbind(y,tmp)
        }
        pre_td<-wtd[m,]
        startDate<-pre_td$startDate
        endDate<-pre_td$endDate
      }
    }
    # to ensure the last record of a wind farm is also kept
    y<-rbind(y,data.frame(farmID,turbineID,startDate,endDate,numField=pre_td$numField,
                          fieldList=pre_td$fieldList))
  }
  write.csv(y,file="data_schema.csv",row.names = FALSE)
}
