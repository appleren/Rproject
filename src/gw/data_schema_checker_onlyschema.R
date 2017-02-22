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
  skipFile<-FALSE
  tryCatch(con <- dbConnect(SQLite(), dbname=dbFile), error=function(e){
    print("connection failed")
    skipFile<-FALSE
  })
  
  
  tryCatch(rs<-dbSendQuery(con, "select * from RUNDATA"), error=function(e) {
    print("dbListFields failed")
    skipFile<-TRUE
  })
  chunk<-data.frame()
  tryCatch(chunk<-dbFetch(rs, n=1), error=function(e) {
    print("dbListFields failed")
    skipFile<-TRUE
  })
  colInfo<-NULL
  tryCatch(colInfo<-dbColumnInfo(rs, colnames(scadaData)), error=function(e) {
    print("dbListFields failed")
    skipFile<-TRUE
  })
  tryCatch(dbClearResult(rs), error=function(e) {
    print("dbListFields failed")
    skipFile<-TRUE
  })
  
  # scadaData<-data.frame()
#   tryCatch(scadaData <- dbGetQuery(con, 'select * from RUNDATA'), error=function(e) {
#     print("dbGetdata failed")
#     skipFile<-TRUE
#   })
  
  dbDisconnect(con)
  
  if(skipFile==TRUE) {
    print(paste("=============scadaDataReader failed::", curTarGz, fileName, sep = " "))
    return (NULL)
  } else if(is.null(colInfo)) {
    print(paste("=============scadaDataReader failed::", curTarGz, fileName, sep = " "))
    return (NULL)
  } else {
    fieldList<-colInfo$name
    fieldType<-colInfo$Sclass
    if(length(fieldList)==0){
      print(paste("error:dbListFieldsFailed", curTarGz, fileName, sep=" "))
    }
    
    rt<-list(fieldList=fieldList, fieldType=fieldType)
    return (rt)
  }
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
  #  turbines<-substr(bnames,1,8)
  bb<-strsplit(bnames, ".", fixed=TRUE)
  dd<-sapply(bb,'[',1)
  turbines<-substr(dd, 1, nchar(dd)-8)
  result<-NULL
  
#   rt<-scadaDataReader(files[1])
#   
#   if(!is.null(rt)) {
#     # scadaData<-rt$scadaData
#     result<-rt$fieldList
#     ft<-rt$fieldType
#     
#     numField<-length(result)
#     b<-strsplit(bnames[1], ".", fixed=TRUE)[[1]][1]
#     startDate<-substr(b, nchar(b)-7, nchar(b))
#     endDate<-startDate
#     i<-1
#     # schemaCount<-1
#     fieldDF<-data.frame(wtid=turbines[i],startDate,endDate, numField,fieldList=paste(result,collapse=";"),fieldType=paste(ft,collapse=";"),stringsAsFactors = FALSE)
#     
#     #   resultFileName1<-paste0(turbines[i],"_",
#     #                           startDate,"_",schemaCount,"_data.csv")
#     #   resultFileName1<-paste(outDir,"/",resultFileName1,sep="")             
#     #   scadaData$turbineID=rep(turbines[i], times=length(scadaData[,1]))
#   }
  
  
  if(length(files)>=1){
    for (i in 1:length(files)){
      if(i>1 && turbines[i]==turbines[i-1]){
        
        newrt<-scadaDataReader(files[i])
        
        if(!is.null(newrt)) {
          newresult<-newrt$fieldList
          newft<-newrt$fieldType
          if(!is.null(result) && paste(newresult,collapse=";")==paste(result,collapse=";")){
            #schema is the same
#             resultFileName1<-paste0(turbines[i-1],"_",
#                                     startDate,"_", schemaCount,"_data.csv")
#             resultFileName1<-paste(outDir,"/",resultFileName1,sep="")    
            # newrt$scadaData$turbineID=rep(turbines[i-1], times=length(newrt$scadaData[,1]))
            b<-strsplit(bnames[i], ".", fixed=TRUE)[[1]][1]
            endDate<-substr(b, nchar(b)-7, nchar(b))
          }else{
            #schema is not the same
            if(exists("fieldDF") && exists("endDate")) {
              fieldDF[dim(fieldDF)[1],]$endDate<-endDate
              b<-strsplit(bnames[i], ".", fixed=TRUE)[[1]][1]
              startDate<-substr(b, nchar(b)-7, nchar(b))
              endDate<-startDate
              result<-newresult
              ft<-newft
              numField<-length(result)
              fieldDF<-rbind(fieldDF,data.frame(wtid=turbines[i],startDate,endDate,numField,fieldList=paste(result,collapse=";"),fieldType=paste(ft,collapse=";"),stringsAsFactors = FALSE))
            } else {
              b<-strsplit(bnames[i], ".", fixed=TRUE)[[1]][1]
              startDate<-substr(b, nchar(b)-7, nchar(b))
              endDate<-startDate
              result<-newresult
              ft<-newft
              numField<-length(result)
              fieldDF<-data.frame(wtid=turbines[i],startDate,endDate,numField,fieldList=paste(result,collapse=";"),fieldType=paste(ft,collapse=";"),stringsAsFactors = FALSE)
            }
#             schemaCount<-schemaCount+1
#             resultFileName1<-paste0(turbines[i-1],"_",
#                                     startDate,"_", schemaCount,"_data.csv")
#             resultFileName1<-paste(outDir,"/",resultFileName1,sep="")   
#             newrt$scadaData$turbineID=rep(turbines[i-1], times=length(newrt$scadaData[,1]))
          }
        }
        
      }else{
        if (i>1) {
          b<-strsplit(bnames[i-1], ".", fixed=TRUE)[[1]][1]
          ed<-substr(b, nchar(b)-7, nchar(b))
          resultFileName<-paste0(turbines[i-1],"_",
                                 startDate,"-",
                                 ed,".csv")
          resultFileName<-paste(outDir,"/",resultFileName,sep="")					   
          fieldDF[dim(fieldDF)[1],]$endDate<-endDate
          write.csv(fieldDF,file=resultFileName)
          outFiles<-paste(outFiles,",",resultFileName,sep="")
        }
        
        rt<-scadaDataReader(files[i])
        
        if (!is.null(rt)){
          # scadaData<-rt$scadaData
          result<-rt$fieldList
          ft<-rt$fieldType
          
          numField<-length(result)
          b<-strsplit(bnames[i], ".", fixed=TRUE)[[1]][1]
          startDate<-substr(b, nchar(b)-7, nchar(b))
          endDate<-startDate
          fieldDF<-data.frame(wtid=turbines[i],startDate,endDate,numField,fieldList=paste(result,collapse=";"),fieldType=paste(ft,collapse=";"),stringsAsFactors = FALSE)
          
          # schemaCount<-schemaCount+1
#           resultFileName1<-paste0(turbines[i],"_",
#                                   startDate,"_", schemaCount,"_data.csv")
#           resultFileName1<-paste(outDir,"/",resultFileName1,sep="")        	 
          # scadaData$turbineID=rep(turbines[i], times=length(scadaData[,1]))  
        }
      }
    }
  }
  
  print(paste0("WindFarmStatistics:resDim:",dim(fieldDF)[1]))
  if(dim(fieldDF)[1]>0){
    b<-strsplit(bnames[i], ".", fixed=TRUE)[[1]][1]
    ed<-substr(b, nchar(b)-7, nchar(b))
    resultFileName<-paste0(turbines[i],"_",
                           startDate,"-",
                           ed,".csv")
    resultFileName<-paste(outDir,"/",resultFileName,sep="")	
    fieldDF[dim(fieldDF)[1],]$endDate<-endDate
    write.csv(fieldDF,file=resultFileName)
    outFiles<-paste(outFiles,",",resultFileName,sep="")
  }
  
  
  outFiles<-substr(outFiles,2,nchar(outFiles))
  outFiles
}

# mainfunction<-function(inputWindFarmTarZg, outputFiles){
#   outFiles<-unlist(strsplit(outputFiles,","))
#   outDir<-dirname(outFiles[1])
#   if(outDir=="."){
#     outDir<-getwd()
#   }
#   
#   filename<-paste(basename(inputWindFarmTarZg),"time.txt", sep="_")
#   filename<-paste(outDir, "/", filename, sep="")
#   aa<-system.time(bb<-mainfunction1(inputWindFarmTarZg, outputFiles))
#   write.matrix(aa, file=filename)
#   utFiles<-paste(bb,",",filename,sep="")
#   return (bb)
#   
# }

inputWindFarmTarZg<-"../../data/gw/gwTarGz/GW150091201603.tar.gz"
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
