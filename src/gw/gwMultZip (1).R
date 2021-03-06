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
# fileName<-"E:/hadoopWorkspace/JOBS_PAS/testR/targz/GW15000320120408.goldwind"
scadaDataReader<-function(fileName){
  print(paste("curTarGz:scadaDataReader:",curTarGz))
  print(paste("scadaDataReaderGoldWindFile:",fileName))
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
	  return(data.frame())
  }
  
  #print(dbFile)
  con <- dbConnect(SQLite(), dbname=dbFile)
  
  fieldList<-c()
  tryCatch(fieldList<-dbListFields(con,"RUNDATA"), error=function(e){
    #print(paste("error:dbListFields",curTarGz, fileName, sep=" "))
  })
  
  if(length(fieldList)==0){
    dbDisconnect(con)
    print(paste("error:dbListFieldsFailed",curTarGz, fileName, sep=" "))
    return(data.frame())
  }
  
  
  #init 
  scadaData_10m<-data.frame()
  baseFileName<-basename(fileName)
  
  # 3 different type of files:arc, goldwind_2500 or 2000, goldwind_1500
  if (tolower(substr(fileName,nchar(fileName)-2,nchar(fileName)))=="arc"){
    fileType<-"arc"
  }else{
    fileType<-substr(baseFileName,3,3)
  }
 
  if ("WTUR.TurSt.Rs.S" %in% fieldList){
    stateSQLstr<-' "WTUR.TurSt.Rs.S" as WTUR_TurSt_Rs_S '
    
  }else if ("WTUR.TurSt.actsp" %in% fieldList){
    stateSQLstr<-' "WTUR.TurSt.actsp" as WTUR_TurSt_Rs_S '
    
  }else{
    stateSQLstr<-' "WMAN.State" as WTUR_TurSt_Rs_S '
    if(fileType=="arc") stateSQLstr<-' "WTUR.State.Rn.I8" as WTUR_TurSt_Rs_S '
    print(paste("WARN:","WTUR.TurSt.Rs.S or WTUR.TurSt.actsp is not in current DB",curTarGz, fileName, sep=" "))
  }
  
  bLimPowSQLstr <- '
    MAX(
    (case lower("WTUR.LimPow")                    when "1" then 1 when "true" then 1 else 0 end) |
    (case lower("WMAN.bLimPow.Flg")               when "1" then 1 when "true" then 1 else 0 end) |
    (case lower("WTUR.Power.LimitMode")           when "1" then 1 when "true" then 1 else 0 end) |
    (case lower("WMAN.bLimPow")                   when "1" then 1 when "true" then 1 else 0 end) |
    (case lower("WTUR.Other.Rn.I16.LimPow")       when "1" then 1 when "true" then 1 else 0 end) |
    (case lower("WTUR.Bool.Rd.b0.PowerFlag")      when "1" then 1 when "true" then 1 else 0 end) |
    (case lower("WTUR.Other.Ri.I16.LitPowByPLC")  when "1" then 1 when "true" then 1 else 0 end) |
    (case lower("WTUR.Other.Ri.I16.bLitPow")      when "1" then 1 when "true" then 1 else 0 end) |
    (case lower("WTUR.PwrAt.Rn.I16.PowerLimit")   when "1" then 1 when "true" then 1 else 0 end) 
    ) as WMAN_bLimPow
  '

  stopMode <- '
   MIN(case lower("WTUR.Other.Wn.I16.StopModeWord") when "6" then 6 when "15" then 15 when "16" then 16 else 1000 end ,
    case lower("WTUR.Stop.ModeWord") when "6" then 6 when "15" then 15 when "16" then 16 else 1000 end ,
    case lower("WMAN.stop_mode_word") when "6" then 6 when "15" then 15 when "16" then 16 else 1000 end ,
    case lower("TUR.Stop.ModeWord") when "6" then 6 when "15" then 15 when "16" then 16 else 1000 end
  )as WAMN_stopMode
  '
  if(fileType=="2"){
    targetDate<-substr(baseFileName,nchar(baseFileName)-16,nchar(baseFileName)-9)
    
    dateFieldName<-"WMAN.Tm" 
    dateFilterStr<-paste0(' from RUNDATA where "',dateFieldName,'"!="0" and "',dateFieldName,'">="', format(strptime(targetDate, "%Y%m%d"), "%Y-%m-%d 00:00:00"),'"',
                          'and "',dateFieldName,'"<="',format(strptime(targetDate, "%Y%m%d"), "%Y-%m-%d 23:59:59"),'" group by substr("',dateFieldName,'",1,15)')
      
    
    sqlStr<-paste0('select MAX(substr("WMAN.Tm",1,15))||"0:00" as WMAN_TM,
                AVG("WNAC.WSpd.InstMag.f") as WNAC_WSpd_InstMag_f,
    AVG("WTUR.PwrAt.InstMag.f") as WTUR_PwrAt_InstMag_f,
    AVG("WROT.CptActualPosi.B1") as WROT_PtAngVal_Bl1,
    AVG("WROT.CptActualPosi.B2") as WROT_PtAngVal_Bl2,
    AVG("WROT.CptActualPosi.B3") as WROT_PtAngVal_Bl3,
    AVG("WGEN.Spd.InstMag.i") as WGEN_Spd_InstMag_i,
    AVG("WNAC.ExlTmp.instMag.f") as WNAC_ExlTmp_instMag_f,
    MAX("WGEN.TotPwr.InstMag.i") as WGEN_TotPwr_InstMag_i,',
                   bLimPowSQLstr,',',
                   stopMode,
    dateFilterStr)
    
    #MAX("WMAN.bLimPow") as WMAN_bLimPow',
    
  }else if (fileType=="arc"){
    targetDate<-substr(baseFileName,nchar(baseFileName)-11,nchar(baseFileName)-4)
    
    dateFieldName<-"WTUR.Tm.Rw.Dt" 
    dateFilterStr<-paste0(' from RUNDATA where "',dateFieldName,'"!="0" and "',dateFieldName,'">="', format(strptime(targetDate, "%Y%m%d"), "%Y-%m-%d 00:00:00"),'"',
                          'and "',dateFieldName,'"<="',format(strptime(targetDate, "%Y%m%d"), "%Y-%m-%d 23:59:59"),'" group by substr("',dateFieldName,'",1,15)')
       
    
    sqlStr<-paste0('select MAX(substr("WTUR.Tm.Rw.Dt",1,15))||"0:00" as WMAN_TM,
                AVG("WTUR.WSpd.Ra.F32") as WNAC_WSpd_InstMag_f,
    AVG("WTUR.PwrAt.Ra.F32") as WTUR_PwrAt_InstMag_f,
    AVG("WTPS.Ang.Ra.F32.blade1") as WROT_PtAngVal_Bl1,
    AVG("WTPS.Ang.Ra.F32.blade2") as WROT_PtAngVal_Bl2,
    AVG("WTPS.Ang.Ra.F32.blade3") as WROT_PtAngVal_Bl3,
    AVG("WGEN.Spd.Ra.F32") as WGEN_Spd_InstMag_i,
    AVG("WTUR.Temp.Ra.F32") as WNAC_ExlTmp_instMag_f,
    MAX("WTUR.TotEgyAt.Wt.F32") as WGEN_TotPwr_InstMag_i,',
                   bLimPowSQLstr,',',
                   stopMode,
    dateFilterStr)
    
    #MAX("WMAN.bLimPow") as WMAN_bLimPow',
    
  }else{
    targetDate<-substr(baseFileName,nchar(baseFileName)-16,nchar(baseFileName)-9)
    
    dateFieldName<-"WMAN.Tm" 
    dateFilterStr<-paste0(' from RUNDATA where "',dateFieldName,'"!="0" and "',dateFieldName,'">="', format(strptime(targetDate, "%Y%m%d"), "%Y-%m-%d 00:00:00"),'"',
                          'and "',dateFieldName,'"<="',format(strptime(targetDate, "%Y%m%d"), "%Y-%m-%d 23:59:59"),'" group by substr("',dateFieldName,'",1,15)')
        
    sqlStr<-paste0('select MAX(substr("WMAN.Tm",1,15))||"0:00" as WMAN_TM,
                AVG("WNAC.WSpd.InstMag.f") as WNAC_WSpd_InstMag_f,
    AVG("WTUR.PwrAt.InstMag.f") as WTUR_PwrAt_InstMag_f,
    AVG("WROT.PtAngVal.Bl1") as WROT_PtAngVal_Bl1,
    AVG("WROT.PtAngVal.Bl2") as WROT_PtAngVal_Bl2,
    AVG("WROT.PtAngVal.Bl3") as WROT_PtAngVal_Bl3,
    AVG("WGEN.Spd.InstMag.i") as WGEN_Spd_InstMag_i,
    AVG("WNAC.ExlTmp.instMag.f") as WNAC_ExlTmp_instMag_f,
    MAX("WGEN.TotPwr.InstMag.i") as WGEN_TotPwr_InstMag_i,',
                   bLimPowSQLstr,',',
                   stopMode,
    dateFilterStr)
    
    #MAX("WMAN.bLimPow") as WMAN_bLimPow',
    
  }
  
  tryCatch(scadaData_10m <- dbGetQuery(con,sqlStr),error=function(e){
    #print(paste("error:dbGetQuery",curTarGz, fileName, sep=" "))
  })
  
   
  dbDisconnect(con)
  
  if (dim(scadaData_10m)[1]==0) {
    print(paste("error:dbGetQueryFailed:", curTarGz," ", fileName, sep=" "))	
    return(data.frame())
  }
  
  errTM<-which(nchar(scadaData_10m$WMAN_TM)<19)
  if(length(errTM)>0){
    print(paste("error:notvalidTM:", curTarGz, fileName, sep=" "))
  }
    
  
  #write.csv(scadaData_10m,file=paste0(substr(fileName,1,nchar(fileName)-8),"csv"))
  scadaData_10m$WAMN_stopMode[scadaData_10m$WAMN_stopMode==1000]<-0
  return(scadaData_10m)
  
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
    print(paste("warn:noValidSizeFile:",curTarGz, inputFilesDir, sep=" "))
    return("")
  }
  outFiles<-""
  
  #files<-c("GW15000120141128.goldwind", "GW2500120131215.goldwind", "GW150007920151226.goldwind")
  bnames<-basename(files)
  #GW15000120141128, GW2500120131215, GW150007920151226
  #TODO .arc
  namesNoType<-substr(bnames,1,nchar(bnames)-9)
  
  turbines<-substr(namesNoType,1,nchar(namesNoType)-8)
  yearMonthDay<-substr(namesNoType,nchar(namesNoType)-7,nchar(namesNoType))
  
  result<-scadaDataReader(files[1])
  startDate<-yearMonthDay[1]
  
  i<-1
  if(length(files)>=2){
    for (i in 2:length(files)){
      if(turbines[i]==turbines[i-1]){
        result<-rbind(scadaDataReader(files[i]),result)
      }else{
        resultFileName<-paste0(turbines[i-1],"",
                               startDate,"-",
                               yearMonthDay[i-1],".csv")
        resultFileName<-paste(outDir,"/",resultFileName,sep="")					   
        write.csv(result,file=resultFileName,row.names = FALSE)
        result<-scadaDataReader(files[i])
        startDate<-yearMonthDay[i]
        
        outFiles<-paste(outFiles,",",resultFileName,sep="")
      }
    }
  }
  
  print(paste0("WindFarmStatistics:resDim:",dim(result)[1]))
  if(dim(result)[1]>0){
    resultFileName<-paste0(turbines[i],"",
                           startDate,"-",
                           yearMonthDay[i],".csv")
    resultFileName<-paste(outDir,"/",resultFileName,sep="")	
    write.csv(result,file=resultFileName,row.names = FALSE)
    outFiles<-paste(outFiles,",",resultFileName,sep="")
    outFiles<-substr(outFiles,2,nchar(outFiles))
  }
  
  outFiles
}

#inputWindFarmTarZg<-"E:/Rworkspace/rproject/goldwind/input/GW250006201603.tar.gz"
#outputFiles<-"E:/Rworkspace/rproject/goldwind/output/tt.txt"
mainfunction<-function(inputWindFarmTarZg, outputFiles){     
  #GW\d{11,13}.tar.gz : 153811 
  #GW\d*2015\d*.tar.gz: 62735
  #GW\d*2015\d{3}.tar.gz: 4298
  #GW\d*2015\d{2}.tar.gz : 58369 this is valid
  #GW\d*2015\d{1}.tar.gz : 67
  
  files<-unlist(strsplit(inputWindFarmTarZg,","))
  outFiles<-unlist(strsplit(outputFiles,","))
  #files<-c('GW150001201509.tar.gz', 'GW250012015091.tar.gz', 'GW1500048201510.tar.gz', '(2).goldwind.tar.gz','GW150.tar.gz', '(.tar.gz','复件.tar.gz')
  targzBnames<-basename(files)
  
  # valid is begin with GW or gw, and index of .tat.gz>14
  #valid fileName
  fSel<-(substr(targzBnames,1,2)=='GW')
  files<-files[fSel]
  if (length(files)==0){
    print(paste0("warn:notvalidFilename:shouldStartWith:GW: ",inputWindFarmTarZg))
    return("")
  }
  targzBnames<-targzBnames[fSel]
  outFiles<-outFiles[fSel]
  
  #pattern<- "GW[[:digit:]]{11,13}.tar.gz"
  #only 2015
  pattern<- "GW[[:digit:]]*2015[[:digit:]]{2}.tar.gz"
  y<-grep(pattern,targzBnames,ignore.case = TRUE)
  
  if (length(y)==0){
    print(paste0("warn:notvalidFilename:shouldBeGW[[:digit:]]*2015[[:digit:]]{2}.tar.gz: ",inputWindFarmTarZg))
    return("")
  }
  files<-files[y]
  outFiles<-outFiles[y]
  targzBnames<-targzBnames[y]
  
  #GW150001201409, GW250012014091, GW150004820151
  wfmonth<-substr(targzBnames,1,nchar(targzBnames)-7)
  
  # get outDir
  step<-0
  outDir<-dirname(outFiles[1])
  if(outDir=="."){
    outDir<-getwd()
  }
  outFiles<-""
  
  
  print(paste("startMainfunction: ",inputWindFarmTarZg,sep=""))
  
  step<-1
  targzDir<-paste(getwd(),"/targzDir",sep="");
  
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
    return(outFiles)
  }  
 
}

# intargz
# dir<-"E:/Rworkspace/rproject/goldwind/input"
# testMainfunction(dir)
testMainfunction<-function(dir){
  fs<-list.files(dir, full.names = TRUE,recursive = TRUE)
  outdir<-paste(getwd(),"/output/a.txt",sep="")
  
  for(f in fs){
    print(f)
    info<-file.info(f)
    if(!info$isdir){
      mainfunction(f,outdir)
    }
    
  }
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
