require(tools)
library(RMySQL)
library(forecast)
library(EMD)
library(rpart)
library(Rssa)
library(plyr)
options(warn=2)

RException<-setClass("RException",
                   slots=c(message="character"))

handleError<-function(e) {
  c<-attr(e, "class")
  if(length(c)!=0 & ("error" %in% c | "simpleError" %in% c)){
    RException(message=conditionMessage(e))
  }
}

# w<-tryCatch(warning("this is a warning"), error =handleError)
# e<-tryCatch(stop("this is an error"), error =handleError)


detach("package:EMD", unload=TRUE)
library(EMD)
getOption("warn")
options(warn=0)
