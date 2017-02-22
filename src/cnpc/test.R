subset2 <- function(x, condition) {
  condition_call <- substitute(condition)
  print(parent.frame())
  r <- eval(condition_call, x, parent.frame())
  x[r, ]
}

scramble <- function(x) {
  print(environment())
  x[sample(nrow(x)), ]
}

subscramble <- function(x, condition) {
  scramble(subset2(x, condition))
}
