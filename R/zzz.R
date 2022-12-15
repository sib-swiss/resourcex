.onAttach <- function(libname, pkgname) {
  
  doRegisterResolver <- function(res) {
    clazz <- class(res)[[1]]
    packageStartupMessage(paste0("Registering ", clazz, "..."))
    resourcer::registerResourceResolver(res)
  }
 doRegisterResolver(SQLFlexResolver$new())
 
}

.onDetach <- function(libpath) {
  resourcer::unregisterResourceResolver("ResourceResolver")
}


