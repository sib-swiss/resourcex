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


.onLoad <- function(...){
  # set some options here temporarily, remove them once the admins set them in opal:
  options('hidden.fields.regexes' = c('source_'))
}
