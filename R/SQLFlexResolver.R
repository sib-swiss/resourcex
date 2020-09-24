' SQL Database Resource Flex resolver
#'
#' The resource is SQL database.
#'
#' @docType class
#' @format A R6 object of class SQLResourceResolver
#' @import R6
#' @import resourcer
#' @export
SQLFlexResolver <- R6::R6Class(
  "SQLFlexResolver",
  inherit = ResourceResolver,
  public = list(
    
    #' @description Check that the provided resource has a registered DBIResourceConnector.
    #' @param x The resource object to evaluate.
    #' @return A logical.
    isFor = function(x) {
      if (super$isFor(x)) {
        !is.null(findDBIResourceConnector(x)) && is.null(x$format)
      } else {
        FALSE
      }
    },
    
    #' @description Creates a SQLFlexClient instance from provided resource.
    #' @param x A valid resource object.
    #' @return A SQLFlexClient object.
    newClient = function(x) {
      if (self$isFor(x)) {
        SQLFlexClient$new(x)
      } else {
        NULL
      }
    }
    
  )
)