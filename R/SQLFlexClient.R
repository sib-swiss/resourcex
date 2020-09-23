#' SQL database resource client
#'
#' Resource client that connects to a SQL database supported by DBI
#'
#' @docType class
#' @format A R6 object of class SQLFlexClient
#' @import R6
#' @import resourcer
#' @export
SQLFlexClient <- R6::R6Class(
  "SQLFlexClient",
  inherit = ResourceClient,
  public = list(
    #' @description Creates a SQLResourceClient from a resource.
    #' @param resource The resource object.
    #' @param dbi.connector An optional DBIResourceConnector object. If not provided, it will be looked up in the DBIResourceConnector registry.
    #' @return The SQLResourceClient object.
    initialize = function(resource, dbi.connector = NULL) {
      super$initialize(resource)
      if (is.null(dbi.connector)) {
        private$.dbi.connector <- findDBIResourceConnector(resource)
      } else {
        private$.dbi.connector <- dbi.connector
      }
      if (is.null(private$.dbi.connector)) {
        stop("DBI resource connector cannot be found: either provide one or register one.")
      }
    },
    #' @description Execute a query in the database and retrieve the results.
    #' @param sqltext a character, the query text
    #' @param ... Additional parameters to dbGetQuery.
    #' @return A data.frame 
    readQuery = function(sqltext, ...) {
      conn <- self$getConnection()
      DBI::dbGetQuery(conn, sqltext, ...)
    },
    
  
    #' @description Silently close the DBI connection.
    close = function() {
      conn <- super$getConnection()
      if (!is.null(conn)) {
        private$.dbi.connector$closeDBIConnection(conn)
        super$setConnection(NULL)
      }
    }
    
  ),
  private = list(
    .dbi.connector = NULL,
    getConnection = function() {
      conn <- super$getConnection()
      if (is.null(conn)) {
        resource <- super$getResource()
        conn <- private$.dbi.connector$createDBIConnection(resource)
        super$setConnection(conn)
      }
      conn
    }
    
  )
)

#' @title Load the result of a query from a database
#' @description Call the readQuery method of a SQLFlexClient object and retrieve the results.
#' @param x a SQLFlexClient object
#' @param sqltext a character, the query text
#' @param ... Additional parameters to dbGetQuery.
#' @return A data.frame 
#' @import dsSwissKnife
#'@export
loadQuery <- function(x,sqltext, ...){
  sqltext <- dsSwissKnife:::.decode.arg(sqltext)
  if ("SQLFlexClient" %in% class(x)) {
    return(x$readQuery(sqltext, ...))
  } else {
    stop("Trying to read data from  an object that is not a SQLFlexClient: ", paste0(class(x), collapse = ", "))
  }
}
