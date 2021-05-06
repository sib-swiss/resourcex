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
    #' @description Creates a SQLFlexClient from a resource.
    #' @param resource The resource object.
    #' @param dbi.connector An optional DBIResourceConnector object. If not provided, it will be looked up in the DBIResourceConnector registry.
    #' @return The SQLResourceClient object.
    initialize = function(resource, dbi.connector = NULL) {
      super$initialize(resource)
      if (is.null(dbi.connector)) {
        private$.dbi.connector <- resourcer::findDBIResourceConnector(resource)
      } else {
        private$.dbi.connector <- dbi.connector
      }
      if (is.null(private$.dbi.connector)) {
        stop("DBI resource connector cannot be found: either provide one or register one.")
      }
    },
    
    #' @description Get or create the DBI connection object that will access the resource.
    #' @return The DBI connection object.
    getConnection = function() {
      conn <- super$getConnection()
      if (is.null(conn)) {
        resource <- super$getResource()
        conn <- private$.dbi.connector$createDBIConnection(resource)
        super$setConnection(conn)
      }
      conn
    },
    
    #' @description Execute a query in the database and retrieve the results.
    #' @param sqltext a character, the query text
    #' @param ... Additional parameters to dbGetQuery.
    #' @return A data.frame 
    readQuery = function(sqltext, params = NULL) {
      conn <- self$getConnection()

      DBI::dbGetQuery(conn, sqltext, params = params)
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
    .dbi.connector = NULL
    
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
loadQuery <- function(x,sqltext, params = NULL){
  sqltext <- dsSwissKnife:::.decode.arg(sqltext)
  if ("SQLFlexClient" %in% class(x)) {
    out <- x$readQuery(sqltext, params = params)
    # transform characters and dates into factors (dsBaseClient doesn't like dates, dsSwissKnifeClient::dssShowFactors likes factors)
    out <- as.data.frame(sapply(out, function(y){
      if(length(intersect(class(y) , c('character', 'Date', 'POSIXct', 'POSIXlt', 'POSIXt'))) >0 ){
        return(factor(y))
      } else {
        return(y)
      }
    }, simplify = FALSE))
  } else {
    stop("Trying to read data from  an object that is not a SQLFlexClient: ", paste0(class(x), collapse = ", "))
  }
  out
}


#' @title List all tables accessible through a SQLFlex object
#' @param x a SQLFlexClient object
#'@export

showTables <- function(x){
 out <- loadQuery(x, "select case table_schema 
                             when 'public' then table_name 
                             else table_schema || '.' || table_name
                             end as table_name, table_type, 
                      pg_size_pretty(pg_table_size(table_schema || '.' || table_name )) as size_in_db
                      from information_schema.tables 
                      where table_schema not in ('information_schema', 'pg_catalog') 
                      order by 1 ")
 
 out
}

#' @title List all columns of a table
#' @param table_name name of the table (maybe qualified with schema name)
#' @param x a SQLFlexClient object
#'@export
showColumns <- function(x, table_name){
  schema_name <- 'public' # default
  if(grepl('\\.', table_name)){
    spl <- strsplit(table_name, '\\.')[[1]]
    schema_name <- spl[1]
    table_name <- spl[2]
  }
  sql <- paste0("select column_name, data_type from information_schema.columns where table_schema = '",
                  schema_name, "' and table_name = '", table_name, "'")
  loadQuery(x, sql)
}

#' @title Estimate the memory size of one or more views
#' @param views a character vector, names of the views (or sql queries)
#' @param db a SQLFlexClient object
#' @export
viewSize<- function(db, views, rowsamp = 5){

  views <- dsSwissKnife:::.decode.arg(views)
  sapply(views, function(x){
    y <- x #set default
    if (!grepl("^\\s*select", x, ignore.case = TRUE)) {
      y <- paste0("select * from ", x)
    }
###########################################################    
#   no more of this temp table nonsense:                  #
###########################################################    
#    sql1 <- paste0("create temporary table xx_tmp as ", y)
#    loadQuery(db, sql1)
#    sql2 <- "select pg_size_pretty(pg_table_size('xx_tmp')) as size"
#    db_size <- loadQuery(db, sql2)
#    loadQuery(db, 'drop table xx_tmp')
########################################################################  
  lbl <- gsub('\\n',' ',x) # no carriage returns for the label
    if (nchar(lbl) > 32){
      lbl <- paste0(substr(lbl,1,32),'...')
    }
  
    do_code <- paste0("DO 
     $$DECLARE
      rec record;
      rows integer;
      query varchar(4000);
     BEGIN
        query := $abc$",y,"$abc$;
        FOR rec IN EXECUTE 'EXPLAIN ANALYZE ' || query LOOP
          rows := substring(rec.\"QUERY PLAN\" FROM 'actual time.*rows=([[:digit:]]+)');
          EXIT WHEN rows IS NOT NULL;
        END LOOP;
        RAISE EXCEPTION USING MESSAGE=rows;
     END$$;")
    r_size <- tryCatch(loadQuery(db, do_code), error = function(e){
      tot_nrows <- strsplit(e$message, 'ERROR:|\\n')[[1]][2] %>% as.numeric()

      rws <- round(tot_nrows * rowsamp/100)
      sql3 <- paste0('select * from (',y,') xx limit ', rws)
      tmp <- loadQuery(db, sql3)
      format(object.size(tmp) * tot_nrows/rowsamp, units='auto', standard = 'SI')
    } )
    
    
    out <- data.frame(object = lbl,  'estimated.size' = r_size)
    out
  },simplify = FALSE) %>% Reduce(rbind,.) 
  
}

