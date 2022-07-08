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
    #' @param params Additional parameters to dbGetQuery.
    #' @return A data.frame 
    readQuery = function(sqltext, params = NULL) {
      conn <- self$getConnection()
#####
     # DBI::dbGetQuery(conn, sqltext, params = params)
##### 
      ### iulian:

      res <- RPostgres::dbSendQuery(conn, sqltext, params = params)
      tps <- RPostgres::dbColumnInfo(res) # get the column types

      tps <- tps[grepl('json', tps$.typname, fixed = TRUE), 'name'] # look for the names of json columns

      if(length(tps) == 0 ){ # we're done here, no json
        return(RPostgres::dbFetch(res))
      }
      ## we have json, fetch some rows at the time, transform and glue:
      while (!RPostgres::dbHasCompleted(res)) {
        chunk <- RPostgres::dbFetch(res, 1000)

        for(cname in tps){ # maybe more than one column
          indx <- which(colnames(chunk) == cname) # index of the column to know where to paste the transformed json
          x <- lapply(chunk[,cname], jsonlite::fromJSON) # transform json to list of lists
          patch <- Reduce(rbind, lapply(x, as.data.frame)) #  reduce the above to a dataframe
          patch <- as.data.frame(sapply(patch,  function(x){
           tryCatch(as.numeric(x), warning = function(w) if(grepl('coercion', w)) x)
          }, simplify = FALSE)) # transform to numeric everything we can 
          # replace the json col with the patch:
          if(indx > 1){
            temp <- cbind(chunk[,1:indx-1], patch)
          } else {
            temp <- patch
          }
          if(length(colnames(chunk)) > indx){
            temp <- cbind(temp, chunk[,(indx+1):length(colnames(chunk))])
          }
          chunk <- temp 
        }
        result <- tryCatch(rbind(result,chunk), error = function(e) if(grepl('not found', e)) chunk) # error handler for the first time
        
      }
      return(result)  

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
qLoad <- function(x,sqltext, params = NULL){
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
  out[, .trim_hidden_fields(colnames(out)),drop=FALSE]
}


#' @title List all tables accessible through a SQLFlex object
#' @param x a SQLFlexClient object
#'@export

showTables <- function(x){
 out <- qLoad(x, "select case table_schema 
                             when 'public' then table_name 
                             else table_schema || '.' || table_name
                             end as table_name, table_type, 
                      pg_size_pretty(pg_table_size(table_schema || '.' || table_name )) as size_in_db
                      from information_schema.tables 
                      where table_schema not in ('information_schema', 'pg_catalog') 
                      order by 1 ")
 out$`number_of_rows` <- unlist(sapply(out$table_name, function(y){
            qLoad(x, paste0('select count(1)::integer from ', y ))
 }))
 
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
  qLoad(x, sql)
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
#    qLoad(db, sql1)
#    sql2 <- "select pg_size_pretty(pg_table_size('xx_tmp')) as size"
#    db_size <- qLoad(db, sql2)
#    qLoad(db, 'drop table xx_tmp')
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
    r_size <- tryCatch(qLoad(db, do_code), error = function(e){
      tot_nrows <- strsplit(e$message, 'ERROR:|\\n')[[1]][2] %>% as.numeric()

      rws <- round(tot_nrows * rowsamp/100)
      sql3 <- paste0('select * from (',y,') xx limit ', rws)
      tmp <- qLoad(db, sql3)
      format(object.size(tmp) * 100/rowsamp, units='auto', standard = 'SI')
    } )
    
    
    out <- data.frame(object = lbl,  'estimated.size' = r_size)
    out
  },simplify = FALSE) %>% Reduce(rbind,.) 
  
}


# helper function to avoid loading useless (or disclosing) columns:
.trim_hidden_fields <- function(cols){
  #first pass:

  for (r in getOption('hidden.fields.regexes')){
    cols <- grep(r, cols, value = TRUE, perl = TRUE, invert = TRUE)
  }
  cols
}

#' wrapper for qLoad, takes care of some parsing  
#' @import dsSwissKnife
#'@export
loadQuery <- function(x,table_name, cols = '*', where_clause = NULL, params = NULL){
  cols <- dsSwissKnife:::.decode.arg(cols)
  where_clause <- dsSwissKnife:::.decode.arg(where_clause)
  # first check the table name
    # take care of the schema.table bother:
#  qualified <- strsplit(table_name, '.', fixed = TRUE)[[1]]
#  if(length(qualified) == 1){
#    table_name <- paste0('public.', table_name)
#  }
  # must be present in the db: 
  tbls  <- showTables(x)
  table_name <- intersect(tbls$table_name, table_name)
  if(length(table_name) != 1){   # exactly one 
     stop(paste0('Table ', table_name,  " doesn't exist."))
  }  
  # ok, it's there, no, we want the list of columns exactly as in the table, no functions or any other funny business:
  # first get rid of accidental spaces:
  cols <- gsub('\\s*', '', cols)
  if(length(cols) > 1 || length(cols) == 1 && cols != '*' ){
    cols_in <- showColumns(x,table_name)$column_name
    retcols <- intersect(tolower(cols), c(tolower(cols_in), '*'))
  }
  if (cols == '*'){
    retcols <- '*'
  }
  if (length(retcols) == 0 ){
    stop(paste0('Only column names are permitted.', tolower(cols_in)))
  }
  # sanitize a bit the where clause:
  if(!is.null(where_clause)){
    where_clause <- tolower(where_clause)
    offenders <- ';|group by|drop|delete|trunc'
    test_injection <- strsplit(where_clause, offenders, fixed = TRUE)[[1]]
    if(length(test_injection) > 1){
      stop('This looks like a sql injection attempt. Not executing.')
    }
  } else {
    where_clause = '1=1'
  }
  # ok, done.
  sqltext <- paste0('select ', paste(retcols, collapse = ', '), ' from ', table_name, ' where ', where_clause)
  
  out <- qLoad(x, sqltext, params)
#  if(!.dsBase_isValidDSS(out)){
#    out <- out[0,]  # nothing if less than 5 rows (normally)
#  }
  out
  
}


