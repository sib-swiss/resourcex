var resourcer = {
  settings: {
    "title": "Base Resources",
    "description": "Provides some of the most common resources: file, mysql, mariadb, postgresql, mongodb, ssh, scp.",
    "web": "https://github.com/obiba/resourcer",
    "categories": [
      {
        "name": "database",
        "title": "Database",
        "description": "Data are stored in a database."
      },
    ],
    "types": [
      {
        "name": "sql",
        "title": "SQL query",
        "description": "Resource is a connection to a database accessible using [DBI](https://www.r-dbi.org) against which SQL queries can be executed. The data can be read as a standard `data.frame` or as a [dbplyr](https://dbplyr.tidyverse.org/)'s `tbl`.",
        "tags": ["database"],
        "parameters": {
          "$schema": "http://json-schema.org/schema#",
          "type": "array",
          "items": [
            {
              "key": "driver",
              "type": "string",
              "title": "Database engine",
              "description": "Database engine implementing the [DBI](https://www.r-dbi.org).",
              "enum": [
                {
                  "key": "mariadb",
                  "title":"MariaDB"
                },
                {
                  "key": "mysql",
                  "title":"MySQL"
                },
                {
                  "key": "postgresql",
                  "title":"PostgreSQL"
                }
              ]
            },
            {
              "key": "host",
              "type": "string",
              "title": "Host",
              "description": "Remote host name or IP address of the database server."
            },
            {
              "key": "port",
              "type": "integer",
              "title": "Port",
              "description": "Database port number."
            },
            {
              "key": "db",
              "type": "string",
              "title": "Database",
              "description": "The database name."
            },
            {
              "key": "table",
              "type": "string",
              "title": "Table",
              "description": "The table name."
            }
          ],
          "required": [
            "driver", "host", "port", "db", "table"
          ]
        },
        "credentials": {
          "$schema": "http://json-schema.org/schema#",
          "type": "array",
          "description": "Credentials are required.",
          "items": [
            {
              "key": "username",
              "type": "string",
              "title": "User name",
              "description": "Valid database user name."
            },
            {
              "key": "password",
              "type": "string",
              "title": "Password",
              "format": "password",
              "description": "The user's password."
            }
          ],
          "required": [
            "username", "password"
          ]
        }
      },
    ]
  },
  asResource: function(type, name, params, credentials) {

          return {
              name: name,
              url: params.driver + "://" + params.host + ":" + params.port + "/" + params.db,
              identity: credentials.username,
              secret: credentials.password
          }
}
