var resourcex = {
  settings: {
    "title": "SQLFlexClient",
    "description": "Provides a queryable SQL client ",
    "types": [
      {
        "name": "sql",
        "title": "SQL query",
        "description": "Resource provides a connection to a database accessible using [DBI](https://www.r-dbi.org) against which SQL queries can be executed.",
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
          ],
          "required": [
            "driver", "host", "port", "db"
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
      }
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
}
