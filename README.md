# Abstract Database Types
MVR abstracts to Postgres types. What this means is anytime a type is needed it should be expressed as a Postgres type. IE parameter types, overriding column types. 

# Parameter Binding
MVR gives you a lot flexibility to craft a query and bind parameters to it. 

## Parameters in Queries

MVR utilizes whatever underlying mechnism is used by the database system. It executes the query passing an array of values to bind to each parameter. The best way to explain this is to just show some examples.

[Postgres](https://pkg.go.dev/github.com/jackc/pgx/v5#Conn.Prepare)
```sql
SELECT * FROM public.users WHERE user = $1 AND active = $2
```

[MS SQL Server](https://pkg.go.dev/github.com/jackc/pgx/v5#Conn.Prepare)
```sql
SELECT * FROM dbo.users WHERE user = @p1 AND active = @p2
```

[Snowflake](https://pkg.go.dev/github.com/snowflakedb/gosnowflake#hdr-Binding_Parameters)
```sql
SELECT * FROM PUBLIC.USERS WHERE USER = ? AND active = ?
```

## Binding Values to Parameters
MVR defines the parameter name and type through YAML. Then you can bind the value through YAML or environment variable.
Names and types ensures that the parameters are bound in the correct order and have the correct type. Remember that all types are abstracted to the Postgres type system so this should match a Postgres type.

The value needs to be the correct type to bind which forces a definition structure to tell MVR how to bind the value.
This is complicated by the fact that it can be bound through an environment variable which will always be a string. If a type is not specified MVR will default to the value as a string.

While this looks like it is doing a named bind, MVR is not doing that. It is taking all the keys and ordering the keys so the values will align. For example using Snowflake you can use whatever name you want, the names just have to be ordered to match the `?` you want to bind with.

**Technically** MS SQL Server is using named parameters, but just view them as an ordered array of parameters.

### Postgres
```yaml
#excerpted yaml
sql: |
  SELECT * FROM public.users WHERE user = $1 AND active = $2
params:
  # This is for order not binding to a named parameter
  P1:
    value: user_to_find
    type: TEXT
  P2:
    value: true
    type: BOOLEAN
```

### MS SQL Server example
```yaml
sql: |
  SELECT * FROM dbo.users WHERE user = @p1 AND active = @p2
params:
  p1:
    value: user_to_find
    type: TEXT
  p2:
    value: true
    type: BOOLEAN
```
### Snowflake
```yaml
sql: |
  SELECT * FROM PUBLIC.USERS WHERE USER = ? AND active = ?
params:
  # This can be named anything. Just make sure it gets sorted before the 
  # the next parameter
  param_1:
    value: user_to_find
    type: TEXT
  param_2:
    value: true
    type: BOOLEAN
```

Now each one of these can be overriden at execution time with:
```bash
# postgres
export MVR_PARAM_P1='another_user'
export MVR_PARAM_P2='false'
# ms sql
export MVR_PARAM_p1='another_user'
export MVR_PARAM_p2='false'
# snowflake
export MVR_PARAM_param_1='another_user'
export MVR_PARAM_param_2='false'
```

### If all parameters are strings
When all the parameters will be bound as strings you can bypass the definition and just bind the values. Here is the Postgres example without the boolean parameter.

```yaml
sql: |
  SELECT * FROM public.users WHERE user = $1
```
And to bind the user:
```bash
export MVR_PARAM_P1='another_user'
```

