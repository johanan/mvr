# Install
Run the install script 
```shell
curl -sf https://raw.githubusercontent.com/johanan/mvr/refs/heads/main/mvr_install.sh | sh
```

# Connection Strings
Connection strings need to be passed by environment variables. No connection information should be in any config files or arguments. 

`MVR_SOURCE` and `MVR_DEST` are the variables to set.

# Abstract Database Types
MVR abstracts to Postgres types. What this means is anytime a type is needed it should be expressed as a Postgres type. IE parameter types, overriding column types. 

# Timestamps
For the most part MVR will keep the timezone or the lack of a timezone into the output file. This means RFC3339 without timezone info for CSV and JSONL. And for parquet this is a logical type with `isAdjustedToUTC` set to true for timezone types and false for no timezone types.

Postgres and MS SQL both have only two timestamp types which fits neatly into having a timezone or not. Snowflake has three types: `TIMESTAMP_NTZ`, `TIMESTAMP_TZ`, and `TIMESTAMP_LTZ`. The first two types do exactly what they say. NTZ stands for no timezone and TZ stands for timezone. These follow the above rules.

`TIMESTAMP_LTZ` stands for local time zone. Why is this a timestamp type? *I am not sure*.🤷 What it means, though, is that the value of the timestamp depends on the timezone.

How does MVR treat it? It is treated as a timestamp without a timezone. Because that is what it is. If the timezone changes then it works just like a timestamp without a timezone. Unfortunately Snowflake will convert it to a timestamp with the session timezone. To ensure you can get the value you need out of it, set the `timezone` parameter in the connection string to the timezone it needs to be in. 

**HOLD ON** remember that it will come from Snowflake as having a timezone and if writing to parquet it will be converted to UTC. If you are -0400 then 4 hours will be added to the time in the parquet. If you need the *exact* timestamp in the column set the timezone to UTC, ie `snowflake://connection_info@account/db/schema?timezone=UTC`.

See [Snowflake Connection Parameters](https://pkg.go.dev/github.com/snowflakedb/gosnowflake#hdr-Connection_Parameters)

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

### MS SQL Server
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

