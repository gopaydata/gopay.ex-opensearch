The sample `/data` folder can be found in the [component's repository](https://bitbucket.org/kds_consulting_team/kds-team.ex-elasticsearch/src/master/component_config/sample-config/). The [config.json](https://bitbucket.org/kds_consulting_team/kds-team.ex-elasticsearch/src/master/component_config/sample-config/config.json) file represents the configuration, that should be passed to the component in order for the component to run successfully.

In Keboola, the component is set up as a row-based component and thus certain parameters (SSH & DB settings) have to be configured only once, while index specific settings can be configured for each index separately.

## Database and SSH Settings

Elasticsearch extractor currently supports only connection to the Elasticsearch instance over SSH tunnel. For successful connection, all database and SSH properties must be configured.

### Database (`db`) settings

The database host and port need to be provided to correctly connect to the engine and download index data.

Required parameters are:

- **Hostname** (`db.hostname`) - specifies the IP address or URL at which the database is located;
- **Port** (`db.port`) - specifies the accompanying port to the hostname.

The correct JSON specification of the database settings then takes the following form.

```json
{
  ...
  "db": {
      "hostname": "127.0.0.1",
      "port": 8080
    }
  ...
}
```

### SSH (`ssh`) settings

Connection to the Elasticsearch instance via an SSH server is supported by the extractor

Required parameters for SSH section of the configuration are:

- **SSH Hostname** (`ssh.hostname`) - a SSH host, to which a connection shall be made;
- **SSH Port** (`ssh.port`) - an accompanying SSH port to `ssh.hostname`;
- **SSH Username** (`ssh.username`) - a user, which will be used for SSH authentication;
- **SSH Private Key** (`ssh.#private_key`) - an SSH private key.

The final SSH configuration should then look like the one below.

```json
{
  ...
  "ssh": {
      "hostname": "ssh-host-url.cz",
      "port": 22,
      "username": "user-ssh",
      "#private_key": "-----BEGIN OPENSSH PRIVATE KEY-----\nENCRYPTED\nSSH\nKEY\n-----END OPENSSH PRIVATE KEY-----"
    }
  ...
}
```

*Note:* If you're using a predefined JSON configuration schema, the new lines in SSH private key will be automatically replaced by `\n`. However, if you're using the raw JSON to configure the component, you need to escape all new lines by `\n`, in order to inject the private key into the configuration properly.


## Row (index) configuration

Index configuration is tied to a specific index you'd like to download. Users are able to configure the extraction according to their needs by specifying a request body, which will be sent along with the request. Additionally, a `{{date}}` placeholder can be used for a specified date to be injected into an index name (please see **Date** section for more information).

### Index Name (`index_name`)

The `index_name` parameter specifies the name of the index in an Elasticsearch index, which will be downloaded. [Search API](https://www.elastic.co/guide/en/elasticsearch/reference/current/search-search.html) is utilized to download all data from an index. You can use `{{date}}` placeholder to be replaced by settings specified in the placeholder settings section.

### Request Body (`request_body`)

In `request_body`, users are able to specify their custom JSON request body, which will be sent along with a request. For a list of all available attributes, which can be specified in the request body, please see [Request body in Search API](https://www.elastic.co/guide/en/elasticsearch/reference/current/search-search.html) documentation.

It's also possible to specify `size` and `scroll` parameters, to control size of the returned page and length of its availability. If `size` or `scroll` are not specified, default values are used for either of the parameters.

An example of sepcifying a request body may be shown by using the `_source` parameter to only extract requested fields. The request body would then take the following form:

```json
{
    "_source": [
        "_id",
        "_index",
        "_score",
        "_type",
        "click.clicked_at",
        "click.result.display_text",
        "click.result.serp_position",
        "click.result.uri",
        "event",
        "market",
        "offset",
        "query.current_value",
        "query.entered_at",
        "serp.displayed_at",
        "session_id",
        "user_hash"
    ]
}
```

### Date Placeholder Replacement (`date`)

A date placeholder `{{date}}` can be used in specifying an index name. This is especially useful if name of your index changes each day (e.g. data for each day are stored in a separate index).

The date placeholder will be automatically replaced based on the specification of the parameters below.

Parameters:

- **Date Shift** (`shift`) - a date in absolute (`YYYY-MM-DD`) format, or relative format (e.g. today, yesterday, 3 days ago, etc.), specifying by which date the placeholder will be replaced;
- **Date Format** (`format`) - the format of date, which will replace the date placeholder. Accepted formats are listed in [Python strftime documentation](https://docs.python.org/3/library/datetime.html#strftime-and-strptime-format-codes).
- **Time Zone** (`time_zone`) - a time zone, at which the date replacement will be evaluated. Accepted format is any standard [DB timezone specification](https://en.wikipedia.org/wiki/List_of_tz_database_time_zones#List).


### Output Table Name (`storage_table`)

Name of the output table, under which the downloaded index will be stored in Keboola storage.

### Primary Keys (`primary_keys`)

An array of columns, specifying a primary key for the storage table inside Keboola.

### Load Type (`incremental`)

Specifies, whether to use incremental load (`true`) or full load (`false`).