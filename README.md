# TrinoQ

This a convient cli to query data from Trino.



## Installation


```python
pipx install git+https://github.com/mmngreco/trinoq
```



## Usage


First you need to setup Trino URL connection string, it's encoded as follows:

```bash
export TRINO_URL=<http_scheme>://<host>:<port>?<user>=<user@google.com>&<catalog>=<value>
export TRINO_URL=https://host:443?user=user@google.com
```


If everything is ok, you can start querying data:

```bash
trinoq --help
trinoq <query>
trinoq "select 1"
```

