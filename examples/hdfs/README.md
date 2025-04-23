# HDFS
Runs Lakekeeper without Authentication and Authorization (unprotected).
Lakekeeper uses Kerberos to authenticate against HDFS. The Keytab is obtained externally and mounted in the Lakekeeper pod.

To run the example run the following commands:

```bash
cd examples/hdfs
docker compose up
```

Now open your Browser:
* Jupyter: [http://localhost:8888](http://localhost:8888)
* Lakekeeper UI: [http://localhost:8181](http://localhost:8181)
