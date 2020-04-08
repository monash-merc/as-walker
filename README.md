# as-walker
asfiles.py script can be used to automate walking AS SFTP service and gather information regarding the available data.

# Running walker script
- Install python dependencies by running
`pip install requirements.txt `
- This script can be configured by changing `config.yml` file located in the root directory. You can change values in 
provided sample `config.yaml` file 
```
    epns_path: 'data/monash_epns'
    hostname: 'sftp.synchrotron.org.au'
    user: 'help@massive.org.au'
    key_path : '/home/ubuntu/.ssh/mx_key'
```
- Run `python asfiles.py`

# Output
asfiles.py script runs multiple processing pools that each walk a subset of the EPNs and record the files in separate sqlite DBs. The schema of sqlite db is as follows:

`CREATE TABLE epns(epn TEXT,size,modified, complete INTEGER DEFAULT 0, bytesTransferred INTEGER DEFAULT 0);`

```
0|epn|TEXT|0||0
1|size||0||0
2|modified||0||0
3|complete|INTEGER|0|0|0
4|bytesTransferred|INTEGER|0|0|0
```
