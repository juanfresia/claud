# CLAUD (Sistemas Distribuidos II, FIUBA)

### Currently on version: v0.1

Free and academic cluster developed in Golang 1.11 by Fresia Juan Manuel & Alejandro Martín García for Sistemas Distribuidos II at FIUBA.

## Steps for launching

1. Download or `git clone` this repository.

2. Either [have Go 1.11 installed](https://golang.org/dl/) or your machine, or [Docker](https://docs.docker.com/install/linux/docker-ce/ubuntu/) (both will work).

3. Navigate to the folder where you have this project's source. Depending on your previous choice, run either `make all` if you are using Go, or `make docker` otherwise.

4. Locate the _claud_ executable under the _bin_ folder. Running it with `--help` flag will show you the help:

   ```bash
   :~$ ls
   bin  claud  Dockerfile  go.mod  go.sum  Makefile  master  README.md
   :~$ cd bin/
   :~$ ./claud --help
   CLAUD: Cluster for Sistemas Distribuidos II, FIUBA.
   
   Usage:
     claud [flags]
     claud [command]
   
   Available Commands:
     help        Help about any command
     master      Launches claud master process
     slave       Launches claud slave process
     version     Print version and build status
   
   Flags:
     -h, --help   help for claud
   
   Use "claud [command] --help" for more information about a command.
   :~$ 
   
   ```

5. Launch as many desired masters with the `master` command (Note each master node must be launched on a different IP:Port pair). You can check the command's help with `./claud help master`:

   ```bash
   :~$ ./claud help master
   Launches claud master process
   
   Usage:
     claud master [flags]
   
   Flags:
     -h, --help          help for master
     -i, --ip string     IP to run the master HTTP server (default "localhost")
     -n, --masters-total uint   Total number of masters to consider in the cluster (default 3)
     -p, --port string   Port to run the master HTTP server (default "8081")
   :~$ ./claud master -i localhost -p 8088
   Launching master process...
   CLUSTER SIZE: 2
   Master Kernel is up!
   
   ```

6. Once all desired masters are up, you can make HTTP requests to them. The supported endpoints are: _/_, _/masters_, _/leader_, _/slaves_ and _/jobs_:

   ```bash
   :~$ curl localhost:8088/ | jq
   % Total    % Received % Xferd  Average Speed   Time    Time     Time  Current Dload  Upload   Total   Spent    Left  Speed
   100    50  100    50    0     0  50000      0 --:--:-- --:--:-- --:--:-- 50000
   {
   "my_UUID": "42d47c7e-9e7a-4d13-a9b9-d1f9f03e2c75"
   }
   :~$ curl localhost:8088/masters | jq
   % Total    % Received % Xferd  Average Speed   Time    Time     Time  Current Dload  Upload   Total   Spent    Left  Speed
   100   225  100   225    0     0   219k      0 --:--:-- --:--:-- --:--:--  219k
   {
   "alive_masters": [
      {
         "UUID": "1e3ac627-07f9-46cc-b4ac-1564977cb3eb",
         "status": "LEADER"
      },
      {
         "UUID": "42d47c7e-9e7a-4d13-a9b9-d1f9f03e2c75",
         "status": "NOT LEADER"
      },
      {
         "UUID": "e81da3da-3cd1-484e-9138-f7addac82f54",
         "status": "NOT LEADER"
      }
      ] 
   }
   
   ```
   
   7. For launching a job, you will first need to launch slave nodes. Launch as many desired slaves with the `slave` command (Note each slave node must be launched on a different IP:Port pair). You may need to launch them with `sudo`to properly run jobs with Docer. You can check the command's help with `./claud help slave`:

   ```bash
   :~$ ./claud help slave
   Launches claud slave process
   
   Usage:
     claud slave [flags]
   
   Flags:
     -h, --help               help for slave
     -i, --ip string          IP to run the slave HTTP server (default "localhost")
     -n, --masters-total uint Total number of masters to consider in the cluster (default 3)
     -m, --memory uint        Memory size (in KiB) yielded to claud for running processes (default 1024)
     -p, --port string        Port to run the slave HTTP server (default "8081")
   :~$ sudo ./claud slave -p 8090 -m 2048
   Launching slave process...
   CLUSTER SIZE: 2
   Slave Kernel is up!

   ```

8. Write the information of any job you wish to launch in a JSON formatted file like the following:

   ```json
   {
      "mem": 700,
      "name": "myFirstJob",
      "image": "nginx"
   }
   ```
9. Perform the proper HTTP requests to the _/jobs_ endpoint of any master of the cluster for properly launching, seeing or stopping jobs:

   ```bash
   :~$ curl -X POST -d @YOUR_JSON_FILE localhost:8081/jobs | jq
   % Total    % Received % Xferd  Average Speed   Time    Time     Time  Current Dload  Upload   Total   Spent    Left  Speed
   100   106  100    50  100    56  10000  11200 --:--:-- --:--:-- --:--:-- 21200
   {
      "job_id": "0eb26758-427d-4c67-83f6-e6a16b9e57e5"
   }
   :~$ curl -X GET localhost:8081/jobs | jq
   % Total    % Received % Xferd  Average Speed   Time    Time     Time  Current Dload  Upload   Total   Spent    Left  Speed
   100   570  100   570    0     0   556k      0 --:--:-- --:--:-- --:--:--  556k
   {
      "jobs": [
         {
            "asigned_slave": "87a1eb99-515e-4ae5-bfcd-8c6e3026418b",
            "image": "nginx",
            "job_full_name": "myFirstJob-0eb26758-427d-4c67-83f6-e6a16b9e57e5",
            "job_id": "0eb26758-427d-4c67-83f6-e6a16b9e57e5",
            "last_update": "2019-02-26 21:50:35.951750705 -0300 -03 m=+1262.201623451",
            "status": "RUNNING"
         }
      ]
   }
   ```
