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
     -p, --port string   Port to run the master HTTP server (default "8081")
   :~$ ./claud master -i localhost -p 8088
   Launching master process...
   
   ```

6. Once all desired masters are up, you can make HTTP requests to them. The three supported endpoints for now are _/_, _/masters_ and _/leader_ with GET methods:

   ```bash
   :~$ curl localhost:8088/
   Hi there, I am master 6e67dd68-f115-4857-a736-0629c33b384d
   :~$ curl localhost:8088/masters
   ALIVE MASTER NODES
   UUID: 6e67dd68-f115-4857-a736-0629c33b384d IP:PORT: 192.168.1.49:46555
   UUID: 4f679970-1d55-4dcb-ac44-efde6a2be519 IP:PORT: 192.168.1.49:45694
   UUID: 78650510-642d-42bb-9bf2-a76a51c71e02 IP:PORT: 192.168.1.49:42486
   :~$ curl localhost:8088/leader
   My leader status: NOT LEADER
   Leader UUID is: 4f679970-1d55-4dcb-ac44-efde6a2be519
   :~$ 
   
   ```
