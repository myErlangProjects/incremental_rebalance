# incremental_rebalance
Automatic resource rebalance(incremental) among erlang applications in k8s. 
In this application zookeeper has been used as the group coordinator. Zookeeper client application [`erlzk`](https://github.com/huaban/erlzk) is used.

Consistant hashing (hash ring) has been used in order to minimize the resource realocation while achieving resource equally dictribution during node joining and leaving of the erlang cluster. Consistent hashing algorithm implementaion lib [`hash_ring`](https://github.com/sile/hash_ring) is used.

Erlang peer discovery within k8s cluster is achieved with [`erlang_k8s_cluster`](https://github.com/myErlangProjects/erlang_k8s_cluster) application.

## Packaging application

### Create rebar release
```
rebar3 release
```
or in order to generate tarball
```
rebar3 tar
```

### Create Docker image
```
docker image build -t {IMAGE-NAME} .
```
or build image with `docker-compose` with defined image name in docker-compose file.
```
docker-compose build
```

## Running application

In order to run this application, Zookeeper service is a pre-equisite.
And also Znode (eg: /zk) need to be created which will be used as chroot for application specific ephemeral znodes.
Docker-compose and k8s manifest will create above pre-requisites before starting of incremental rebalance erlang application.

### Docker compose
```
docker-compose up
```
Clear out
```
docker-compose down
```

### K8s
Change working directory 
`cd k8s/manifests`
```
kubectl apply -f k8s.io.zookeeper.yaml
kubectl apply -f erlang-k8s-incremental-rebalance.yaml
```
Clear out
```
kubectl delete -f erlang-k8s-incremental-rebalance.yaml
kubectl apply -f k8s.io.zookeeper.yaml

```
