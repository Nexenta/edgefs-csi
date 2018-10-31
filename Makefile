PLUGIN_NAME=edgefs-csi
IMAGE_NAME=$(PLUGIN_NAME)
DOCKER_FILE=Dockerfile
REGISTRY=10.3.30.75:5000
IMAGE_TAG=latest

.PHONY: all nfs 

all: nfs

.get:
	GOPATH=`pwd` go get || true
	# to workaround log_dir, etc panic ... ugly
	rm -rf src/github.com/kubernetes-csi/drivers/vendor/github.com/container-storage-interface
	rm -rf src/github.com/kubernetes-csi/drivers/vendor/google.golang.org
	rm -rf src/github.com/kubernetes-csi/drivers/vendor/github.com/golang
	rm -rf src/k8s.io/kubernetes/vendor/github.com/golang
	GOPATH=`pwd` go get || true
	touch $@

nfs: .get
	GOPATH=`pwd` go build -o $(PLUGIN_NAME) main.go

build-container: nfs 
	docker build -f $(DOCKER_FILE) -t $(IMAGE_NAME) .

push-container: build-container
	docker tag  $(IMAGE_NAME) $(REGISTRY)/$(IMAGE_NAME):$(IMAGE_TAG)
	docker push $(REGISTRY)/$(IMAGE_NAME):$(IMAGE_TAG)

test:
	GOPATH=`pwd` go test -count=1 -v ./csi

clean:
	-rm -rf $(PLUGIN_NAME) src .get
