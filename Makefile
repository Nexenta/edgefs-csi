PLUGIN_NAME=edgefs-csi
IMAGE_NAME=$(PLUGIN_NAME)
DOCKER_FILE=Dockerfile
REGISTRY ?= $(DOCKER_REGISTRY)/nexenta
EDGEFS_VERSION ?= 1.1.215

.PHONY: all csi

all: csi

.get:
	GO111MODULE=on go mod download
csi: .get
	# dynamic builds faster, but not good for Dockerfile
	#GOPATH=`pwd` go build -o $(PLUGIN_NAME) main.go
	# static build
	GO111MODULE=on CGO_ENABLED=0 GOOS=linux go build -a -ldflags '-extldflags "-static"' -o $(PLUGIN_NAME) main.go

build-container: csi
	docker build --build-arg EDGEFS_IMAGE=$(REGISTRY)/edgefs:$(EDGEFS_VERSION) -f $(DOCKER_FILE) -t $(IMAGE_NAME) .

push-container: build-container
	docker tag  $(IMAGE_NAME) $(REGISTRY)/$(IMAGE_NAME):$(EDGEFS_VERSION)
	docker push $(REGISTRY)/$(IMAGE_NAME):$(EDGEFS_VERSION)

skaffold:
	# temp hack until skaffold #543 is fixed
	echo "        EDGEFS_IMAGE: $(REGISTRY)/edgefs:$(EDGEFS_VERSION)" >> skaffold.yaml
	export VERSION=$(EDGEFS_VERSION) && skaffold -v info build --insecure-registry=$(DOCKER_REGISTRY) -f skaffold.yaml
	git checkout skaffold.yaml

test:
	GOPATH=`pwd` go test -count=1 -v ./csi -run TestAttachISCSIVolume #$TestAttachISCSIVolume #TestGetISCSIDevices

clean:
	-rm -rf $(PLUGIN_NAME) src .get
