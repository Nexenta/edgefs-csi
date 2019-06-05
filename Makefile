PLUGIN_NAME=edgefs-csi
IMAGE_NAME=$(PLUGIN_NAME)
DOCKER_FILE=Dockerfile
REGISTRY ?= edgefs
LABEL ?= segments

.PHONY: all csi

all: csi

# make sure that `dep` already installed
.get:
	dep ensure -v
	touch $@

csi: .get
	# dynamic builds faster, but not good for Dockerfile
	# static build
	CGO_ENABLED=0 GOOS=linux go build -a -ldflags '-extldflags "-static"' -o $(PLUGIN_NAME) main.go

build-container: csi
	docker build  -f $(DOCKER_FILE) -t $(IMAGE_NAME) .

push-container: build-container
	docker tag  $(IMAGE_NAME) $(REGISTRY)/$(IMAGE_NAME):$(LABEL)
	docker push $(REGISTRY)/$(IMAGE_NAME):$(LABEL)

skaffold:
	# temp hack until skaffold #543 is fixed
	echo "        EDGEFS_IMAGE: $(REGISTRY)/edgefs:$(EDGEFS_VERSION)" >> skaffold.yaml
	export VERSION=$(EDGEFS_VERSION) && skaffold -v info build --insecure-registry=$(DOCKER_REGISTRY) -f skaffold.yaml
	git checkout skaffold.yaml

test:
	go test -count=1 -v ./csi -run TestAttachISCSIVolume #$TestAttachISCSIVolume #TestGetISCSIDevices

clean:
	-rm -rf $(PLUGIN_NAME) src .get
