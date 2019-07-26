# Base image for csi service build. Edgefs images contains grpc protobuff interfaces to build with
EDGEFS_IMAGE ?= edgefs/edgefs:1.2.31

# Output registry and image names for csi plugin image
REGISTRY ?= edgefs
LOCAL_REGISTRY ?= 10.3.30.75:5000

# Output plugin name and its image name and tag 
PLUGIN_NAME=edgefs-csi
PLUGIN_TAG=dev

GIT_BRANCH = $(shell git rev-parse --abbrev-ref HEAD | sed -e "s/.*\\///")
GIT_TAG = $(shell git describe --tags)

# use git branch as default version if not set by env variable, if HEAD is detached that use the most recent tag
VERSION ?= $(if $(subst HEAD,,${GIT_BRANCH}),$(GIT_BRANCH),$(GIT_TAG))
COMMIT ?= $(shell git rev-parse HEAD | cut -c 1-7)
DATETIME ?= $(shell date +'%F_%T')
LDFLAGS ?= \
        -extldflags "-static" \
	-X github.com/Nexenta/edgefs-csi/csi/driver.Version=${VERSION} \
	-X github.com/Nexenta/edgefs-csi/csi/driver.Commit=${COMMIT} \
	-X github.com/Nexenta/edgefs-csi/csi/driver.DateTime=${DATETIME}

IMAGE_TAG ?= dev
REGISTRY_PATH=${REGISTRY}/${PLUGIN_NAME}:${PLUGIN_TAG}
DEV_REGISTRY_PATH=${LOCAL_REGISTRY}/${PLUGIN_NAME}:${PLUGIN_TAG}

#Creates csi driver image name (host specific)
BUILD_HOST=$(shell hostname)
SHA256CMD=${SHA256CMD:-shasum -a 256}
BUILD_REGISTRY=build-$(shell echo ${BUILD_HOST} | shasum -a 256 | cut -c1-8)
CSI_IMAGE = $(BUILD_REGISTRY)/edgefs-csi

.PHONY: all
all:
	@echo "Available commands:"
	@echo "  build                           - build source code"
	@echo "  container                       - build drivers container"
	@echo "  push                            - push driver to dockerhub registry (${REGISTRY_PATH})"
	@echo "  push-dev                        - push driver to local dev registry (${DEV_REGISTRY_PATH})"
	@echo ""
	@make print-variables --no-print-directory

.PHONY: print-variables
print-variables:
	@echo "Variables:"
	@echo "  VERSION:    ${VERSION}"
	@echo "  GIT_BRANCH: ${GIT_BRANCH}"
	@echo "  GIT_TAG:    ${GIT_TAG}"
	@echo "  COMMIT:     ${COMMIT}"
	@echo "Testing variables:"
	@echo " Produced Image: ${CSI_IMAGE}"
	@echo " REGISTRY_PATH: ${REGISTRY_PATH}"
	@echo " LOCAL_REGISTRY_PATH: ${LOCAL_REGISTRY_PATH}"


.get:
	rm -rf ./bin
	GO111MODULE=on go mod download
#Copies grpc services protobuf files to local folder
.copy-edgefs-grpc:
	docker pull ${EDGEFS_IMAGE}
	$(eval CONTAINER_ID := $(shell docker create ${EDGEFS_IMAGE}))
	docker cp ${CONTAINER_ID}:/opt/nedge/lib/grpc-efsproxy ./
	docker rm ${CONTAINER_ID}

build: .get .copy-edgefs-grpc
	mkdir -p ./bin
	GO111MODULE=on CGO_ENABLED=0 GOOS=linux go build -a -ldflags '$(LDFLAGS)' -o ./bin/$(PLUGIN_NAME) main.go

container: build
	docker build -f Dockerfile -t $(CSI_IMAGE) .

push: container
	docker tag  $(CSI_IMAGE) ${REGISTRY_PATH}
	docker push ${REGISTRY_PATH}

push-dev: container
	docker tag  $(CSI_IMAGE) $(DEV_REGISTRY_PATH)
	docker push $(DEV_REGISTRY_PATH)
test:
	go test -count=1 -v ./csi -run TestAttachISCSIVolume #$TestAttachISCSIVolume #TestGetISCSIDevices

clean:
	-rm -rf $(PLUGIN_NAME) src .get bin
