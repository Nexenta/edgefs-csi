ARG EDGEFS_IMAGE=edgefs/edgefs:latest
FROM ${EDGEFS_IMAGE} as builder

#FROM centos:centos6
#FROM ubuntu:18.10
FROM alpine:3.2

LABEL name="edgefs-csi-driver"
LABEL maintainer="Nexenta Systems, Inc."
LABEL description="Edgefs NFS/ISCSI CSI Driver"
LABEL io.k8s.description="Edgefs NFS/ISCSI CSI Driver"

#CentOS installation section
#RUN yum update -y
#RUN yum install -y lsscsi iscsi-initiator-utils device-mapper-multipath
#RUN yum install -y nfs-utils

#RUN apk update

#Debian installation version
#RUN apt-get update -y
#RUN apt-get install nfs-common -y
#RUN apt-get install open-iscsi -y 

RUN apk update || true &&  \
	apk add coreutils util-linux blkid nfs-utils \
	lsscsi \
	e2fsprogs \
	bash \
	kmod \
	curl \
	jq \
	ca-certificates


RUN mkdir /edgefs
ADD chroot-host-wrapper.sh /edgefs

RUN mkdir -p /etc/
RUN mkdir -p /config/

COPY --from=builder /opt/nedge/sbin/edgefs-csi /
COPY --from=builder /opt/nedge/sbin/csc /

RUN chmod 777 /edgefs/chroot-host-wrapper.sh
RUN    ln -s /edgefs/chroot-host-wrapper.sh /edgefs/blkid \
    && ln -s /edgefs/chroot-host-wrapper.sh /edgefs/blockdev \
    && ln -s /edgefs/chroot-host-wrapper.sh /edgefs/df \
    && ln -s /edgefs/chroot-host-wrapper.sh /edgefs/iscsiadm \
    && ln -s /edgefs/chroot-host-wrapper.sh /edgefs/lsscsi \
    && ln -s /edgefs/chroot-host-wrapper.sh /edgefs/mkfs.ext3 \
    && ln -s /edgefs/chroot-host-wrapper.sh /edgefs/mkfs.ext4 \
    && ln -s /edgefs/chroot-host-wrapper.sh /edgefs/mkfs.xfs \
    && ln -s /edgefs/chroot-host-wrapper.sh /edgefs/multipath \
    && ln -s /edgefs/chroot-host-wrapper.sh /edgefs/multipathd 
#    && ln -s /netapp/chroot-host-wrapper.sh /edgefs/ls \ 
#    && ln -s /netapp/chroot-host-wrapper.sh /edgefs/mkdir \
#    && ln -s /netapp/chroot-host-wrapper.sh /edgefs/mount \
#    && ln -s /netapp/chroot-host-wrapper.sh /edgefs/stat \
#    && ln -s /netapp/chroot-host-wrapper.sh /edgefs/umount \
#    && ln -s /netapp/chroot-host-wrapper.sh /edgefs/rmdir
#
ENV PATH="/edgefs:${PATH}"

ENTRYPOINT ["/edgefs-csi"]
