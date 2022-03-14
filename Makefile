image-build:
	#docker buildx create --use --name=builder-cn --driver docker-container --driver-opt image=dockerpracticesig/buildkit:master-tencent
	docker buildx build --platform linux/amd64,linux/arm/v7,linux/arm64/v8,linux/ppc64le,linux/s390x -t ccr.ccs.tencentyun.com/stream/dispatcher:latest . --push
local:
	cd cmd && CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -a -o dispatcher main.go &&cd .. && docker build -f Dockerfile-local -t ccr.ccs.tencentyun.com/stream/dispatcher:latest . && kind load docker-image ccr.ccs.tencentyun.com/stream/dispatcher:latest --name c1