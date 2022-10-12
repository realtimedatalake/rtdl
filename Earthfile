ARG ver='0.2.1'

release:
    BUILD ./config+docker --ver=$ver
    BUILD ./deltawriter+docker --ver=$ver
    BUILD ./ingest+docker --ver=$ver
    BUILD ./ingester+docker --ver=$ver
    BUILD ./pii-detection+docker --ver=$ver

release-amd64:
    BUILD --platform=linux/amd64 +release

release-arm64:
    BUILD --platform=linux/arm64/v8 +release

release-multiplatform:
    BUILD --platform=linux/amd64 --platform=linux/arm64/v8 +release