# Tools

This folder contains various scripts and configurations, used by CI or otherwise.

You can use the Dockerfile, for example, to run clippy on systems without systemd or fuse libraries:

    docker build -t s3wofs-dev -f .tools/Dockerfile .
    docker run --rm -v $(pwd):/workspace s3wofs-dev cargo clippy --workspace --lib --bins --tests --all-targets -- -Dwarnings
