# MiniOB Docker Development Image

This image targets the MiniOB training environment baseline: Ubuntu 22.04 with GCC/G++ 11.

Build the default image, including the current source tree after `.dockerignore` exclusions:

```bash
docker build --platform linux/amd64 -f Dockerfile.dev -t miniob2026-dev:latest .
```

Run an interactive shell from the packaged source:

```bash
docker run --rm -it --platform linux/amd64 miniob2026-dev:latest
```

For day-to-day development, build only the dependency layer and mount the working tree so edits stay on the host:

```bash
docker build --platform linux/amd64 -f Dockerfile.dev --target dev-base -t miniob2026-dev:base .
docker run --rm -it \
  --platform linux/amd64 \
  -v "$PWD":/workspace/miniob \
  -w /workspace/miniob \
  miniob2026-dev:base
```

Inside the container:

```bash
bash build.sh debug -DWITH_CPPLINGS=OFF -DWITH_UNIT_TESTS=ON --make -j4
python3 test/case/miniob_test.py --test-cases=basic
```

The Docker context excludes local build products, Git history, runtime scratch files, and the optional `mysql-server/.git` history. The optional MySQL source tree itself is left available when present so MySQL-backed result generation can still be prepared inside the container.
