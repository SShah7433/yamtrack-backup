# Debian slim rather than Alpine: google-cloud-storage pulls in google-crc32c,
# which ships prebuilt manylinux wheels but has no musl wheels, so Alpine would
# force a C toolchain into the image just to compile it.
FROM python:3.12-slim

# Surface log lines immediately instead of buffering them until exit, so
# `docker logs` is useful while a long snapshot is running.
ENV PYTHONUNBUFFERED=1

WORKDIR /backup

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY backup.py ./

# Runs the daily scheduler by default; append --once for a single backup.
ENTRYPOINT ["python", "backup.py"]
