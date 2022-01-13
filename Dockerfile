FROM python:3.10-slim

RUN apt-get update && apt-get install -y gcc
COPY . /src
WORKDIR /src

RUN pip install -U pip && \
    pip install -r requirements.txt && \
    pip install -r requirements-build.txt

RUN python setup.py build_ext -t build -b release -f && \
    strip release/*.so release/routes/*.so release/util/*.so && \
    mv release /transientdb && \
    cp src/main.py conf/config.ini /transientdb && \
    pip uninstall -y -r requirements-build.txt

WORKDIR /transientdb

RUN rm -rf /src

ENV PYTHONPATH=/transientdb

# ENTRYPOINT [ "tini", "--" ]
ENTRYPOINT [ "python" ]
EXPOSE 8000
# CMD [ "python", "main.py" ]
CMD [ "main.py" ]