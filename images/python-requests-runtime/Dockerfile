FROM python:3.10-alpine3.23

RUN apk add --no-cache mount \
    && ln -s /usr/local/bin/python3 /usr/bin/python3 \
    && pip install --no-cache-dir --no-compile cloudpickle==3.1.2 requests==2.34.2 \
    && apk del tzdata \
    && rm -rf /usr/local/bin/pip* \
        /usr/local/bin/wheel \
        /usr/local/include \
        /usr/local/lib/libpython3.10.a \
        /usr/local/lib/pkgconfig \
        /usr/local/lib/python3.10/config-* \
        /usr/local/lib/python3.10/ensurepip \
        /usr/local/lib/python3.10/idlelib \
        /usr/local/lib/python3.10/lib2to3 \
        /usr/local/lib/python3.10/pydoc_data \
        /usr/local/lib/python3.10/site-packages/pip* \
        /usr/local/lib/python3.10/site-packages/packaging* \
        /usr/local/lib/python3.10/site-packages/setuptools* \
        /usr/local/lib/python3.10/site-packages/wheel* \
        /usr/local/lib/python3.10/site-packages/pkg_resources \
        /usr/local/lib/python3.10/site-packages/_distutils_hack \
        /usr/local/lib/python3.10/site-packages/distutils-precedence.pth \
        /usr/local/lib/python3.10/test \
        /usr/local/lib/python3.10/turtledemo \
        /usr/local/share/man \
        /root/.cache

CMD ["/bin/true"]
