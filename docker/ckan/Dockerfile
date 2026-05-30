FROM ckan/ckan-base:2.11.4

USER root

RUN pip install --no-cache-dir \
    ckanext-hierarchy \
    ckanext-dcat \
    ckanext-scheming

RUN pip install --no-cache-dir -e \
    "git+https://github.com/ckan/ckanext-spatial.git#egg=ckanext-spatial"

USER ckan