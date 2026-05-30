FROM ckan/ckan-base:2.11.4

USER root

RUN pip install --no-cache-dir \
    ckanext-hierarchy \
    ckanext-dcat \
    ckanext-scheming \
    rdflib-jsonld \
    "git+https://github.com/ckan/ckanext-spatial.git#egg=ckanext-spatial" && \
    pip install --no-cache-dir \
    -r https://raw.githubusercontent.com/ckan/ckanext-spatial/master/requirements.txt

USER ckan