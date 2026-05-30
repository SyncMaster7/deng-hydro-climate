FROM ckan/ckan-base:2.11.4

# Install CKAN extensions into system site-packages (not user dir)
RUN pip install --no-cache-dir --target=/usr/local/lib/python3.10/site-packages \
    ckanext-hierarchy \
    ckanext-dcat \
    ckanext-scheming \
    "git+https://github.com/ckan/ckanext-spatial.git#egg=ckanext-spatial"