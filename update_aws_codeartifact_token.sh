#!/bin/bash

DOMAIN="agro-domain"
DOMAIN_OWNER="267684368050"
REGION="eu-west-1"
REPOSITORY="agro-repository"

# Ottieni il token
TOKEN=$(aws codeartifact get-authorization-token --domain $DOMAIN --domain-owner $DOMAIN_OWNER --region $REGION --query authorizationToken --output text)

# Crea o aggiorna il file .pypirc
cat <<EOF > ~/.pypirc
[distutils]
index-servers =
    codeartifact

[codeartifact]
repository: https://$DOMAIN-$DOMAIN_OWNER.d.codeartifact.$REGION.amazonaws.com/pypi/$REPOSITORY/
username: aws
password: $TOKEN
EOF
