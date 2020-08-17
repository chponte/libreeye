#!/bin/sh

dnf install -y rpm-build rpm-devel python3 python3-devel python3-setuptools
mkdir -p /libreeye-${VERSION}/packaging/rpm/SOURCES
tar cz --exclude="libreeye-${VERSION}/packaging" \
    -f /libreeye-${VERSION}/packaging/rpm/SOURCES/$VERSION.tar.gz \
    -C / libreeye-${VERSION}
rpmbuild --define "_topdir /libreeye-${VERSION}/packaging/rpm/" -ba \
    /libreeye-${VERSION}/packaging/rpm/SPECS/libreeye.spec
