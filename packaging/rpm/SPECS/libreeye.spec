Name:           libreeye
Version:        %{getenv:VERSION}
Release:        1%{?dist}
Summary:        Camera surveillance program

License:        GPLv3
URL:            https://github.com/chponte/libreeye
Source0:        https://github.com/chponte/libreeye/archive/%{version}.tar.gz

BuildArch:      noarch
Autoreq:        0
BuildRequires:  python3-devel
BuildRequires:  python3-setuptools
Requires(pre):  shadow-utils
Requires:       python3
Requires:       python3-daemon
Requires:       python3-google-api-client
Requires:       python3-google-auth
Requires:       python3-google-auth-oauthlib
Requires:       python3-numpy
Requires:       python3-opencv
Requires:       ffmpeg

%global _description %{expand:
A camera surveillance program written in python with redundant network storage, motion detection and event notification capabilities.}

%description %_description

%prep
%autosetup -n libreeye-%{version}

%build
%py3_build

%pre
getent group libreeye >/dev/null || groupadd -r libreeye
getent passwd libreeye >/dev/null || \
    useradd -r -g libreeye -s /sbin/nologin libreeye
exit 0

%install
%py3_install
mkdir -p %{buildroot}/usr/lib/systemd/system
sed -e 's$ExecStart=$ExecStart=%{_bindir}/libreeyed$' systemd/libreeye.service > %{buildroot}/usr/lib/systemd/system/libreeye.service
mkdir -p %{buildroot}/etc
cp -r conf %{buildroot}/etc/libreeye
mkdir -p %{buildroot}/var/log/libreeye

#%%check

%files
# %license COPYING
# %doc README.rst
%{python3_sitelib}/*
%{_bindir}/libreeye
%{_bindir}/libreeyed
/usr/lib/systemd/system/libreeye.service
%attr(-, root, libreeye) /etc/libreeye/libreeye.conf
%attr(-, root, libreeye) /etc/libreeye/storage.conf
%attr(-, root, libreeye) /etc/libreeye/cameras.d
%attr(750, root, libreeye) /etc/libreeye/secrets
%attr(775, root, libreeye) /var/log/libreeye