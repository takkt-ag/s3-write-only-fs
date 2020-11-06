%define __spec_install_post %{nil}
%define __os_install_post %{_dbpath}/brp-compress
%define debug_package %{nil}

Name: s3-write-only-fs
Summary: FUSE filesystem to mount an S3-bucket in a write-only fashion
Version: @@VERSION@@
Release: @@RELEASE@@%{?dist}
License: Proprietary
Group: Applications/System
Source0: %{name}-%{version}.tar.gz

Provides: s3wofs
Requires: fuse-libs

BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-root

%description
%{summary}

%prep
%setup -q

%install
rm -rf %{buildroot}
mkdir -p %{buildroot}/%{_bindir}
mkdir -p %{buildroot}/%{_sbindir}
install -m 0755 %{name} %{buildroot}/%{_bindir}/s3wofs
ln -s %{_bindir}/s3wofs %{buildroot}/%{_sbindir}/mount.s3wofs
ln -s %{_bindir}/s3wofs %{buildroot}/%{_sbindir}/mount.fuse.s3wofs

%clean
rm -rf %{buildroot}

%files
%defattr(-,root,root,-)
%{_bindir}/*
%{_sbindir}/*
