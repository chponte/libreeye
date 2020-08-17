import os
import setuptools

# Version is read from file
with open('VERSION', 'r') as f:
    version = f.read().strip()

# Requirements attending to distribution
fedora_build = 'RPM_PACKAGE_RELEASE' in os.environ
if fedora_build:
    # Fedora RPM package includes python3-opencv as a requirement, which doesn't
    # appear under pip, therefore it's not included as a requirement in
    # setuptools project
    requirements = [
        'ffmpeg-python',
        'google-auth',
        'google-api-python-client',
        'google-auth-oauthlib',
        'google-auth-httplib2',
        'numpy',
        'python-daemon'
    ]
else:
    requirements = [
        'ffmpeg-python',
        'google-auth',
        'google-api-python-client',
        'google-auth-oauthlib',
        'google-auth-httplib2',
        'numpy',
        'opencv-python-headless',
        'python-daemon'
    ]


# Call setup method
setuptools.setup(
    name='libreeye',
    version=version,
    description='Camera surveillance program',
    url='https://chponte.github.io/libreeye',
    author='Christian Ponte',
    author_email='chponte@pm.me',
    classifiers=[
        'Development Status :: 4 - Beta',
        'License :: OSI Approved :: GNU General Public License v3 (GPLv3)',
        'Programming Language :: Python :: 3.7'
    ],
    keywords='libreeye',
    package_dir={'': 'src'},
    packages=setuptools.find_packages(where='src'),
    python_requires='>=3.7',
    install_requires=requirements,
    entry_points={
        'console_scripts': [
            'libreeye=libreeye.main:main',
            'libreeyed=libreeye.daemon.daemon:main'
        ],
    },
    project_urls={
        'Bug Reports': 'https://github.com/chponte/libreeye/issues',
        'Source': 'https://github.com/chponte/libreeye/'
    },
)
