import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name='handler_cf_v1',
    packages=['handler_cf_v1'],
    version='1.0.43',
    license='MIT',
    description='Testing installation of Package',
    long_description=long_description,
    long_description_content_type="text/markdown",
    author='Luis Moreno',
    author_email='luis.cfh.90@gmail.com',
    url='https://github.com/luigicfh/cf_handler_module',
    project_urls={
        "Bug Tracker": "https://github.com/luigicfh/cf_handler_module/issues"
    },
    install_requires=['requests', 'five9',
                      'google-cloud-firestore', 'pandas', "beautifulsoup4", "sqlalchemy", 'pymysql'],
    keywords=["pypi", "handler_module", "cloud_functions"],
    classifiers=[                                   # https://pypi.org/classifiers
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Documentation',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
    ],
    download_url="https://github.com/luigicfh/cf_handler_module/archive/refs/tags/1.0.43.tar.gz",
)
