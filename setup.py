from distutils.core import setup

setup(
    name="migrate",
    version="0.1",
    description="Simple DB Schema Migration Library for Nimbus Cloud Stack",
    author="Zach Bullough",
    author_email="zach@qmulosoft.com",
    packages=["migrate"],
    package_dir={'migrate': 'src/migrate'}
)
