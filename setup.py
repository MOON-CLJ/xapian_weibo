from setuptools import setup

setup(name='xapian_weibo',
      version='0.1',
      url='https://github.com/MOON-CLJ/xapian_weibo',
      author='CLJ',
      packages=['xapian_weibo'],
      install_requires=[
          'pymongo',
      ],
      dependency_links=[
      ],
)

# extra requires
# xapian and python-binding
# git@github.com:MOON-CLJ/pyscws.git
