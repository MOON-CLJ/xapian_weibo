from setuptools import setup

setup(name='xapian_weibo',
      version='0.1',
      url='https://github.com/MOON-CLJ/xapian_weibo',
      author='CLJ',
      packages=['xapian_weibo'],
      data_files=[('dict', ['dict/userdic.txt', 'dict/stopword.txt', 'dict/emotionlist.txt', 'dict/one_word_white_list.txt'])],
      install_requires=[
          'pyzmq',
      ],
      dependency_links=[
      ],
)

# extra requires
# xapian and python-binding
# git@github.com:MOON-CLJ/pyscws.git
