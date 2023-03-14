from distutils.core import setup
from Cython.Build import cythonize
from distutils.extension import Extension
from Cython.Distutils import build_ext
ext = [Extension("db_c",
                 sources=['db_c.pyx'],
                 extra_compile_args=['-O3'],
                 )]
for e in ext:
    e.cython_directives = {'language_level': "3"} #all are Python-3
setup(name='db_c', ext_modules=ext, cmdclass={
      'build_ext': build_ext})
