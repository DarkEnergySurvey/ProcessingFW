import distutils
from distutils.core import setup
import glob

bin_files = glob.glob("bin/*")
tools_files = glob.glob("tools/*")
etc_files = glob.glob("etc/*")
libexec_files = glob.glob("libexec/*")
share_files = glob.glob("share/condor/*")

# The main call
setup(name='ProcessingFW',
      version ='2.3.11',
      license = "GPL",
      description = "DESDM's processing framework",
      author = "Michelle Gower",
      author_email = "mgower@illinois.edu",
      packages = ['processingfw'],
      package_dir = {'': 'python'},
      scripts = bin_files,
      data_files=[('ups',['ups/ProcessingFW.table']),
                  ('tools',tools_files),
                  ('libexec',libexec_files),
                  ('etc',etc_files),
                  ('share/condor', share_files)]
      )

